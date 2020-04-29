#!/usr/bin/env python3

import csv
import hashlib
import logging
import re
from datetime import datetime
from functools import partial
from multiprocessing import Pool
from optparse import OptionParser
from urllib.parse import urlparse


import bookmarks_parser as bp
import requests
import sqlite3
from bs4 import BeautifulSoup as bs
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)


def ingest_data(bookmark_url, host, port, index, db_location, user_agent):

    url_hash = hashlib.sha256(bytes(bookmark_url, "utf-8")).hexdigest()

    ingestion_client = get_ingestion_client(host, port)

    connection = sqlite3.connect(db_location)
    cursor = connection.cursor()

    exists = cursor.execute(
        "SELECT EXISTS(SELECT  1 FROM PROCESS_STATUS ps WHERE hash=?)", (url_hash,)
    ).fetchone()

    logging.info("Processing %s ...", bookmark_url)

    if not exists[0]:
        try:

            headers = {"User-Agent": user_agent}
            raw_resp = requests.get(bookmark_url, timeout=10, headers=headers)

            if raw_resp.status_code == 200:
                logging.info("Transforming %s ...", bookmark_url)
                raw_text = bs(raw_resp.content, "html.parser").get_text(strip=True)
                processed_data = re.sub(
                    r"[\r\t\n]", "", re.sub(r"[^\w\s]", "", raw_text)
                )
                website = "{uri.netloc}".format(uri=urlparse(bookmark_url))
                logging.info("Ingesting %s ...", bookmark_url)
                ingestion_client.index(
                    index=index,
                    body={
                        "url": bookmark_url,
                        "content": processed_data,
                        "website": website,
                    },
                )
                logging.info("Successfully processed %s", bookmark_url)
                cursor.execute("insert into PROCESS_STATUS values (?)", (url_hash,))
                connection.commit()
                return {"status": "success", "url": bookmark_url, "reason": None}
            else:
                logging.info("Unable to Process %s", bookmark_url)
                return {
                    "status": "fail",
                    "reason": str(raw_resp.status_code),
                    "url": bookmark_url,
                }

        except Exception as error:
            logging.info("Unable to Process %s", bookmark_url)
            return {"status": "fail", "reason": str(error), "url": bookmark_url}

        finally:
            connection.close()

    else:
        logging.info("Skipping %s as it exists", bookmark_url)


def get_bookmark_urls(bookmarks_tree):
    bookmarks_url_list = []
    for bookmark in bookmarks_tree:
        if "children" in bookmark and len(bookmark["children"]) > 0:
            bookmarks_url_list += get_bookmark_urls(bookmark["children"])
        else:
            if bookmark["type"] == "bookmark":
                bookmarks_url_list.append(bookmark["url"])

    return bookmarks_url_list


def get_bookmark_tree(parsed_bookmarks):
    bookmarks_tree = bp.parse(parsed_bookmarks)
    return bookmarks_tree


def get_ingestion_client(host, port):
    return Elasticsearch([{"host": host, "port": port}])


def initialize_db(db_location):
    logging.info("Initializing Database ...")
    connection = sqlite3.connect(db_location)
    cursor = connection.cursor()
    cursor.execute(
        """
                    CREATE TABLE IF NOT EXISTS PROCESS_STATUS (hash text)"""
    )
    connection.commit()
    connection.close()
    logging.info("Database initialized")


def process(location, host, port, index, db_location, cpu, user_agent):

    bookmarks_tree = get_bookmark_tree(location)
    bookmarks_url_list = get_bookmark_urls(bookmarks_tree)

    with Pool(cpu) as mp:
        # results = mp.map(ingest_data, bookmarks_url_list)
        results = mp.map(
            partial(
                ingest_data,
                host=host,
                port=port,
                index=index,
                db_location=db_location,
                user_agent=user_agent,
            ),
            bookmarks_url_list,
        )

    result_filename = "result_" + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + ".csv"
    with open(result_filename, "w", newline="") as result_file:
        writer = csv.writer(result_file)
        writer.writerow(["URL", "STATUS", "REASON"])
        for result in results:
            if result is not None:
                writer.writerow([result["url"], result["status"], result["reason"]])


def main():

    parser = OptionParser(usage="usage: %prog [options]", version="%prog 1.0")

    parser.add_option(
        "-s",
        "--server",
        dest="server",
        default="localhost",
        type="string",
        help="Elasticsearch Host",
    )

    parser.add_option(
        "-p",
        "--port",
        dest="port",
        default=9200,
        type="int",
        help="Elasticsearch Port",
    )

    parser.add_option(
        "-i",
        "--index",
        dest="index",
        default="bookmark",
        type="string",
        help="Elasticsearch Index",
    )

    parser.add_option(
        "-l",
        "--location",
        dest="location",
        type="string",
        help="Location to Bookmarkfile",
    )

    parser.add_option(
        "-d",
        "--database",
        dest="database_location",
        type="string",
        help="Location to SqliteDB, it will be created if not provided",
    )

    parser.add_option(
        "-c", "--cpu", dest="cpu", default=4, type="int", help="Number of CPUs",
    )

    parser.add_option(
        "-a",
        "--agent",
        dest="agent",
        default="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
        type="string",
        help="User-Agent to use while scraping websites",
    )

    (options, args) = parser.parse_args()

    required_flags = ["location", "server", "port", "index", "database_location"]

    for flag in required_flags:
        if options.__dict__[flag] is None:
            parser.error("Parameter --%s required" % flag)

    initialize_db(options.database_location)

    process(
        location=options.location,
        host=options.server,
        port=options.port,
        index=options.index,
        db_location=options.database_location,
        cpu=options.cpu,
        user_agent=options.agent,
    )


if __name__ == "__main__":
    main()
