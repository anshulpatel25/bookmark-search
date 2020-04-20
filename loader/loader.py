import csv
import re
from datetime import datetime
from functools import partial
from multiprocessing import Pool
from optparse import OptionParser
from urllib.parse import urlparse


import bookmarks_parser as bp
import requests
from bs4 import BeautifulSoup as bs
from elasticsearch import Elasticsearch


def ingest_data(bookmark_url, host, port, index):

    ingestion_client = get_ingestion_client(host, port)

    print(f"Processing {bookmark_url} ...")
    try:
        raw_resp = requests.get(bookmark_url, timeout=10)

        print(f"Transforming {bookmark_url} ...")
        raw_text = bs(raw_resp.content, "html.parser").get_text(strip=True)
        processed_data = re.sub(r"[\r\t\n]", "", re.sub(r"[^\w\s]", "", raw_text))
        website = "{uri.netloc}".format(uri=urlparse(bookmark_url))
        print(f"Ingesting {bookmark_url} ...")
        ingestion_client.index(
            index=index,
            body={"url": bookmark_url, "content": processed_data, "website": website},
        )
        print(f"Successfully processed {bookmark_url}")
        return {"status": "success", "url": bookmark_url, "reason": None}
    except Exception as error:
        print(f"Unable to Process {bookmark_url}")
        return {"status": "fail", "reason": str(error), "url": bookmark_url}


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


def process(location, host, port, index):

    bookmarks_tree = get_bookmark_tree(location)
    bookmarks_url_list = get_bookmark_urls(bookmarks_tree)

    test_list = bookmarks_url_list[:10]

    with Pool(4) as mp:
        # results = mp.map(ingest_data, bookmarks_url_list)
        results = mp.map(
            partial(ingest_data, host=host, port=port, index=index), test_list
        )

    result_filename = "result_" + datetime.now().strftime("%H_%M_%S") + ".csv"
    with open(result_filename, "w", newline="") as result_file:
        writer = csv.writer(result_file)
        writer.writerow(["URL", "STATUS", "REASON"])
        for result in results:
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

    (options, args) = parser.parse_args()

    process(
        location=options.location,
        host=options.server,
        port=options.port,
        index=options.index,
    )


if __name__ == "__main__":
    main()
