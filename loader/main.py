from bs4 import BeautifulSoup as bs
import requests
import re
import bookmarks_parser as bp
import pprint
import json
from elasticsearch import Elasticsearch
from multiprocessing import Pool
from functools import partial
import pickle
from urllib.parse import urlparse
import csv
from datetime import datetime



def ingest_data(bookmark_url):
    ingestion_client = get_ingestion_client('localhost', 9200)

    print(f"Processing {bookmark_url} ...")
    try:
        raw_resp = requests.get(bookmark_url, timeout=10)

        print(f"Transforming {bookmark_url} ...")
        raw_text = bs(raw_resp.content, 'html.parser').get_text(strip=True)
        processed_data = re.sub('[\r\t\n]','',re.sub('[^\w\s]','', raw_text))
        website = "{uri.netloc}".format(uri=urlparse(bookmark_url))
        print(f"Ingesting {bookmark_url} ...")
        ingestion_client.index(
            index="bookmarks",
            body={
                "url": bookmark_url,
                "content": processed_data,
                "website": website
            }
        )
        print(f"Successfully processed {bookmark_url}")
        return {
            "status": "success",
            "url": bookmark_url,
            "reason": None
        }
    except Exception as error:
            print(f"Unable to Process {bookmark_url}")
            return {
                "status": "fail",
                "reason": str(error),
                "url": bookmark_url
            }


def get_bookmark_urls(bookmarks_tree):
    bookmarks_url_list = []
    for bookmark in bookmarks_tree:
        if 'children' in bookmark and len(bookmark["children"]) > 0:
            bookmarks_url_list += get_bookmark_urls(bookmark["children"])
        else:
            if bookmark['type'] == "bookmark":
                bookmarks_url_list.append(bookmark['url'])

    return bookmarks_url_list


def get_bookmark_tree(parsed_bookmarks):
    bookmarks_tree = bp.parse(parsed_bookmarks)
    return bookmarks_tree

def get_ingestion_client(host, port):
    return Elasticsearch([{
        'host': host,
        'port': port
    }])



def main():
    bookmarks_tree = get_bookmark_tree("/home/anshul/Documents/google_bookmarks_18_04_2020.html")
    bookmarks_url_list = get_bookmark_urls(bookmarks_tree)

    with Pool(4) as mp:
        results = mp.map(ingest_data, bookmarks_url_list)

    result_filename = "result_"+ datetime.now().strftime("%H_%M_%S") + ".csv"
    with open(result_filename, "w", newline='') as result_file:
        writer = csv.writer(result_file)
        writer.writerow(["URL", "STATUS", "REASON"])
        for result in results:
            writer.writerow([result['url'], result['status'], result['reason']])

if __name__ == "__main__":
    main()


