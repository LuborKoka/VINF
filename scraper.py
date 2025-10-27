import requests
import os
import csv
from typing import Any, Dict, List, Set, cast
from datetime import date
import random
import time

from config import CRAWL_DELAY, HEADERS, HTML_DIR, METADATA_DIR, BASE_URL
from object_types import METADATA
from parser import get_hrefs

session = requests.Session()

def sleep() -> None:
    random_offset = random.uniform(0, 2)
    sleep_len = CRAWL_DELAY + random_offset
    time.sleep(sleep_len)

def read_metadata() -> Dict[str, str]:
    filepath = os.path.join(METADATA_DIR, 'all_metadata.tsv')
    file = open(filepath, 'r', encoding='UTF-8')
    
    result: Dict[str, str] = {}

    reader = csv.DictReader(file, delimiter='\t')
    for row in reader:
        data = cast(METADATA, row)
        url = data['download_url']
        if url in result:
            continue

        result[data['download_url']] = data['file_path']

    return result
    

def write_metadata(writer: Any, metadata_dict: METADATA, write_header: bool = False) -> None:
    if write_header:
        writer.writerow(metadata_dict.keys())

    writer.writerow(metadata_dict.values())

def create_filename(relative_url: str) -> str:
    if relative_url.startswith('/'):
        relative_url = relative_url[1:]
    return relative_url.replace('/', '_')    

def fetch_root() -> str:
    if not os.path.exists(os.path.join(HTML_DIR, 'index.html')):
        response = session.get(BASE_URL, headers=HEADERS, timeout=50)

        with open(os.path.join(HTML_DIR, 'index.html'), 'w', encoding='UTF-8') as index:
            index.write(response.text)

        with open(os.path.join(METADATA_DIR, 'index', 'index_metadata.tsv'), 'w', encoding='UTF-8') as outfile:
            data: METADATA = {
                'download_date': date.today().strftime('%d--%m--%Y'),
                'download_url': BASE_URL,
                'file_path': os.path.join(HTML_DIR, 'index.html')
            }
            writer = csv.writer(outfile, delimiter='\t')
            write_metadata(writer, data, True)
        sleep()
        return response.text
    else:
        with open(os.path.join(HTML_DIR, 'index.html'), 'r', encoding='UTF-8') as index:
            return index.read()



def fetch_file(relative_url: str, downloaded_urls: Dict[str, str], writer: Any) -> str:
    url = f'{BASE_URL}{relative_url}'

    if url in downloaded_urls: # toto actually neni filter, len nebudem znova stahovat stranky, ktore uz mam ulozene
        path = downloaded_urls[url]
        with open(path, 'r', encoding='UTF-8') as file:
            print(f'{relative_url} skipped')
            return file.read()
        
    filepath = os.path.join(HTML_DIR, create_filename(relative_url))

    response = requests.get(url, headers=HEADERS, timeout=30)
    metadata: METADATA = {
        'download_date': date.today().strftime('%d-%m-%Y'),
        'download_url': f'{BASE_URL}{relative_url}',
        'file_path': filepath
    }

    with open(filepath, 'w', encoding='UTF-8') as outfile:
        outfile.write(response.text)
        write_metadata(writer, metadata, False)
        print(f'{relative_url} downloaded')
        sleep()

    return response.text


def push_to_stack(stack: List[str], urls: List[str], visited: Set[str]):
    for url in urls: # toto je cast filtra
        if url in visited:
            continue

        stack.append(url)


def run_scraper() -> None:
    stack = ['/']
    session_visited_urls: Set[str] = set()
    downloaded_urls = read_metadata()  # lebo sak nemam to pustene 24/7 3 tyzdne

    #fetch_root() # call this on first call of run_scraper

    with open(os.path.join(METADATA_DIR, 'all_metadata.tsv'), 'a', encoding='UTF-8') as mtdt:
        writer = csv.writer(mtdt, delimiter='\t')
        while len(stack) > 0:
            next_url = stack.pop()
            session_visited_urls.add(next_url)
            html = fetch_file(next_url, downloaded_urls, writer)
            urls = get_hrefs(html)
            push_to_stack(stack, urls, session_visited_urls)

    