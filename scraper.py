import typing
import requests
import os
import csv
from typing import List, Set, TextIO
from datetime import date
import random
import time

from config import CRAWL_DELAY, HEADERS, HTML_DIR, LIST_DIR, METADATA_DIR, SCRAPE_DIR, BASE_URL
from object_types import METADATA
from parser import find_list_of_list_of_players_url, find_list_of_players_urls, find_players_urls

session = requests.Session()

def sleep() -> None:
    random_offset = random.uniform(0, 2)
    sleep_len = CRAWL_DELAY + random_offset
    time.sleep(sleep_len)
    

def write_metadata(file: TextIO, metadata_dict: METADATA, write_header: bool = False) -> None:
    """
    Write metadata to a TSV file.
    
    Args:
        file: An open file object
        metadata_dict: Dictionary containing metadata values
        write_header: If True, writes the header row (keys) before the values
    """
    writer = csv.writer(file, delimiter='\t')

    if write_header:
        writer.writerow(metadata_dict.keys())

    writer.writerow(metadata_dict.values())

def fetch_root() -> str:
    if not os.path.exists(os.path.join(SCRAPE_DIR, 'index.html')):
        response = session.get(BASE_URL, headers=HEADERS, timeout=50)

        with open(os.path.join(SCRAPE_DIR, 'index.html'), 'w', encoding='UTF-8') as index:
            index.write(response.text)

        with open(os.path.join(METADATA_DIR, 'index', 'index_metadata.tsv'), 'w', encoding='UTF-8') as outfile:
            data: METADATA = {
                'download_date': date.today().strftime('%d--%m--%Y'),
                'download_url': BASE_URL,
                'file_path': os.path.join(SCRAPE_DIR, 'index.html')
            }
            write_metadata(outfile, data, True)
        sleep()
        return response.text
    else:
        with open(os.path.join(SCRAPE_DIR, 'index.html'), 'r', encoding='UTF-8') as index:
            return index.read()



def fetch_list_of_list_of_players(url: str) -> str:
    if os.path.exists(os.path.join(SCRAPE_DIR, 'sitemap.html')):
        with open(os.path.join(SCRAPE_DIR, 'sitemap.html'), 'r', encoding='UTF-8') as infile:
            return infile.read()
        
    response = session.get(f'{BASE_URL}{url}', headers=HEADERS, timeout=50)
    with open(os.path.join(SCRAPE_DIR, 'sitemap.html'), 'w', encoding='UTF-8') as sitemap_file:
        with open(os.path.join(METADATA_DIR, 'sitemap', 'sitemap_metadata.tsv'), 'w', encoding='UTF-8') as outfile:
            data: METADATA = {
                'download_date': date.today().strftime('%d-%m-%Y'),
                'download_url': f'{BASE_URL}{url}',
                'file_path': os.path.join(SCRAPE_DIR, 'sitemap.html')
            }
            write_metadata(outfile, data, True)

        sitemap_file.write(response.text)
        sleep()
        return response.text


def fetch_list_of_players_urls(relative_url: str):
    letter = relative_url.strip("/").split("/")[-1]
    filename = f"players_{letter}.html"
    
    if os.path.exists(os.path.join(LIST_DIR, filename)):
        with open(os.path.join(LIST_DIR, filename), 'r', encoding='UTF-8') as infile:
            return infile.read()


    response = session.get(f'{BASE_URL}{relative_url}', headers=HEADERS, timeout=50)
    with open(os.path.join(LIST_DIR, filename), 'w', encoding='UTF-8') as sitemap_file:
        with open(os.path.join(METADATA_DIR, 'sitemap', 'sitemap_metadata.tsv'), 'a', encoding='UTF-8') as outfile:
            data: METADATA = {
                'download_date': date.today().strftime('%d-%m-%Y'),
                'download_url': f'{BASE_URL}{relative_url}',
                'file_path': os.path.join(LIST_DIR, filename)
            }
            write_metadata(outfile, data)

        sitemap_file.write(response.text)
        sleep()
        return response.text


def fetch_file(relative_url: str, index: int, metadata_file: TextIO):
    file_name = relative_url.split("/")[-1]
    filepath = os.path.join(HTML_DIR, file_name)

    metadata: METADATA = {
        'download_date': '13-10-2025',
        'download_url': f'{BASE_URL}{relative_url}',
        'file_path': filepath
    }


    response = session.get(f'{BASE_URL}{relative_url}', headers=HEADERS, timeout=50)
    with open(filepath, 'w', encoding='UTF-8') as outfile:
        outfile.write(response.text)
        metadata['download_date'] = date.today().strftime('%d-%m-%Y')
        write_metadata(metadata_file, metadata, index == 0)
        print(f'{relative_url} downloaded')
        sleep()

def read_metadata():
    filepath = os.path.join(METADATA_DIR, 'all_metadata.tsv')
    file = open(filepath, 'r', encoding='UTF-8')
    unique_urls: Set[str] = set()

    reader = csv.DictReader(file, delimiter='\t')
    for row in reader:
        data = typing.cast(METADATA, row)
        url = data['download_url']
        if url in unique_urls:
            continue

        unique_urls.add(url)


def run_scraper(skip_dirs: List[str]) -> None:
    index_html = fetch_root()
    players_url = find_list_of_list_of_players_url(index_html)

    if players_url is None:
        print('aint no players url')
        exit()

    sitemap_html = fetch_list_of_list_of_players(players_url)

    list_urls = find_list_of_players_urls(sitemap_html, players_url)

    if list_urls is None:
        exit()

    for url in list_urls:
        current_dir = url[-2]

        if current_dir in skip_dirs:
            continue

        players_sitemap_html = fetch_list_of_players_urls(url)
        players = find_players_urls(players_sitemap_html)



        metadata_outfile = open(os.path.join(METADATA_DIR, 'players', f'{current_dir}.tsv'), 'w', encoding='UTF-8')

        for index, [path, _] in enumerate(players):
            fetch_file(path, index, metadata_outfile)

        metadata_outfile.close()    






