from parser import parse_downloaded_stuff
from scraper import run_scraper
from indexer import Index, get_index, POSITION



if __name__ == '__main__':
    #run_scraper()
    #parse_downloaded_stuff()
    ix = get_index()

    query_string = f"dob march, position {POSITION.DEFENSE.value}, games_played between 200 and 500"
    ix.search(query_string, limit = 5)

