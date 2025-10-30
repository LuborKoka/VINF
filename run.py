from parser import parse_downloaded_stuff
from scraper import run_scraper
from indexer import get_index, POSITION, IDF



if __name__ == '__main__':
    #run_scraper()
    #parse_downloaded_stuff()
    ix = get_index()

    query_string = f"position {POSITION.GOALKEEPER.value}, games_played more than 1000"

    print('\n\n', '='*40, 'IDF PROBABILISTIC', '='*40, '\n\n')
    ix.search(query_string, limit = 5, idf=IDF.PROBABILISTIC)
    print('\n\n', '='*40, 'IDF STANDARD', '='*40, '\n\n')
    ix.search(query_string, limit = 5, idf=IDF.STANDARD)


