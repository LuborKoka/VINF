from parser import parse_downloaded_stuff
from scraper import run_scraper


if __name__ == '__main__':
    SKIP_DIRS  = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w'] # scraped sitemaps

    run_scraper()
    #parse_downloaded_stuff(2000)