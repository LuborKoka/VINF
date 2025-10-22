import os

BASE_URL = "https://www.hockey-reference.com"

CRAWL_DELAY = 5

HEADERS = {
    "User-Agent": (
        "AcademicResearchBot/1.0 "
        "contact: emil.igelitka@gmail.com"
    ),
    "Accept": "text/html,application/xhtml+xml"
}

FORBIDDEN_PATHS = [
    "/hockey/",
    "/play-index/*cgi",
    "/scoring/",
    "/gamelog/",
    "/splits/",
    "/player_search.cgi",
    "/boxscores/index",
    "/my/",
    "/7103",
    "/req/",
    "/short/",
    "/nocdn/",
]

SCRAPE_DIR = ".\\scraped"

HTML_DIR = os.path.join(SCRAPE_DIR, 'players')

METADATA_DIR = ".\\metadata"

PROCESSED_DIR = ".\\processed"