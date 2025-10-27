import os
from typing import Set

BASE_URL = "https://www.hockey-reference.com"

CRAWL_DELAY = 5

HEADERS = {
    "User-Agent": (
        "AcademicResearchBot/1.0 "
        "contact: emil.igelitka@gmail.com"
    ),
    "Accept": "text/html,application/xhtml+xml"
}

FORBIDDEN_PATHS: Set[str] = set([
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
])

SCRAPE_DIR = ".\\scraped"

HTML_DIR = os.path.join(SCRAPE_DIR, 'players')

METADATA_DIR = ".\\metadata"

PROCESSED_DIR = ".\\processed"

INDEX_DIR = ".\\index"


FIELD_PLAYER_STATS = {
    "games_played": "GP",
    "goals": "G",
    "assists": "A",
    "points": "PTS",
    "plus_minus": "+/-",
    "point_shares": "PS",
    "penalty_minutes": "PIM",
    "shots_on_goal": "SH",
    "game_winning_goals": "GWG"
}

GOALKEEPER_STATS = {
    "games_played": "GP",
    "wins": "W",
    "losses": "L",
    "ties_ot_losses": "T",
    "shootouts": "SHO",
    "point_shares": "PS",
    "minutes": "MIN",
    "gaa": "GAA",
    "save_percentage": "SV%"
}   


JOBLIB_DIR = '.\\jblib'