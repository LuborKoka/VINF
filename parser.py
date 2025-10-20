import re
import csv
import os
from typing import List, Optional, Set, cast
import typing
from urllib.parse import ParseResult, urlparse

from config import BASE_URL, FORBIDDEN_PATHS, METADATA_DIR, PROCESSED_DIR
from object_types import METADATA, PLAYER_DATA


def get_hrefs(html: str) -> List[str]:
    href_pattern = re.compile(
        rf'href=["\']((?:{re.escape(BASE_URL)}/(?!/)|/(?!/))[^"\']*)["\']',
        re.IGNORECASE
    )

    all_hrefs = href_pattern.findall(html)

    unique_hrefs: Set[str] = set()

    for href in all_hrefs:
        if href.startswith(BASE_URL):
            parsed = cast(ParseResult, urlparse(href))
            href = parsed.path or '/'
        elif not href.startswith('/'):
            continue

        skip = False
        for forbidden in FORBIDDEN_PATHS:
            pattern = '^' + re.escape(forbidden).replace('\\*', '.*') + '$'
            if re.search(pattern, href):
                skip = True
                break
        if skip:
            continue

        unique_hrefs.add(href)

    return sorted(unique_hrefs)


def process_file(relative_url: str, file_path: str) -> Optional[PLAYER_DATA]:
    if not os.path.exists(file_path):
        print(f"{file_path} aint exist")
        return None

    with open(file_path, "r", encoding="UTF-8") as infile:
        html = infile.read()

    data: PLAYER_DATA = {
        "file_path": file_path,
        "download_url": relative_url,
        "player_name": "",
        "position": "",
        "shoots": "",
        "height": 0,
        "weight": 0,
        "games_played": 0,
        "goals": 0,
        "assists": 0,
        "points": 0,
        "plus_minus": 0,
        "point_shares": 0.0,
        "penalty_minuts": 0,
        "shots_on_goal": 0,
        "game_winning_goals": 0,
    }

    name_match = re.search(r"<p>\s*<strong>\s*Full Name:\s*</strong>\s*([^<]+)\s*</p>", html)
    if name_match is None:
        return None
    data["player_name"] = name_match.group(1).strip()

    pos_shoots_match = re.search(
        r"<p>\s*<strong>\s*Position\s*</strong>\s*:\s*([^<]+?)&nbsp;.*?<strong>\s*Shoots\s*</strong>\s*:\s*([^<]+)",
        html,
        re.DOTALL,
    )
    if pos_shoots_match:
        data["position"] = pos_shoots_match.group(1).strip()
        data["shoots"] = pos_shoots_match.group(2).strip()

    hw_match = re.search(
        r"<p><span>[\d\-]+</span>,&nbsp;<span>\d+lb</span>&nbsp;\((\d+)cm,&nbsp;(\d+)kg\)",
        html,
    )
    if hw_match:
        data["height"] = int(hw_match.group(1))
        data["weight"] = int(hw_match.group(2))

    stats = {
        "Games Played": "games_played",
        "Goals": "goals",
        "Assists": "assists",
        "Points": "points",
        "Plus/Minus": "plus_minus",
        "Point Shares": "point_shares",
        "Penalties in Minutes": "penalty_minuts",
        "Shots on Goal": "shots_on_goal",
        "Game-Winning Goals": "game_winning_goals",
    }

    for tip, key in stats.items():
        match = re.search(
            rf'data-tip="[^"]*{re.escape(tip)}[^"]*".*?(?:<p>([^<]*)</p>)+',
            html,
            re.DOTALL,
        )
        if match:
            all_p = re.findall(r"<p>([^<]*)</p>", match.group(0))
            if all_p:
                last_value = all_p[-1].strip()
                try:
                    if key == "point_shares":
                        data[key] = float(last_value)
                    else:
                        data[key] = int(float(last_value))
                except ValueError:
                    data[key] = 0

    return data



def parse_downloaded_stuff(count_files: int | None = None):
    file_path = os.path.join(METADATA_DIR, 'all_metadata.tsv')
    with open(file_path, 'r', encoding='UTF-8') as infile:
        reader = csv.DictReader(infile, delimiter='\t')
        players_data: List[PLAYER_DATA] = []
        for row in reader:
            data = typing.cast(METADATA, row)
            player = process_file(data['download_url'], data['file_path'])
            
            if player is None:
                continue

            players_data.append(player)
            
            if count_files is not None and len(players_data) >= count_files:
                break
    
    out_path = os.path.join(PROCESSED_DIR, 'data.tsv')
    
    if not players_data:
        print("No player data found.")
        return

    with open(out_path, 'w', encoding='UTF-8', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=players_data[0].keys(), delimiter='\t')
        writer.writeheader()
        writer.writerows(players_data)

