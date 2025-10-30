import re
import csv
import os
from typing import List, Optional, Set, cast
import typing
from urllib.parse import ParseResult, urlparse
from config import BASE_URL, FIELD_PLAYER_STATS, FORBIDDEN_PATHS, GOALKEEPER_STATS, METADATA_DIR, PROCESSED_DIR
from object_types import METADATA, PLAYER_DATA
from html.parser import HTMLParser


def get_hrefs(html: str) -> List[str]:
    href_pattern = re.compile( # linky na inde domeny nechcem
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
        for forbidden in FORBIDDEN_PATHS: # druha cast filtra
            pattern = '^' + re.escape(forbidden).replace('\\*', '.*') + '$'
            if re.search(pattern, href):
                skip = True
                break
        if skip:
            continue

        unique_hrefs.add(href)

    return sorted(unique_hrefs)

class Parser(HTMLParser):
    def __init__(self, fp: str, url: str):
        super().__init__() 

        self.text = ""
        self.file_path = fp
        self.url = url
        self.current_index = 1


    def handle_data(self, data: str):
        self.text += data

    def process_file(self):
        data: PLAYER_DATA = {
            #"id": self.current_index,
            "file_path": self.file_path,
            "download_url": self.url,
            "player_name": "",
            "dob": "",
            "position": "",
            "draft_team": None,
            "hand": "",
            "height": None,
            "weight": None,
            "games_played": None,
            "wins": None,
            "losses": None,
            "ties_ot_losses": None,
            "minutes": None,
            "gaa": None,
            "save_percentage": None,
            "shootouts": None,
            "goals": None,
            "assists": None,
            "points": None,
            "plus_minus": None,
            "point_shares": None,
            "penalty_minutes": None,
            "shots_on_goal": None,
            "game_winning_goals": None,
        }

        name_match = re.search(r"Full Name:\s*([A-Za-zÀ-ÖØ-öø-ÿ' -]+)", self.text)
        if name_match is None:
            return None
        data['player_name'] = name_match.group(1).strip()

        pos_match = re.search(r"Position:\s*([A-Z]+)", self.text)
        if not pos_match:
            return None
        position = pos_match.group(1)
        data['position'] = position

        hand_match = re.search(r"(?:Catches|Shoots):\s*([A-Za-z]+)", self.text)
        if hand_match:
            data['hand'] = hand_match.group(1)

        hw_match = re.search(r"\((\d+)cm,\s*(\d+)kg\)", self.text)
        if hw_match:
            data['height'] = int(hw_match.group(1))
            data['weight'] = int(hw_match.group(2))

        dob_match = re.search(
            r"Born\s*:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})",
            self.text.replace('\xa0', ' '),
            flags=re.IGNORECASE
        )
        if dob_match:
            data['dob'] = dob_match.group(1).strip()

        draft_match = re.search(r"Draft:\s*([A-Za-z .'-]+),", self.text)
        
        if draft_match:
            data['draft_team'] = draft_match.group(1).strip()

        stats = GOALKEEPER_STATS if position == 'G' else FIELD_PLAYER_STATS

        for key, search_value in stats.items():
            pattern = rf"{re.escape(search_value)}-?[0-9]?\.?[0-9]*\s*[\r\n ]+(-?\d+(?:\.\d+)?|-?\.\d+)"
            match = re.search(pattern, self.text)
            if match:
                value = match.group(1)
                value = float(value) if "." in value else int(value)
                data[key] = value
        
        self.current_index += 1
        return data



def process_file(relative_url: str, file_path: str) -> Optional[PLAYER_DATA]:
    if not os.path.exists(file_path):
        print(f"{file_path} aint exist")
        return None

    with open(file_path, "r", encoding="UTF-8") as infile:
        html = infile.read()

    parser = Parser(file_path, relative_url)
    parser.feed(html)
    return parser.process_file()



    


def parse_downloaded_stuff(count_files: int | None = None):
    file_path = os.path.join(METADATA_DIR, 'all_metadata.tsv')
    with open(file_path, 'r', encoding='UTF-8') as infile:
        reader = csv.DictReader(infile, delimiter='\t')
        players_data: List[PLAYER_DATA] = []

        total_files = sum(1 for _ in open(file_path, encoding='UTF-8')) - 1
        infile.seek(0)
        next(reader)

        print(f'Total files: {total_files}')

        for idx, row in enumerate(reader, start=1):
            data = typing.cast(METADATA, row)
            if '/players' not in data['download_url']:
                print(f"  ⚠️ Skipped {data['file_path']} (irrelevant url)")
                continue
            print(f"[{idx}/{total_files}] Processing {data['file_path']}...")

            player = process_file(data['download_url'], data['file_path'])
            if player is None:
                print(f"  ⚠️ Skipped {data['file_path']} (player=None)")
                continue

            players_data.append(player)

            if count_files is not None and len(players_data) >= count_files:
                print(f"✅ Processed {len(players_data)} files, stopping early.")
                break
        
    if not players_data:
        print("No player data found.")
        return

    out_path = os.path.join(PROCESSED_DIR, 'data.tsv')
    with open(out_path, 'w', encoding='UTF-8', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=players_data[0].keys(), delimiter='\t')
        writer.writeheader()
        writer.writerows(players_data)