import re
import csv
import os
from typing import List, Optional, Set, cast
import typing
from urllib.parse import ParseResult, urlparse

from config import BASE_URL, FORBIDDEN_PATHS, HTML_DIR, METADATA_DIR, PROCESSED_DIR
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
        "dob": "",
        "position": "",
        "hand": "",
        "draft_team": None,
        "height": None,
        "weight": None,
        "games_played": None,
        "wins": None,
        "losses": None,
        "ties_ot_losses": None,
        "minutes": None,
        "gaa": None,
        "save_percentage": None,
        "goals": None,
        "assists": None,
        "points": None,
        "plus_minus": None,
        "point_shares": None,
        "penalty_minutes": None,
        "shots_on_goal": None,
        "game_winning_goals": None,
    }

    name_match = re.search(r"<p>\s*<strong>\s*Full Name:\s*</strong>\s*([^<]+)\s*</p>", html)
    if name_match is None:
        return None
    data["player_name"] = name_match.group(1).strip()

    pos_shoots_match = re.search(
        r"<p>\s*<strong>\s*Position\s*</strong>\s*:\s*([^<]+?)&nbsp;.*?<strong>\s*(?:Shoots|Catches)\s*</strong>\s*:\s*([^<]+)",
        html,
        re.DOTALL,
    )
    if pos_shoots_match:
        position = pos_shoots_match.group(1).strip()
        data["position"] = position
        data["hand"] = pos_shoots_match.group(2).strip()

    hw_match = re.search(
        r"<p><span>[\d\-]+</span>,&nbsp;<span>\d+lb</span>&nbsp;\((\d+)cm,&nbsp;(\d+)kg\)",
        html,
    )
    if hw_match:
        data["height"] = int(hw_match.group(1))
        data["weight"] = int(hw_match.group(2))

    field_player_stats = {
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

    gk_stats = {
        "Games Played": "games_played",
        "Wins": "wins",
        "Losses": "losses",
        "Ties plus Overtime/Shootout Losses": "ties_ot_losses",
        "Shutouts": "shootout",
        "Minutes": "minutes",
        "Goals Against Average": "gaa",
        "Save Percentage": "save_percentage"
    }

    stats = gk_stats if position == 'G' else field_player_stats

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


class Parser(HTMLParser):
    def __init__(self, fp: str, url: str):
        super().__init__() 

        self.text = ""
        self.file_path = fp
        self.url = url


    def handle_data(self, data: str):
        self.text += data

    def process_file(self):
        data: PLAYER_DATA = {
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

        field_player_stats = {
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

        gk_stats = {
            "games_played": "GP",
            "wins": "W",
            "losses": "L",
            "ties_ot_losses": "T",
            "shootout": "SHO",
            "point_shares": "PS",
            "minutes": "MIN",
            "gaa": "GAA",
            "save_percentage": "SV%"
        }

        stats = gk_stats if position == 'G' else field_player_stats

        for key, search_value in stats.items():
            pattern = rf"{re.escape(search_value)}\s*[\r\n ]+(-?\d+(?:\.\d+)?)"
            match = re.search(pattern, self.text)
            if match:
                value = match.group(1)
                value = float(value) if "." in value else int(value)
                data[key] = value
        

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



if __name__ == '__main__':
    with open(os.path.join(HTML_DIR, 'alvesjo01.html'), 'r', encoding='UTF-8') as file:
        data = file.read()
    f = Parser('.\\scraped\\players\\alvesjo01.html', 'https://www.hockey-reference.com/players/a/anderpe01.html')
    f.feed(data)
    print(f.process_file())