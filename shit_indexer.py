# type: ignore
import os
import csv
from whoosh import index
from whoosh.fields import Schema, TEXT, ID, NUMERIC
from whoosh.analysis import StemmingAnalyzer
from config import INDEX_DIR, PROCESSED_DIR

schema = Schema(
    file_path=ID(stored=True, unique=True),
    download_url=ID(stored=True),
    player_name=TEXT(stored=True, analyzer=StemmingAnalyzer()),
    dob=TEXT(stored=True),
    draft_team=TEXT(stored=True),
    position=TEXT(stored=True),
    hand=TEXT(stored=True),  # Catches/Shoots
    height=NUMERIC(stored=True),
    weight=NUMERIC(stored=True),
    point_shares=NUMERIC(stored=True, decimal_places=2),

    # Field player stats
    games_played=NUMERIC(stored=True),
    goals=NUMERIC(stored=True),
    assists=NUMERIC(stored=True),
    points=NUMERIC(stored=True),
    plus_minus=NUMERIC(stored=True, signed=True),
    penalty_minutes=NUMERIC(stored=True),
    shots_on_goal=NUMERIC(stored=True),
    game_winning_goals=NUMERIC(stored=True),

    # Goalkeeper stats
    wins=NUMERIC(stored=True),
    losses=NUMERIC(stored=True),
    ties_ot_losses=NUMERIC(stored=True),
    shootouts=NUMERIC(stored=True),
    minutes=NUMERIC(stored=True),
    gaa=NUMERIC(stored=True, decimal_places=2),
    save_percentage=NUMERIC(stored=True, decimal_places=3),
)

def load_index():
    if not os.path.exists(INDEX_DIR):
        raise Exception('Index dir doesn\'t exist.')
    
    return index.open_dir(INDEX_DIR, schema=schema)

def create_index():
    if not os.path.exists(INDEX_DIR):
        os.mkdir(INDEX_DIR)
    return index.create_in(INDEX_DIR, schema)

def safe_int(value):
    try:
        return int(value) if value else 0
    except ValueError:
        return 0

def safe_float(value):
    try:
        return float(value) if value else 0.0
    except ValueError:
        return 0.0

def make_index():
    file_path = os.path.join(PROCESSED_DIR, 'data.tsv')
    ix = create_index()
    writer = ix.writer()
    
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        
        for row in reader:
            writer.add_document(
                file_path=row['file_path'],
                download_url=row['download_url'],
                player_name=row['player_name'],
                dob=row.get('dob', ''),
                draft_team=row.get('draft_team', ''),
                position=row.get('position', ''),
                hand=row.get('hand', ''),
                height=safe_int(row.get('height')),
                weight=safe_int(row.get('weight')),
                games_played=safe_int(row.get('games_played')),
                wins=safe_int(row.get('wins')),
                losses=safe_int(row.get('losses')),
                ties_ot_losses=safe_int(row.get('ties_ot_losses')),
                shootouts=safe_int(row.get('shootouts')),
                minutes=safe_int(row.get('minutes')),
                gaa=safe_float(row.get('gaa')),
                save_percentage=safe_float(row.get('save_percentage')),
                goals=safe_int(row.get('goals')),
                assists=safe_int(row.get('assists')),
                points=safe_int(row.get('points')),
                plus_minus=safe_int(row.get('plus_minus')),
                point_shares=safe_float(row.get('point_shares')),
                penalty_minutes=safe_int(row.get('penalty_minutes')),
                shots_on_goal=safe_int(row.get('shots_on_goal')),
                game_winning_goals=safe_int(row.get('game_winning_goals'))
            )
    
    writer.commit()
    return ix

