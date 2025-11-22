import lucene
from object_types import PLAYER_DATA, WIKI_PLAYER
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.analysis.standard import StandardAnalyzer
from java.nio.file import Paths
from org.apache.lucene.document import Document, Field, TextField, StringField, StoredField, IntPoint, DoublePoint, LongPoint
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser

lucene.initVM()

from typing import List, Union
import pandas as pd
import joblib
index_path = "./index"  # Change this to where you want the index
index_dir = MMapDirectory(Paths.get(index_path))

analyzer = StandardAnalyzer()
config = IndexWriterConfig(analyzer)

writer = IndexWriter(index_dir, config)

def safe_int(input_string: str) -> int | None:
    if not isinstance(input_string, (str, int)) or input_string is None:
        if isinstance(input_string, int):
            return input_string
        return None

    try:
        return int(input_string)
    except ValueError:
        return None
    
def safe_float(input_string: str) -> float | None:
    if not isinstance(input_string, (str, float)) or input_string is None:
        if isinstance(input_string, float):
            return input_string
        return None

    try:
        return float(input_string)
    except ValueError:
        return None


def make_index(data: List[Union[PLAYER_DATA, WIKI_PLAYER]]):
    for player in data:
        doc = Document()
        
        # Text fields - searchable AND stored
        if player['player_name']:
            doc.add(TextField("player_name", str(player['player_name']), Field.Store.YES))
        if player['full_name']:
            doc.add(TextField("full_name", str(player['full_name']), Field.Store.YES))
        if player['dob']:
            doc.add(TextField("dob", str(player['dob']), Field.Store.YES))
        if player['position']:
            doc.add(TextField("position", str(player['position']), Field.Store.YES))
        if player['hand']:
            doc.add(TextField("hand", str(player['hand']), Field.Store.YES))
        if player['birthplace']:
            doc.add(TextField("birthplace", str(player['birthplace']), Field.Store.YES))
        if player['draft']:
            doc.add(TextField("draft", str(player['draft']), Field.Store.YES))
        if player['draft_year']:
            doc.add(TextField("draft_year", str(player['draft_year']), Field.Store.YES))
        if player['draft_team']:
            doc.add(TextField("draft_team", str(player['draft_team']), Field.Store.YES))
        if player['current_league']:
            doc.add(TextField("current_league", str(player['current_league']), Field.Store.YES))
        if player['national_team']:
            doc.add(TextField("national_team", str(player['national_team']), Field.Store.YES))
        if player['current_team']:
            doc.add(TextField("current_team", str(player['current_team']), Field.Store.YES))
        if player['nationality']:
            doc.add(TextField("nationality", str(player['nationality']), Field.Store.YES))
        
        # Integer fields - searchable (range queries) AND stored
        if safe_int(player['height']) is not None:
            doc.add(IntPoint("height", safe_int(player['height'])))
            doc.add(StoredField("height", safe_int(player['height'])))
        if safe_int(player['weight']) is not None:
            doc.add(IntPoint("weight", safe_int(player['weight'])))
            doc.add(StoredField("weight", safe_int(player['weight'])))
        if safe_int(player['games_played']) is not None:
            doc.add(IntPoint("games_played", safe_int(player['games_played'])))
            doc.add(StoredField("games_played", safe_int(player['games_played'])))
        if safe_int(player['wins']) is not None:
            doc.add(IntPoint("wins", safe_int(player['wins'])))
            doc.add(StoredField("wins", safe_int(player['wins'])))
        if safe_int(player['losses']) is not None:
            doc.add(IntPoint("losses", safe_int(player['losses'])))
            doc.add(StoredField("losses", safe_int(player['losses'])))
        if safe_int(player['ties_ot_losses']) is not None:
            doc.add(IntPoint("ties_ot_losses", safe_int(player['ties_ot_losses'])))
            doc.add(StoredField("ties_ot_losses", safe_int(player['ties_ot_losses'])))
        if safe_int(player['minutes']) is not None:
            doc.add(IntPoint("minutes", safe_int(player['minutes'])))
            doc.add(StoredField("minutes", safe_int(player['minutes'])))
        if safe_int(player['goals']) is not None:
            doc.add(IntPoint("goals", safe_int(player['goals'])))
            doc.add(StoredField("goals", safe_int(player['goals'])))
        if safe_int(player['assists']) is not None:
            doc.add(IntPoint("assists", safe_int(player['assists'])))
            doc.add(StoredField("assists", safe_int(player['assists'])))
        if safe_int(player['points']) is not None:
            doc.add(IntPoint("points", safe_int(player['points'])))
            doc.add(StoredField("points", safe_int(player['points'])))
        if safe_int(player['plus_minus']) is not None:
            doc.add(IntPoint("plus_minus", safe_int(player['plus_minus'])))
            doc.add(StoredField("plus_minus", safe_int(player['plus_minus'])))
        if safe_int(player['penalty_minutes']) is not None:
            doc.add(IntPoint("penalty_minutes", safe_int(player['penalty_minutes'])))
            doc.add(StoredField("penalty_minutes", safe_int(player['penalty_minutes'])))
        if safe_int(player['shots_on_goal']) is not None:
            doc.add(IntPoint("shots_on_goal", safe_int(player['shots_on_goal'])))
            doc.add(StoredField("shots_on_goal", safe_int(player['shots_on_goal'])))
        if safe_int(player['game_winning_goals']) is not None:
            doc.add(IntPoint("game_winning_goals", safe_int(player['game_winning_goals'])))
            doc.add(StoredField("game_winning_goals", safe_int(player['game_winning_goals'])))
        if safe_int(player['career_start']) is not None:
            doc.add(IntPoint("career_start", safe_int(player['career_start'])))
            doc.add(StoredField("career_start", safe_int(player['career_start'])))
        if safe_int(player['career_end']) is not None:
            doc.add(IntPoint("career_end", safe_int(player['career_end'])))
            doc.add(StoredField("career_end", safe_int(player['career_end'])))
        
        # Float/Double fields - searchable AND stored
        if safe_float(player['shootouts']) is not None:
            doc.add(DoublePoint("shootouts", safe_float(player['shootouts'])))
            doc.add(StoredField("shootouts", safe_float(player['shootouts'])))
        if safe_float(player['gaa']) is not None:
            doc.add(DoublePoint("gaa", safe_float(player['gaa'])))
            doc.add(StoredField("gaa", safe_float(player['gaa'])))
        if safe_float(player['save_percentage']) is not None:
            doc.add(DoublePoint("save_percentage", safe_float(player['save_percentage'])))
            doc.add(StoredField("save_percentage", safe_float(player['save_percentage'])))
        if safe_float(player['point_shares']) is not None:
            doc.add(DoublePoint("point_shares", safe_float(player['point_shares'])))
            doc.add(StoredField("point_shares", safe_float(player['point_shares'])))
        
        # Stored only - not searchable
        if player['file_path']:
            doc.add(StoredField("file_path", str(player['file_path'])))
        if player['download_url']:
            doc.add(StoredField("download_url", str(player['download_url'])))
        
        writer.addDocument(doc)

    writer.commit()
    writer.close()


def load_df(file_path = './processed_pages.joblib'):
    pd_df: pd.DataFrame = joblib.load(file_path)
    return pd_df.to_dict('records')


if __name__ == '__main__':
    rows = load_df('./left_merged.joblib')

    make_index(rows)

    reader = DirectoryReader.open(index_dir)
    searcher = IndexSearcher(reader)

    text_fields = ["player_name", "full_name", "position", "birthplace", "hand",
               "nationality", "draft_team", "current_team", "national_team",
               "draft", "draft_year", "current_league", "dob"]


    while True:
        query_str = input("Enter search query (or 'quit' to exit): ")
        
        if query_str.lower() == 'quit':
            break
        
        # Parse using static method
        query = MultiFieldQueryParser.parse(
            query_str, 
            text_fields, 
            [BooleanClause.Occur.SHOULD] * len(text_fields),  # OR across fields
            analyzer
        )
        hits = searcher.search(query, 10)
            
        print(f"\nFound {hits.totalHits.value()} results:\n")
        
        # Display results
        for i, hit in enumerate(hits.scoreDocs):
            doc = searcher.storedFields().document(hit.doc)
            print(f"{i+1}. {doc.get('player_name')} - {doc.get('position')}")
            print(f"   Team: {doc.get('current_team')}")
            print(f"   URL: {doc.get('download_url')}\n")

    # Close when done
    reader.close()