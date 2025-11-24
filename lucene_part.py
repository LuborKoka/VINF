import lucene
from object_types import PLAYER_DATA, WIKI_PLAYER
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader, Term
from org.apache.lucene.analysis.standard import StandardAnalyzer
from java.nio.file import Paths
from org.apache.lucene.document import Document, Field, TextField, StoredField, IntPoint, DoublePoint
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause, BooleanQuery, BooleanClause, TermQuery, BoostQuery
import csv
import re

lucene.initVM()

from typing import List, Union

index_path = "./index" 
index_dir = MMapDirectory(Paths.get(index_path))

analyzer = StandardAnalyzer()
config = IndexWriterConfig(analyzer)

writer = IndexWriter(index_dir, config)

def safe_int(input_string: str | None) -> int | None:
    if not input_string:
        return None
    
    if input_string == 'NaN':
        return None
    
    return int(float(input_string))
    
def safe_float(input_string: str | None) -> float | None:
    if not input_string:
        return None
    if input_string == 'NaN':
        return None
    return float(input_string)


def make_index(data: List[Union[PLAYER_DATA, WIKI_PLAYER]]):
    for player in data:
        doc = Document()
        
        # Text fields
        if player['title']:
            doc.add(TextField("title", str(player['title']), Field.Store.YES))
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
        
        # Integer fields
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
        
        # Float/Double fields
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

def print_result(i, hit, doc, queried_fields):
    print(f"{'='*80}")
    print(f"#{i+1} | Score: {hit.score:.4f}")
    print(f"{'='*80}")
    
    print(f"Player:       {doc.get('player_name') or 'N/A'}")
    print(f"DOB:          {doc.get('dob') or 'N/A'}")
    
    if queried_fields:
        print(f"\n{'-'*80}")
        print("Queried Fields:")
        print(f"{'-'*80}")
        
        for field in queried_fields: 
            value = str(doc.get(field))
            if value:
                field_name = field.replace('_', ' ').title()
                print(f"{field_name:20s}: {value}")
    else:
        print(f"\n{'-'*80}")
        print("Player Stats:")
        print(f"{'-'*80}")
        
        stats = [
            ('Position', doc.get('position')),
            ('Current Team', doc.get('current_team')),
            ('Nationality', doc.get('nationality')),
            ('Height', doc.get('height')),
            ('Weight', doc.get('weight')),
            ('Goals', doc.get('goals')),
            ('Assists', doc.get('assists')),
            ('Points', doc.get('points')),
            ('Goals', doc.get('goals')),
        ]
        
        for label, value in stats:
            if value:
                print(f"{label:20s}: {value}")
    
    print(f"\n{'-'*80}")
    print(f"URL:          {doc.get('download_url') or 'N/A'}")
    print()

import re

def parse_query_string(query_str, text_fields, numeric_fields):
    """
    Parse a query string into structured field queries.
    
    Example:
        "dob january 1995 and national_team canada and goals [200 TO 500]"
        ->
        [
            {'field': 'dob', 'tokens': ['january', '1995'], 'type': 'text'},
            {'field': 'national_team', 'tokens': ['canada'], 'type': 'text'},
            {'field': 'goals', 'range': (200, 500), 'type': 'numeric'}
        ]
    """
    parsed_queries = []
    
    all_fields = text_fields + list(numeric_fields.keys())
    
    segments = re.split(r'\s+(?:and|or)\s+', query_str, flags=re.IGNORECASE)
    
    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue
        
        matched_field = None
        for field in all_fields:
            if segment.strip().startswith(field):
                matched_field = field
                break
        
        if not matched_field:
            continue
        
        value_part = segment[len(matched_field):].strip()
        
        if matched_field in numeric_fields:
            range_match = re.match(r'\[(\d+(?:\.\d+)?)\s+TO\s+(\d+(?:\.\d+)?)\]', value_part)
            if range_match:
                point_type = numeric_fields[matched_field]
                if point_type == DoublePoint:
                    min_val = float(range_match.group(1))
                    max_val = float(range_match.group(2))
                else: 
                    min_val = int(float(range_match.group(1)))
                    max_val = int(float(range_match.group(2)))
                
                parsed_queries.append({
                    'field': matched_field,
                    'range': (min_val, max_val),
                    'type': 'numeric'
                })
            else:
                try:
                    point_type = numeric_fields[matched_field]
                    if point_type == DoublePoint:
                        value = float(value_part)
                    else:
                        value = int(float(value_part))
                    
                    parsed_queries.append({
                        'field': matched_field,
                        'value': value,
                        'type': 'numeric'
                    })
                except ValueError:
                    continue
        else:
            tokens = re.findall(r'\b\w+\b', value_part.lower())
            
            if tokens:
                parsed_queries.append({
                    'field': matched_field,
                    'tokens': tokens,
                    'type': 'text'
                })
    
    return parsed_queries

def build_lucene_query(parsed_queries, numeric_fields):
    """
    Build a Lucene BooleanQuery from parsed query structure.
    
    For text fields: all tokens must be present (AND logic within field)
    For numeric fields: apply as range filters
    """

    
    builder = BooleanQuery.Builder()
    
    for pq in parsed_queries:
        if pq['type'] == 'text':
            field = pq['field']
            tokens = pq['tokens']
            
            if len(tokens) == 1:
                term = Term(field, tokens[0])
                term_query = TermQuery(term)
                builder.add(term_query, BooleanClause.Occur.MUST)
            else:
                sub_builder = BooleanQuery.Builder()
                for token in tokens:
                    term = Term(field, token)
                    term_query = TermQuery(term)
                    sub_builder.add(term_query, BooleanClause.Occur.MUST)
                
                builder.add(sub_builder.build(), BooleanClause.Occur.MUST)
        
        elif pq['type'] == 'numeric':
            field = pq['field']
            
            if 'range' in pq:
                min_val, max_val = pq['range']
                point_type = numeric_fields[field]
                range_query = point_type.newRangeQuery(field, min_val, max_val)
                builder.add(range_query, BooleanClause.Occur.MUST)
            elif 'value' in pq:
                value = pq['value']
                point_type = numeric_fields[field]
                exact_query = point_type.newExactQuery(field, value)
                builder.add(exact_query, BooleanClause.Occur.MUST)
    
    return builder.build()

def parse_query_with_numerics(query_str):
    numeric_fields = {
        'height': IntPoint,
        'weight': IntPoint,
        'games_played': IntPoint,
        'wins': IntPoint,
        'losses': IntPoint,
        'ties_ot_losses': IntPoint,
        'minutes': IntPoint,
        'goals': IntPoint,
        'assists': IntPoint,
        'points': IntPoint,
        'plus_minus': IntPoint,
        'penalty_minutes': IntPoint,
        'shots_on_goal': IntPoint,
        'game_winning_goals': IntPoint,
        'career_start': IntPoint,
        'career_end': IntPoint,
        'shootouts': DoublePoint,
        'gaa': DoublePoint,
        'save_percentage': DoublePoint,
        'point_shares': DoublePoint
    }

    text_fields = ["player_name", "full_name", "position", "birthplace", "hand",
               "nationality", "draft_team", "current_team", "national_team",
               "draft", "draft_year", "current_league", "dob"]
    

    queries = parse_query_string(query_str, text_fields, numeric_fields)

    if len(queries) == 0:
        tokens = re.findall(r'\b\w+\b', query_str.lower())

        boosts = {
            'player_name': 3.0,
            'full_name': 3.0,
            'title': 2.0,
            'dob': 2.0,
            'draft_team': 1.5,
            'current_team': 1.5
        }
        
        builder = BooleanQuery.Builder()
        
        for token in tokens:
            field_builder = BooleanQuery.Builder()
            
            for field in text_fields:
                term = Term(field, token)
                term_query = TermQuery(term)

                if field in boosts:
                    boosted_query = BooleanQuery.Builder()
                    boosted_query.add(term_query, BooleanClause.Occur.SHOULD)
                    built_query = boosted_query.build()
                    term_query = BoostQuery(built_query, boosts[field])

                field_builder.add(term_query, BooleanClause.Occur.SHOULD)
            
            builder.add(field_builder.build(), BooleanClause.Occur.MUST)
        
        query = builder.build()
        return query, []

    query = build_lucene_query(queries, numeric_fields)

    queried_fields = []
     
    for q in queries:
        if q['field'] in query_str and q['field'] not in ['player_name', 'dob', 'download_url']:
            queried_fields.append(q['field'])

    return query, queried_fields

def load_df(file_path='./processed_pages.tsv'):
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        return list(reader)


if __name__ == '__main__':
    rows = load_df('./df/merged_df.tsv')

    make_index(rows)

    reader = DirectoryReader.open(index_dir)
    searcher = IndexSearcher(reader)


    while True:
        query_str = input("Enter search query (or 'quit' to exit): ")
        
        if query_str.lower() == 'quit':
            break
        
        query, queried_fields = parse_query_with_numerics(query_str)
        hits = searcher.search(query, 5)
            
        print(f"\nFound {hits.totalHits.value()} results:\n")
        
        for i, hit in enumerate(hits.scoreDocs):
            doc = searcher.storedFields().document(hit.doc)
            print_result(i, hit, doc, queried_fields)

    reader.close()