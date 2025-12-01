import lucene
from object_types import PLAYER_DATA, WIKI_PLAYER
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader, Term
from org.apache.lucene.analysis.standard import StandardAnalyzer
from java.nio.file import Paths
from org.apache.lucene.document import Document, Field, TextField, StoredField, IntPoint, DoublePoint, DoubleDocValuesField, NumericDocValuesField
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause, BooleanQuery, BooleanClause, TermQuery, BoostQuery, MatchAllDocsQuery, PhraseQuery, Sort, SortField, TopScoreDocCollector
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
            doc.add(NumericDocValuesField("height", safe_int(player['height'])))
        if safe_int(player['weight']) is not None:
            doc.add(IntPoint("weight", safe_int(player['weight'])))
            doc.add(StoredField("weight", safe_int(player['weight'])))
            doc.add(NumericDocValuesField("weight", safe_int(player['weight'])))
        if safe_int(player['games_played']) is not None:
            doc.add(IntPoint("games_played", safe_int(player['games_played'])))
            doc.add(StoredField("games_played", safe_int(player['games_played'])))
            doc.add(NumericDocValuesField("games_played", safe_int(player['games_played'])))
        if safe_int(player['wins']) is not None:
            doc.add(IntPoint("wins", safe_int(player['wins'])))
            doc.add(StoredField("wins", safe_int(player['wins'])))
            doc.add(NumericDocValuesField("wins", safe_int(player['wins'])))
        if safe_int(player['losses']) is not None:
            doc.add(IntPoint("losses", safe_int(player['losses'])))
            doc.add(StoredField("losses", safe_int(player['losses'])))
            doc.add(NumericDocValuesField("losses", safe_int(player['losses'])))
        if safe_int(player['ties_ot_losses']) is not None:
            doc.add(IntPoint("ties_ot_losses", safe_int(player['ties_ot_losses'])))
            doc.add(StoredField("ties_ot_losses", safe_int(player['ties_ot_losses'])))
            doc.add(NumericDocValuesField("ties_ot_losses", safe_int(player['ties_ot_losses'])))
        if safe_int(player['minutes']) is not None:
            doc.add(IntPoint("minutes", safe_int(player['minutes'])))
            doc.add(StoredField("minutes", safe_int(player['minutes'])))
            doc.add(NumericDocValuesField("minutes", safe_int(player['minutes'])))
        if safe_int(player['goals']) is not None:
            doc.add(IntPoint("goals", safe_int(player['goals'])))
            doc.add(StoredField("goals", safe_int(player['goals'])))
            doc.add(NumericDocValuesField("goals", safe_int(player['goals'])))
        if safe_int(player['assists']) is not None:
            doc.add(IntPoint("assists", safe_int(player['assists'])))
            doc.add(StoredField("assists", safe_int(player['assists'])))
            doc.add(NumericDocValuesField("assists", safe_int(player['assists'])))
        if safe_int(player['points']) is not None:
            doc.add(IntPoint("points", safe_int(player['points'])))
            doc.add(StoredField("points", safe_int(player['points'])))
            doc.add(NumericDocValuesField("points", safe_int(player['points'])))
        if safe_int(player['plus_minus']) is not None:
            doc.add(IntPoint("plus_minus", safe_int(player['plus_minus'])))
            doc.add(StoredField("plus_minus", safe_int(player['plus_minus'])))
            doc.add(NumericDocValuesField("plus_minus", safe_int(player['plus_minus'])))
        if safe_int(player['penalty_minutes']) is not None:
            doc.add(IntPoint("penalty_minutes", safe_int(player['penalty_minutes'])))
            doc.add(StoredField("penalty_minutes", safe_int(player['penalty_minutes'])))
            doc.add(NumericDocValuesField("penalty_minutes", safe_int(player['penalty_minutes'])))
        if safe_int(player['shots_on_goal']) is not None:
            doc.add(IntPoint("shots_on_goal", safe_int(player['shots_on_goal'])))
            doc.add(StoredField("shots_on_goal", safe_int(player['shots_on_goal'])))
            doc.add(NumericDocValuesField("shots_on_goal", safe_int(player['shots_on_goal'])))
        if safe_int(player['game_winning_goals']) is not None:
            doc.add(IntPoint("game_winning_goals", safe_int(player['game_winning_goals'])))
            doc.add(StoredField("game_winning_goals", safe_int(player['game_winning_goals'])))
            doc.add(NumericDocValuesField("game_winning_goals", safe_int(player['game_winning_goals'])))
        if safe_int(player['career_start']) is not None:
            doc.add(IntPoint("career_start", safe_int(player['career_start'])))
            doc.add(StoredField("career_start", safe_int(player['career_start'])))
            doc.add(NumericDocValuesField("career_start", safe_int(player['career_start'])))
        if safe_int(player['career_end']) is not None:
            doc.add(IntPoint("career_end", safe_int(player['career_end'])))
            doc.add(StoredField("career_end", safe_int(player['career_end'])))
            doc.add(NumericDocValuesField("career_end", safe_int(player['career_end'])))
        
        # Float/Double fields
        if safe_float(player['shootouts']) is not None:
            doc.add(DoublePoint("shootouts", safe_float(player['shootouts'])))
            doc.add(StoredField("shootouts", safe_float(player['shootouts'])))
            doc.add(DoubleDocValuesField("shootouts", safe_float(player['shootouts'])))
        if safe_float(player['gaa']) is not None:
            doc.add(DoublePoint("gaa", safe_float(player['gaa'])))
            doc.add(StoredField("gaa", safe_float(player['gaa'])))
            doc.add(DoubleDocValuesField("gaa", safe_float(player['gaa'])))
        if safe_float(player['save_percentage']) is not None:
            doc.add(DoublePoint("save_percentage", safe_float(player['save_percentage'])))
            doc.add(StoredField("save_percentage", safe_float(player['save_percentage'])))
            doc.add(DoubleDocValuesField("save_percentage", safe_float(player['save_percentage'])))
        if safe_float(player['point_shares']) is not None:
            doc.add(DoublePoint("point_shares", safe_float(player['point_shares'])))
            doc.add(StoredField("point_shares", safe_float(player['point_shares'])))
            doc.add(DoubleDocValuesField("point_shares", safe_float(player['point_shares'])))
        
        
        # Stored only - not searchable
        if player['file_path']:
            doc.add(StoredField("file_path", str(player['file_path'])))
        if player['download_url']:
            doc.add(StoredField("download_url", str(player['download_url'])))
        
        writer.addDocument(doc)

    writer.commit()
    writer.close()

def print_result(i, hit, doc):
    print(f"{'='*80}")
    print(f"#{i+1}")
    print(f"{'='*40}")
    
    print(f"Player:       {doc.get('player_name') or 'N/A'}")
    print(f"DOB:          {doc.get('dob') or 'N/A'}")
    
    print(f"\n{'-'*80}")
    print("Player Stats:")
    print(f"{'-'*80}")
    
    stats = [
        ('Title', doc.get('title')),
        ('Full Name', doc.get('full_name')),
        ('Position', doc.get('position')),
        ('Hand', doc.get('hand')),
        ('Birthplace', doc.get('birthplace')),
        ('Height', doc.get('height')),
        ('Weight', doc.get('weight')),
        ('Current Team', doc.get('current_team')),
        ('Current League', doc.get('current_league')),
        ('National Team', doc.get('national_team')),
        ('Nationality', doc.get('nationality')),
        ('Draft', doc.get('draft')),
        ('Draft Year', doc.get('draft_year')),
        ('Draft Team', doc.get('draft_team')),
        ('Career Start', doc.get('career_start')),
        ('Career End', doc.get('career_end')),
        ('Games Played', doc.get('games_played')),
        ('Goals', doc.get('goals')),
        ('Assists', doc.get('assists')),
        ('Points', doc.get('points')),
        ('Plus Minus', doc.get('plus_minus')),
        ('Penalty Minutes', doc.get('penalty_minutes')),
        ('Shots On Goal', doc.get('shots_on_goal')),
        ('Game Winning Goals', doc.get('game_winning_goals')),
        ('Wins', doc.get('wins')),
        ('Losses', doc.get('losses')),
        ('Ties OT Losses', doc.get('ties_ot_losses')),
        ('Minutes', doc.get('minutes')),
        ('Shootouts', doc.get('shootouts')),
        ('GAA', doc.get('gaa')),
        ('Save Percentage', doc.get('save_percentage')),
        ('Point Shares', doc.get('point_shares')),
    ]
    
    for label, value in stats:
        if value:
            print(f"{label:20s}: {value}")
    
    print(f"\n{'-'*80}")
    print(f"URL:          {doc.get('download_url') or 'N/A'}")
    print()


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

def build_lucene_query(parsed_query):
    """
    Build a Lucene query from parsed query tokens.
    
    Args:
        parsed_query: Dictionary with 'limit' and 'query_tokens'
        all_fields: List of all searchable field names
        numeric_fields: Dictionary of numeric field names (to exclude from text search)
        default_search_field: Default field to search in for text queries (unused now)
    
    Returns:
        Tuple of (BooleanQuery, sort_fields)
        - BooleanQuery: The Lucene query object
        - sort_fields: List of (field_name, reverse) tuples for sorting
    """
    builder = BooleanQuery.Builder()
    sort_fields = []
    has_text_query = False
    
    boosted_fields = {
        'player_name': 2.0,
        'current_league': 2.0,
        'current_team': 2.0
    }

    text_fields = {'title', 'player_name', 'full_name', 'dob', 'position', 'hand', 'birthplace', 'draft', 'draft_year', 'draft_team', 'current_league', 'national_team', 'current_team', 'nationality'}

        
    for token in parsed_query['query_tokens']:
        if token['field'] is not None:
            reverse = (token['order'] == 'desc')
            sort_fields.append((token['field'], reverse))
        else:
            search_value = token['value']
            
            field_query_builder = BooleanQuery.Builder()
            
            for field in text_fields:
                if ' ' in search_value:
                    phrase_builder = PhraseQuery.Builder()
                    words = search_value.split()
                    for word in words:
                        phrase_builder.add(Term(field, word.lower()))
                    phrase_query = phrase_builder.build()
                    
                    if field in boosted_fields:
                        boosted_query = BoostQuery(phrase_query, boosted_fields[field])
                        field_query_builder.add(boosted_query, BooleanClause.Occur.SHOULD)
                    else:
                        field_query_builder.add(phrase_query, BooleanClause.Occur.SHOULD)
                else:
                    term_query = TermQuery(Term(field, search_value.lower()))
                    
                    if field in boosted_fields:
                        boosted_query = BoostQuery(term_query, boosted_fields[field])
                        field_query_builder.add(boosted_query, BooleanClause.Occur.SHOULD)
                    else:
                        field_query_builder.add(term_query, BooleanClause.Occur.SHOULD)

            field_query = field_query_builder.build()
            builder.add(field_query, BooleanClause.Occur.MUST)
    
    lucene_query = builder.build()
    
    return lucene_query, sort_fields

def parse_full_text_query(query_string: str):
    numeric_fields = {
        'height',
        'weight',
        'games_played',
        'wins',
        'losses',
        'ties_ot_losses',
        'minutes',
        'goals',
        'assists',
        'points',
        'plus_minus',
        'penalty_minutes',
        'shots_on_goal',
        'game_winning_goals',
        'career_start',
        'career_end',
        'shootouts',
        'gaa',
        'save_percentage',
        'point_shares'
    }

    result = {
        'limit': None,
        'query_tokens': []
    }

    query = query_string.strip().lower()
    top_k_pattern = r'^top\s+(\d+)\s+'
    top_k_match = re.match(top_k_pattern, query)

    if top_k_match:
        result['limit'] = int(top_k_match.group(1))
        query = query[top_k_match.end():]

    order_keywords = {
        'highest': 'desc',
        'most': 'desc',
        'best': 'desc',
        'greatest': 'desc',
        'top': 'desc',
        'maximum': 'desc',
        'max': 'desc',
        'lowest': 'asc',
        'least': 'asc',
        'worst': 'asc',
        'minimum': 'asc',
        'min': 'asc',
        'fewest': 'asc'
    }

    field_display_to_name = {}
    for field_name in numeric_fields:
        display_name = field_name.replace('_', ' ')
        field_display_to_name[display_name] = field_name

        tokens = []
    
    token_pattern = r'"([^"]+)"|(\S+)'
    
    for match in re.finditer(token_pattern, query):
        if match.group(1):  # Quoted string
            tokens.append(match.group(1))
        else:  # Single word
            tokens.append(match.group(2))
    
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token in order_keywords:
            order = order_keywords[token]
            
            field_found = False
            
            for field_length in range(len(field_display_to_name), 0, -1):
                if i + field_length < len(tokens):
                    potential_field = ' '.join(tokens[i+1:i+1+field_length])
                    
                    if potential_field in field_display_to_name:
                        field_name = field_display_to_name[potential_field]
                        result['query_tokens'].append({
                            'field': field_name,
                            'value': None,
                            'order': order
                        })
                        i += field_length + 1 
                        field_found = True
                        break
            
            if not field_found:
                result['query_tokens'].append({
                    'field': None,
                    'value': token,
                    'order': None
                })
                i += 1
        else:
            result['query_tokens'].append({
                'field': None,
                'value': token,
                'order': None
            })
            i += 1
    
    return result



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
        
        parsed_query = parse_full_text_query(query_str)
        query, sort_fields = build_lucene_query(parsed_query)

        limit = parsed_query['limit'] if parsed_query['limit'] else 5
        
        if sort_fields:
            # Build Sort object
            sort_field_objects = []
            for field_name, reverse in sort_fields:
                # Determine the field type for sorting
                if field_name in ['shootouts', 'gaa', 'save_percentage', 'point_shares']:
                    sort_field = SortField(field_name, SortField.Type.DOUBLE, reverse)
                else:
                    sort_field = SortField(field_name, SortField.Type.INT, reverse)
                sort_field_objects.append(sort_field)
            
            sort = Sort(*sort_field_objects)
            hits = searcher.search(query, limit, sort)
        else:
            hits = searcher.search(query, limit, Sort.RELEVANCE)
            
        print(f"\nFound {hits.totalHits.value()} results:\n")
        
        for i, hit in enumerate(hits.scoreDocs):
            doc = searcher.storedFields().document(hit.doc)
            print_result(i, hit, doc)

    reader.close()