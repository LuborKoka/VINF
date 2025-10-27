import math
import os
import csv
from typing import Counter, Dict, Optional, List, Sequence, Set, Tuple, Union, cast
from object_types import INDEX, PLAYER_DATA
from config import JOBLIB_DIR, PROCESSED_DIR
import bisect
import joblib # type: ignore
from enum import Enum

class POSITION(Enum):
    FORWARD = 'F'  
    GOALKEEPER = 'G'            
    DEFENSE = 'D'         
    CENTER = 'C'            
    RIGHT_SIDE = 'R'   
    LEFT_SIDE = 'L'     
    WINGER_GENERAL = 'W'    
    RIGHT_WING = 'RW'       
    LEFT_WING = 'LW' 


class IDF(Enum):
    SMOOTH = 'smooth'
    PROBABILISTIC = 'probabilistic'
    STANDARD = 'standard'

class Index:
    def __init__(self) -> None:
        self.text_fields = ['player_name', 'dob', 'position', 'draft_team', 'hand']
        self.numeric_fields = ['height', 'weight', 'games_played', 'wins', 'losses', 
                      'ties_ot_losses', 'minutes', 'shootouts', 'gaa', 
                      'save_percentage', 'goals', 'assists', 'points', 
                      'plus_minus', 'point_shares', 'penalty_minutes', 
                      'shots_on_goal', 'game_winning_goals']

        self.data: List[PLAYER_DATA] = []

        self.index: INDEX = {
            "player_name": {},
            "dob": {},
            "draft_team": {},
            "position": {},
            "hand": {},
            "height": None,
            "weight": None,
            "games_played": None,
            "wins": None,
            "losses": None,
            "ties_ot_losses": None,
            "minutes": None,
            "shootouts": None,
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

        self.tf: Dict[str, Dict[int, Dict[str, int]]] = {}
        self.df: Dict[str, Dict[str, int]] = {}
        self.doc_lengths: Dict[str, Dict[int, int]] = {}

    @staticmethod
    def _safe_int(val: str) -> Optional[int]:
        return int(val) if val else None

    @staticmethod
    def _safe_float(val: str) -> Optional[float]:
        return float(val) if val else None


    def load_tsv(self):
        tsv_path = os.path.join(PROCESSED_DIR, 'data_with_id.tsv')

        with open(tsv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                player_dict: PLAYER_DATA = {
                    # Non-Optional fields
                    #'id': int(row['id']),
                    'file_path': row['file_path'],
                    'download_url': row['download_url'],
                    'player_name': row['player_name'],
                    'dob': row.get('dob', ''),
                    'position': row.get('position', ''),
                    'hand': row.get('hand', ''),
                    'draft_team': row.get('draft_team', '') ,

                    'height': self._safe_int(row['height']),
                    'weight': self._safe_int(row['weight']),
                    'games_played': self._safe_int(row['games_played']),
                    'wins': self._safe_int(row['wins']),
                    'losses': self._safe_int(row['losses']),
                    'ties_ot_losses': self._safe_int(row['ties_ot_losses']),
                    'minutes': self._safe_int(row['minutes']),
                    'goals': self._safe_int(row['goals']),
                    'assists': self._safe_int(row['assists']),
                    'points': self._safe_int(row['points']),
                    'plus_minus': self._safe_int(row['plus_minus']),
                    'penalty_minutes': self._safe_int(row['penalty_minutes']),
                    'shots_on_goal': self._safe_int(row['shots_on_goal']),
                    'game_winning_goals': self._safe_int(row['game_winning_goals']),

                    'shootouts': self._safe_float(row['shootouts']),
                    'gaa': self._safe_float(row['gaa']),
                    'save_percentage': self._safe_float(row['save_percentage']),
                    'point_shares': self._safe_float(row['point_shares']),
                }
                self.data.append(player_dict)

    def make_index(self) -> None:
        for field in self.text_fields:
            self.tf[field] = {}
            self.df[field] = {}
            self.doc_lengths[field] = {}

        for i, row in enumerate(self.data):
            self.extend_index(row, i)

        for field in self.numeric_fields:
            values: List[Tuple[Union[int, float], int]] = [
                (cast(Union[int, float], row[field]), i) 
                for i, row in enumerate(self.data) 
                if row[field] is not None
            ]
            self.index[field] = sorted(values, key=lambda x: x[0])
        return
        
    def extend_index(self, row: PLAYER_DATA, id: int):
        for key in self.text_fields:
            if key in row:
                value = cast(str | None, row[key])
                if isinstance(value, str):
                    data: str = value
                    tokens = data.lower().strip().split(' ')

                    if not tokens:
                        continue

                    term_counts = Counter(tokens)
                    self.tf[key][id] = dict(term_counts)
                    self.doc_lengths[key][id] = len(tokens)

                    for token in tokens:
                        index_dict = cast(Dict[str, Set[int]], self.index[key])
                        if token in index_dict:
                            index_dict[token].add(id)
                        else:
                            index_dict[token] = set([id])
                    
                        if token not in self.df[key]:
                            self.df[key][token] = 0
                        self.df[key][token] += 1

                        

    def get_tf(self, field: str, doc_id: int, term: str) -> float:
        """
        Get term frequency (raw count) for a term in a document.
        Returns 0 if term not found.
        """
        if doc_id not in self.tf.get(field, {}):
            return 0.0
        return float(self.tf[field][doc_id].get(term, 0))
    
    def get_tf_log_normalized(self, field: str, doc_id: int, term: str) -> float:
        """
        Get log-normalized term frequency: 1 + log(TF).
        Reduces the impact of very high term frequencies.
        """
        tf = self.get_tf(field, doc_id, term)
        return 1 + math.log(tf) if tf > 0 else 0.0

    def get_idf_standard(self, field: str, term: str) -> float:
        """
        Standard IDF: log(N / df)
        where N = total documents, df = documents containing term
        
        Returns 0 if term not found.
        """
        N = len(self.data)
        df = self.df.get(field, {}).get(term, 0)
        
        if df == 0:
            return 0.0
        
        return math.log(N / df)
    
    def get_idf_smooth(self, field: str, term: str) -> float:
        """
        Smoothed IDF: log((N + 1) / (df + 1)) + 1
        
        Advantages:
        - Prevents division by zero
        - Prevents zero IDF for terms in all documents
        - Adds 1 to ensure all terms have positive weight
        """
        N = len(self.data)
        df = self.df.get(field, {}).get(term, 0)
        
        return math.log((N + 1) / (df + 1)) + 1
    
    def get_idf_probabilistic(self, field: str, term: str) -> float:
        """
        Probabilistic IDF: log((N - df) / df)
        
        Interpretation: log of the ratio of documents NOT containing the term
        to documents containing the term.
        
        Advantages:
        - Based on probability theory
        - Directly models the odds of a document being relevant
        - Used in BM25 and probabilistic retrieval models
        
        Note: Returns 0 if term appears in all documents (df = N)
        Can be negative if term appears in more than half the documents
        """
        N = len(self.data)
        df = self.df.get(field, {}).get(term, 0)
        
        if df == 0 or df == N:
            return 0.0
        
        return max(0, math.log((N - df) / df))
    
    def get_tfidf(self, field: str, doc_id: int, term: str, idf_method: IDF = IDF.STANDARD) -> float:
        """
        Calculate TF-IDF score for a term in a document.
        
        Args:
            field: Text field name
            doc_id: Document ID
            term: Term to score
            tf_method: 'raw', 'normalized', or 'log'
            idf_method: 'standard' or 'smooth'
        
        Returns:
            TF-IDF score
        """
        tf = self.get_tf_log_normalized(field, doc_id, term)

        match idf_method:
            case IDF.PROBABILISTIC:
                idf = self.get_idf_probabilistic(field, term)
            case IDF.SMOOTH:
                idf = self.get_idf_smooth(field, term)
            case IDF.STANDARD:
                idf = self.get_idf_standard(field, term)
        
        return tf * idf
    

    def search_text_fields(self, query: List[Tuple[str, str]], limit: Optional[int] = None) -> List[Tuple[int, float]]:
        """
        Search text fields and rank documents by TF-IDF score.
        Documents must contain ALL tokens from ALL fields in the query (AND logic).
        
        Args:
            query: List of (field_name, search_value) tuples
                e.g., [('player_name', 'wayne gretzky'), ('position', 'center')]
            limit: Maximum number of results to return (None = all results)
        
        Returns:
            List of (doc_id, score) tuples, sorted by score descending
        """
        if not query:
            return []
        
        all_token_sets: List[Set[int]] = []
        query_tokens: List[Tuple[str, str]] = []
        
        for field_name, search_value in query:
            if field_name not in self.text_fields:
                continue
            
            search_tokens = search_value.lower().strip().split()
            
            for token in search_tokens:
                query_tokens.append((field_name, token))
                
                if token in self.index.get(field_name, {}):
                    token_docs = cast(Set[int], self.index[field_name][token])
                    all_token_sets.append(token_docs)
                else:
                    return []
        
        if not all_token_sets:
            return []
        
        matching_docs = all_token_sets[0].intersection(*all_token_sets[1:])
        
        if not matching_docs:
            return []
        
        doc_scores: Dict[int, float] = {}
        
        for doc_id in matching_docs:
            total_score = 0.0
            for field_name, token in query_tokens:
                tfidf = self.get_tfidf(field_name, doc_id, token, IDF.PROBABILISTIC)
                total_score += tfidf
            
            doc_scores[doc_id] = total_score
        
        ranked_results = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)

        if limit is not None:
            return ranked_results[:limit]

        return ranked_results
             

    def search_numeric_fields(self, query: List[Tuple[str, Union[int, float], Union[int, float]]]) -> List[Tuple[int, None]]:
        """
        Search multiple numeric fields with range conditions.
        Documents must satisfy ALL numeric conditions (AND logic).
        
        Args:
            query: List of (field_name, min_value, max_value) tuples
        
        Returns:
            List of (doc_id, None) tuples for documents matching all conditions
        """
        if not query:
            return []
        
        all_matching_sets: List[Set[int]] = []
        
        for field_name, min_val, max_val in query:
            if field_name not in self.numeric_fields:
                continue
            
            if self.index[field_name] is None:
                continue
            
            sorted_list: List[Tuple[Union[int, float], int]] = cast(
                List[Tuple[Union[int, float], int]], 
                self.index[field_name]
            )
            
            field_matches: Set[int] = set()
            
            actual_min = min_val if min_val != float('-inf') else None
            actual_max = max_val if max_val != float('inf') else None
            
            start_idx = bisect.bisect_left(sorted_list, (actual_min, 0)) if actual_min is not None else 0
            end_idx = bisect.bisect_right(sorted_list, (actual_max, float('inf'))) if actual_max is not None else len(sorted_list)
            
            for _, idx in sorted_list[start_idx:end_idx]:
                field_matches.add(idx)
            
            all_matching_sets.append(field_matches)
        
        if not all_matching_sets:
            return []
        
        matching_docs = all_matching_sets[0].intersection(*all_matching_sets[1:])
        
        return [(doc_id, None) for doc_id in matching_docs]
    

    def show_results(self, results: Sequence[Tuple[int, Optional[float]]], query_keys: List[str]) -> None:
        for id, score in results:
            doc = self.data[id]

            print(f'Player Name: {doc['player_name']}')
            for key in query_keys:
                if key == 'player_name':
                    continue
                print(f'{key.capitalize()}: {doc[key]}')

            print(f'URL: {doc['download_url']}')
            print(f'Score: {score}\n', '='*80)


    def parse_query_string(self, query_string: str) -> Tuple[List[Tuple[str, str]], List[Tuple[str, int | float, int |float]]]:
        """
        Parse a query string into text and numeric field filters.
        
        Returns:
            tuple: (text_filters, numeric_filters)
                - text_filters: list of tuples [(field, value), ...]
                - numeric_filters: list of tuples [(field, min_value, max_value), ...]
        """
        text_filters: List[Tuple[str, str]] = []
        numeric_filters: List[Tuple[str, int | float, int |float]] = []
        
        tokens = query_string.replace(',', '').split()
        i = 0
        
        while i < len(tokens):
            current_field = None
            
            if tokens[i] in self.text_fields:
                current_field = tokens[i]
                field_type = 'text'
            elif tokens[i] in self.numeric_fields:
                current_field = tokens[i]
                field_type = 'numeric'
            else:
                i += 1
                continue
            
            i += 1
            
            if i >= len(tokens):
                break
            
            if field_type == 'text':
                value_parts: List[str] = []
                while i < len(tokens) and tokens[i] not in self.text_fields and tokens[i] not in self.numeric_fields:
                    value_parts.append(tokens[i])
                    i += 1
                text_filters.append((current_field, ' '.join(value_parts)))
            
            else:
                if i < len(tokens) and tokens[i].lower() == 'between':
                    i += 1
                    
                    if i < len(tokens):
                        try:
                            min_value = float(tokens[i])
                            i += 1
                            
                            if i < len(tokens) and tokens[i].lower() == 'and':
                                i += 1
                            if i < len(tokens):
                                try:
                                    max_value = float(tokens[i])
                                    i += 1
                                    numeric_filters.append((current_field, min_value, max_value))
                                except ValueError:
                                    pass
                        except ValueError:
                            pass
                    continue
                
                range_operator = None
                
                if i < len(tokens) - 1:
                    two_word = f"{tokens[i]} {tokens[i+1]}".lower()
                    if two_word in ['more than', 'greater than', 'less than', 'smaller than']:
                        range_operator = two_word
                        i += 2
                
                if range_operator is None and i < len(tokens):
                    single = tokens[i].lower()
                    if single in ['>', '<', 'greater', 'smaller', 'more', 'less']:
                        range_operator = single
                        i += 1
                        if i < len(tokens) and tokens[i].lower() == 'than':
                            i += 1
                
                if i < len(tokens):
                    try:
                        value = float(tokens[i])
                        i += 1
                        
                        if range_operator in ['>', 'more', 'greater', 'more than', 'greater than']:
                            numeric_filters.append((current_field, value, float('inf')))
                        elif range_operator in ['<', 'less', 'smaller', 'less than', 'smaller than']:
                            numeric_filters.append((current_field, float('-inf'), value))
                        else:
                            numeric_filters.append((current_field, value, value))
                    except ValueError:
                        pass
        
        return text_filters, numeric_filters


    def search(self, query_string: str, limit: Optional[int] = None) -> None:
        """
        Search using both text and numeric fields from a query string.
        
        Args:
            query_string: Natural language query string
            limit: Maximum number of results to return
        """
        text_query, numeric_query = self.parse_query_string(query_string)

        if len(numeric_query) == 0 and len(text_query) == 0:
            print('Aint no search, failed to parse query string.')
            return

        if len(numeric_query) == 0:
            results = self.search_text_fields(text_query, limit)
            self.show_results(results, [key for key, _ in text_query])
            return
        
        if len(text_query) == 0:
            results = self.search_numeric_fields(numeric_query)
            if limit is not None:
                results = results[:limit]
            self.show_results(results, [key for key, _, _ in numeric_query])
            return
        

        text_results = self.search_text_fields(text_query, limit=None)
        numeric_results = self.search_numeric_fields(numeric_query)

        text_doc_ids = {doc_id for doc_id, _ in text_results}
        numeric_doc_ids = {doc_id for doc_id, _ in numeric_results}
        
        matching_doc_ids = text_doc_ids.intersection(numeric_doc_ids)
        
        if not matching_doc_ids:
            print('No results found matching all criteria.')
            return
        
        final_results = [(doc_id, score) for doc_id, score in text_results if doc_id in matching_doc_ids]
        
        if limit is not None:
            final_results = final_results[:limit]
        
        query_keys = [key for key,_ in text_query] +[key for key, _, _ in numeric_query]
        self.show_results(final_results, query_keys)


def get_index(force_recreate: bool = False, save: bool = False) -> Index:
    """
    Get and instance of the Index class.
    Prefers to load from a file.
    """
    file_path = os.path.join(JOBLIB_DIR, 'index.joblib')

    if force_recreate:
        index = Index()
        index.load_tsv()
        index.make_index()

        if save:
            joblib.dump(index, file_path) # type: ignore
        
        return index
    
    if os.path.exists(file_path):
        index: Index = joblib.load(file_path) #type: ignore
        return index
    
    index = Index()
    index.load_tsv()
    index.make_index()

    if save:
        joblib.dump(index, file_path) # type: ignore
    
    return index
    
    