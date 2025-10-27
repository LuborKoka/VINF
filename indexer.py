import math
import os
import csv
from typing import Counter, Dict, Optional, List, Set, Tuple, Union, cast
from object_types import INDEX, PLAYER_DATA
from config import JOBLIB_DIR, PROCESSED_DIR
import bisect
import joblib # type: ignore

class Index:
    def __init__(self) -> None:
        self.text_fields = ['player_name', 'dob', 'position', 'draft_team', 'hand']
        self.skip_fields = ['id', 'file_path', 'download_url']
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

        self.tf: Dict[str, Dict[int, Dict[str, int]]] = {}  # field -> doc_id -> {term: count}
        self.df: Dict[str, Dict[str, int]] = {}  # field -> {term: doc_frequency}
        self.doc_lengths: Dict[str, Dict[int, int]] = {}  # field -> {doc_id: total_terms}

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

    def make_index(self) -> INDEX:
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
        return self.index
        
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
    
    def get_tf_normalized(self, field: str, doc_id: int, term: str) -> float:
        """
        Get normalized term frequency (TF / doc_length).
        Normalizes by document length to avoid bias toward longer documents.
        """
        tf = self.get_tf(field, doc_id, term)
        doc_length = self.doc_lengths.get(field, {}).get(doc_id, 1)
        return tf / doc_length if doc_length > 0 else 0.0
    
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
    
    def get_tfidf(self, field: str, doc_id: int, term: str, 
                  tf_method: str = 'raw', idf_method: str = 'standard') -> float:
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
        # Get TF
        if tf_method == 'raw':
            tf = self.get_tf(field, doc_id, term)
        elif tf_method == 'normalized':
            tf = self.get_tf_normalized(field, doc_id, term)
        elif tf_method == 'log':
            tf = self.get_tf_log_normalized(field, doc_id, term)
        else:
            raise ValueError(f"Unknown TF method: {tf_method}")
        
        # Get IDF
        if idf_method == 'standard':
            idf = self.get_idf_standard(field, term)
        elif idf_method == 'smooth':
            idf = self.get_idf_smooth(field, term)
        else:
            raise ValueError(f"Unknown IDF method: {idf_method}")
        
        return tf * idf
         

    def search_numeric_range(self, field: str, min_val: Optional[Union[int, float]] = None, max_val: Optional[Union[int, float]] = None) -> Set[int]:
        """Search for values in range [min_val, max_val]"""
        if self.index[field] is None:
            return set()

        sorted_list: List[Tuple[Union[int, float], int]] = cast(
            List[Tuple[Union[int, float], int]], 
            self.index[field]
        )
        result: Set[int] = set()
        
        start_idx = bisect.bisect_left(sorted_list, (min_val, 0)) if min_val is not None else 0
        end_idx = bisect.bisect_right(sorted_list, (max_val, float('inf'))) if max_val is not None else len(sorted_list)
        
        for _, idx in sorted_list[start_idx:end_idx]:
            result.add(idx)
        
        return result


def get_index(force_recreate: bool = False, save: bool = False) -> Index:
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
    
    