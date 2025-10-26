# type: ignore
from indexer import load_index
import math
from whoosh.scoring import WeightingModel
from whoosh.qparser import QueryParser, MultifieldParser


class SimpleIDF(WeightingModel):
    """
    A simple custom IDF (Inverse Document Frequency) weighting model.
    
    How it works:
    - IDF measures how rare/unique a term is across all documents
    - Rare terms get higher scores (more discriminative)
    - Common terms get lower scores (less useful for distinguishing documents)
    
    Formula: IDF = log(total_docs / docs_with_term)
    
    Example:
    - If "hockey" appears in 900 out of 1000 docs: IDF = log(1000/900) = 0.05
    - If "Gretzky" appears in 10 out of 1000 docs: IDF = log(1000/10) = 2.0
    """
    
    def scorer(self, searcher, fieldname, text, qf=1):
        """
        Returns a scorer object for this weighting model.
        
        Args:
            searcher: The searcher object
            fieldname: The field being searched
            text: The search term
            qf: Query frequency (how many times term appears in query)
        """
        # Get document frequency stats
        total_docs = searcher.doc_count_all()  # Total number of documents
        docs_with_term = searcher.doc_frequency(fieldname, text)  # Docs containing this term
        
        # Calculate IDF
        if docs_with_term == 0:
            idf = 0.0
        else:
            # Add 1 to avoid division by zero and smooth the score
            idf = math.log((total_docs + 1) / (docs_with_term + 1))
        
        return SimpleIDFScorer(idf, qf)


class SimpleIDFScorer:
    """
    The scorer that actually calculates document scores.
    """
    
    def __init__(self, idf, qf):
        """
        Args:
            idf: The IDF value for this term
            qf: Query frequency
        """
        self.idf = idf
        self.qf = qf  # Query frequency (boost if term appears multiple times in query)
    
    def score(self, matcher):
        """
        Calculate the score for a document.
        
        Args:
            matcher: Contains info about the current document match
        """
        # Get term frequency in this document
        tf = matcher.weight()  # How many times term appears in this document
        
        # Simple scoring: TF * IDF * Query Frequency
        # - More occurrences in doc = higher score (TF)
        # - Rarer term overall = higher score (IDF)
        # - Term repeated in query = higher boost (QF)
        score = tf * self.idf * self.qf
        
        return score
    
    def max_quality(self):
        """
        Returns the maximum possible score for this term.
        Required for OR queries to work properly.
        """
        # Maximum quality is when TF is at its highest
        # We assume max TF could be around 10 (a term appearing 10 times in a doc)
        # You can adjust this if needed
        return 10.0 * self.idf * self.qf
    
    def supports_block_quality(self):
        return False
    

class ProbabilisticIDF(WeightingModel):
    """
    Probabilistic IDF weighting model (inspired by BM25).
    
    How it works:
    - Uses probability theory to calculate relevance
    - Balances term frequency with document length
    - Prevents very long documents from dominating results
    
    Formula: IDF = log((total_docs - docs_with_term + 0.5) / (docs_with_term + 0.5))
    
    This gives higher weight to rare terms but handles edge cases better.
    When a term appears in almost all documents, it can even get negative scores
    (meaning it's so common it's actually a negative signal).
    """
    
    def __init__(self, k1=1.5, b=0.75):
        """
        Args:
            k1: Controls term frequency saturation (1.2-2.0 typical)
                Higher = term frequency matters more
            b: Controls document length normalization (0-1)
               0 = ignore length, 1 = full length penalty
        """
        self.k1 = k1
        self.b = b
    
    def scorer(self, searcher, fieldname, text, qf=1):
        # Get document frequency stats
        total_docs = searcher.doc_count_all()
        docs_with_term = searcher.doc_frequency(fieldname, text)
        
        # Probabilistic IDF formula
        if docs_with_term == 0:
            idf = 0.0
        else:
            # This can be negative for very common terms (which is intentional)
            idf = math.log((total_docs - docs_with_term + 0.5) / (docs_with_term + 0.5))
        
        # Get average document length for normalization
        avg_doc_length = searcher.avg_field_length(fieldname) or 1.0
        
        return ProbabilisticIDFScorer(idf, qf, self.k1, self.b, avg_doc_length)

class ProbabilisticIDFScorer:
    """
    Scorer for probabilistic IDF model.
    """
    
    def __init__(self, idf, qf, k1, b, avg_doc_length):
        self.idf = idf
        self.qf = qf
        self.k1 = k1
        self.b = b
        self.avg_doc_length = avg_doc_length or 1.0
    
    def score(self, matcher):
        # Get term frequency
        tf = matcher.weight()
        
        # For simplicity, we'll use average document length for normalization
        # In a more complex implementation, you'd store actual doc lengths
        doc_length = self.avg_doc_length
        
        # Normalize document length
        length_norm = 1 - self.b + self.b * (doc_length / self.avg_doc_length)
        
        # BM25-style term frequency saturation
        # This prevents documents with many repetitions from dominating
        tf_component = (tf * (self.k1 + 1)) / (tf + self.k1 * length_norm)
        
        # Final score
        score = self.idf * tf_component * self.qf
        
        return max(0.0, score)  # Ensure non-negative scores
    
    def max_quality(self):
        # Maximum when TF is very high
        max_tf = 100.0
        tf_component = (max_tf * (self.k1 + 1)) / (max_tf + self.k1)
        return max(0.0, self.idf * tf_component * self.qf)
    
    def supports_block_quality(self):
        return False

if __name__ == '__main__':
    ix = load_index()
    with ix.searcher(weighting=ProbabilisticIDF()) as searcher:
        query = QueryParser("player_name", ix.schema).parse("john OR harry")
        results = searcher.search(query, limit=10)
        
        for hit in results:
            print(f"{hit['player_name']}: {hit.score}")

    with ix.searcher(weighting=SimpleIDF()) as searcher:
        query = QueryParser("player_name", ix.schema).parse("john OR harry")
        results = searcher.search(query, limit=10)
        
        for hit in results:
            print(f"{hit['player_name']}: {hit.score}")
