import os
import re
from typing import List
import joblib
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import pandas as pd


from config import PROCESSED_DIR
from object_types import CAREER_FRAME, DRAFT, WIKI_PLAYER

MERGED_DIR = './players_merged'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Parse data") \
    .config("spark.driver.host", "0.0.0.0") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.shuffle.service.enabled", "false") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.sql.execution.pythonUTF8StringEncoding", "true") \
    .getOrCreate()


WIKI_PLAYER_SCHEMA = StructType([
    StructField("full_name", StringType(), nullable=False),
    StructField("birthplace", StringType(), nullable=True),
    StructField("career_start", IntegerType(), nullable=True),
    StructField("career_end", IntegerType(), nullable=True),
    StructField("draft", StringType(), nullable=True),
    StructField("draft_year", StringType(), nullable=True),
    StructField("draft_team", StringType(), nullable=True),
    StructField("current_league", StringType(), nullable=True),
    StructField("national_team", StringType(), nullable=True),
    StructField("current_team", StringType(), nullable=True),
    StructField("nationality", StringType(), nullable=True),
])

PLAYER_DATA_SCHEMA = StructType([
    StructField("file_path", StringType(), nullable=False),
    StructField("download_url", StringType(), nullable=False),
    StructField("player_name", StringType(), nullable=False),
    StructField("dob", StringType(), nullable=False),
    StructField("draft_team", StringType(), nullable=True),
    StructField("position", StringType(), nullable=False),
    StructField("hand", StringType(), nullable=False),
    StructField("height", IntegerType(), nullable=True),
    StructField("weight", IntegerType(), nullable=True),
    StructField("games_played", IntegerType(), nullable=True),
    StructField("wins", IntegerType(), nullable=True),
    StructField("losses", IntegerType(), nullable=True),
    StructField("ties_ot_losses", IntegerType(), nullable=True),
    StructField("minutes", IntegerType(), nullable=True),
    StructField("shootouts", FloatType(), nullable=True),
    StructField("gaa", FloatType(), nullable=True),
    StructField("save_percentage", FloatType(), nullable=True),
    StructField("goals", IntegerType(), nullable=True),
    StructField("assists", IntegerType(), nullable=True),
    StructField("points", IntegerType(), nullable=True),
    StructField("plus_minus", IntegerType(), nullable=True),
    StructField("point_shares", FloatType(), nullable=True),
    StructField("penalty_minutes", IntegerType(), nullable=True),
    StructField("shots_on_goal", IntegerType(), nullable=True),
    StructField("game_winning_goals", IntegerType(), nullable=True),
])


def safe_int(input_string: str) -> int | None:
    if not isinstance(input_string, (str, int)) or input_string is None:
        if isinstance(input_string, int):
            return input_string
        return None

    try:
        return int(input_string)
    except ValueError:
        return None

def get_full_name(page: str) -> str | None:
    pattern = r"(?<!')'''([^']+?)'''(?!')"
    match = re.search(pattern, page)
    if match:
        value = match.group(1).strip()
        return value if value else None
    
    return None
        
def get_birthplace(page: str) -> str | None:
    raw_match = re.search(r"\| birth_place\s*=\s*([^\n|]*?)(?=\s*\n|\s*\|)", page)
    if not raw_match:
        return None
    raw = raw_match.group(1).strip()

    city_match = re.search(r"\[\[.*?\|(.*?)\]\]", raw)
    if not city_match:
        city_match = re.search(r"\[\[(.*?)\]\]", raw)
    
    city = city_match.group(1).strip().replace('[', '').replace(']', '') if city_match else ""
    parts = [p.strip() for p in re.split(r",", re.sub(r"\[\[.*?\|(.*?)\]\]", r"\1", raw))]
    country = parts[-1].replace('[', '').replace(']', '') if parts else ""

    cleaned_city = re.sub(r"[\[\]]", "", city)
    cleaned_country = re.sub(r"[\[\]]", "", country)

    return f'{cleaned_city}, {cleaned_country}'    


def get_carrer_frame(page: str) -> CAREER_FRAME:
    start_pattern = r"\| (career_start)\s*=\s*([^\n|]*?)(?=\s*\n|\s*\|)"
    end_pattern = r"\| (career_end)\s*=\s*([^\n|]*?)(?=\s*\n|\s*\|)"

    start_match = re.search(start_pattern, page)
    end_match = re.search(end_pattern, page)

    start = None
    end = None

    if start_match:
        start = safe_int(start_match.group(2).strip())
    
    if end_match:
        end = safe_int(end_match.group(2).strip())

    return {
        'start': start,
        'end': end
    }


def get_draft(page: str) -> DRAFT:
    draft_pattern = r"\| draft\s*=\s*([^\n|]*?)(?=\s*\n|\s*\|)"
    draft_year_pattern = r"\| draft_year\s*=\s*([^\n|]*?)(?=\s*\n|\s*\|)"
    draft_team_pattern = r"\| draft_team\s*=\s*([^\n|]*?)(?=\s*\n|\s*\|)"

    draft_match = re.search(draft_pattern, page)
    draft_year_match = re.search(draft_year_pattern, page)
    draft_team_match = re.search(draft_team_pattern, page)

    draft = None
    year = None
    team = None

    if draft_match:
        draft = draft_match.group(1).strip() if draft_match.group(1).strip() else None

    if draft_year_match:
        year = safe_int(draft_year_match.group(1).strip())

    if draft_team_match:
        team = re.sub(r"[\[\]]", "", draft_team_match.group(1).strip()) if draft_team_match.group(1).strip() else None

    return {
        'draft': draft,
        'draft_team': team,
        'draft_year': year
    }


def get_league(page: str) -> str | None:
    pattern = r"\| league\s*=\s*([^\n|]*?)(?=\s*\n|\s*\|)"

    match = re.search(pattern, page)

    if not match:
        return None
    
    raw = match.group(1).strip()

    if not raw:
        return None

    wiki_match = re.match(r"\[\[(.*?)\|(.*?)\]\]", raw)
    if wiki_match:
        full_name = re.sub(r"[\[\]]", "", wiki_match.group(1))
        abbr = re.sub(r"[\[\]]", "", wiki_match.group(2))
        return f"{full_name} ({abbr})"
    
    wiki_match2 = re.match(r"\[\[(.*?)\]\]", raw)
    if wiki_match2:
        return re.sub(r"[\[\]]", "", wiki_match2.group(1))
    return re.sub(r"[\[\]]", "", raw)


def get_national_team(page: str) -> str | None:
    match = re.search(r"\| ntl_team\s*=\s*([^\n|]*?)(?=\s*\n|\s*\|)", page)
    if match:
        value = match.group(1).strip()
        return value if value else None
    return None

def get_team(page: str) -> str | None:
    match = re.search(r"\| team\s*=\s*([^\n|]*?)(?=\s*\n|\s*\|)", page)
    if not match:
        return None
    
    if not match.group(1).strip():
        return None
    
    raw = re.sub(r"[\[\]]", "", match.group(1).strip())
    return raw

def get_nationality(article_text: str) -> str | None:
    match = re.search(
        r"'''[^']+'''\s*(?:\(.*?\))?\s+is an?\s+(?:\[\[.*?\|(.*?)\]\]|([A-Z][a-z]*(?:\s[A-Z][a-z]*)*))",
        article_text
    )
    if match:
        return match.group(1) or match.group(2)
    return None



def process_page(page: str) -> WIKI_PLAYER | None:
    name = get_full_name(page)

    if name is None:
        return None
    
    draft = get_draft(page)
    career = get_carrer_frame(page)

    return {
        'full_name': name,
        'birthplace': get_birthplace(page),
        'career_start': career['start'],
        'career_end': career['end'],
        'current_league': get_league(page),
        'current_team': get_team(page),
        'draft': draft['draft'],
        'draft_team': draft['draft_team'],
        'draft_year': draft['draft_year'],
        'national_team': get_national_team(page),
        'nationality': get_nationality(page)
    }

def get_page_generator_from_file(file_path: str):
    """Yields <page>...</page> blocks from a single file."""
    buffer: List[str] = []
    is_page = False
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            if "<page>" in line:
                is_page = True
                buffer = []
            if is_page:
                buffer.append(line)
            if "</page>" in line and is_page:
                yield "".join(buffer)
                is_page = False

def get_all_pages_from_file(file_path: str) -> List[str]:
    """Collect all pages from a single file into a list."""
    logger.info(f"Processing file: {file_path}")
    pages = list(get_page_generator_from_file(file_path))
    logger.info(f"Extracted {len(pages)} pages from {file_path}")
    return pages


def process_pages(merged_dir: str, output_path: str, chunk_size: int = 500):    
    try:
        all_pages = []
        files = [f for f in os.listdir(merged_dir) if os.path.isfile(os.path.join(merged_dir, f))]
        logger.info(f"Found {len(files)} files to process")
        
        for file in files:
            file_path = os.path.join(merged_dir, file)
            pages = get_all_pages_from_file(file_path)
            all_pages.extend(pages)
        
        logger.info(f"Total pages collected: {len(all_pages)}")
        
        rdd = spark.sparkContext.parallelize(all_pages, numSlices=len(all_pages) // chunk_size + 1)
        
        logger.info("Processing pages in parallel...")
        processed_rdd = rdd.map(process_page)

        filtered_rdd = processed_rdd.filter(lambda x: x is not None)

        
        logger.info("Collecting processed results...")
        processed_data = filtered_rdd.collect()
        
        logger.info(f"Creating DataFrame with {len(processed_data)} records...")
        df = spark.createDataFrame(processed_data, schema=WIKI_PLAYER_SCHEMA)
        
        logger.info(f"Saving to {output_path}...")
        logger.info(f"Successfully saved DataFrame to {output_path}")
        
        dump_df(df)
        
        return df
        
    finally:
        spark.stop()
        logger.info("Spark session stopped")




def dump_df(df: DataFrame, file_path = './processed_pages.joblib'):
    pd_df = df.toPandas()
    joblib.dump(pd_df, file_path)

def load_df(file_path = './processed_pages.joblib') -> DataFrame:
    pd_df: pd.DataFrame = joblib.load(file_path)
    return spark.createDataFrame(pd_df)


def load_from_tsv() -> DataFrame:
    df = spark.read \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .option("nullValue", None) \
    .option("emptyValue", None) \
    .schema(PLAYER_DATA_SCHEMA) \
    .csv(os.path.join(PROCESSED_DIR, 'data.tsv'))
    return df 


if __name__ == "__main__":
    OUTPUT_PATH = "processed_pages.joblib"
    
    #df = process_pages(MERGED_DIR, OUTPUT_PATH, chunk_size=500)
    #print(f"Processing complete.")

    #df: DataFrame = load_df()
    #df.show(n=1)
    df = load_from_tsv()
    dump_df(df, './html_df.joblib')
