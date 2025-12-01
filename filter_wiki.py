from pyspark.sql import SparkSession
import os
from typing import List
import re


WIKI_DUMP = "./wiki/enwiki-latest-pages-articles.xml.bz2"
OUT_DIR = "./wiki_pages_players"
MERGED_DIR = './players_merged'
CHUNK_SIZE = 500

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Parse data") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .getOrCreate()


hockey_pattern = re.compile(
    r"\{\{[Ss]hort description\|[^}]*ice hockey player[^}]*\}\}", re.IGNORECASE
)

def is_ice_hockey_player(page: str) -> bool:
    """Return True if the page has a short description containing 'ice hockey player'."""
    return bool(hockey_pattern.search(page))


def is_redirect(page: str) -> bool:
    pattern = r"<redirect.+?/>"
    return re.match(pattern, page) is not None

def is_about_hockey_player(page: str) -> bool:
    if is_redirect(page):
        return False
    
    return is_ice_hockey_player(page)

def extract_pages_from_partition(iterator):
    """Extract <page>...</page> blocks from text lines."""
    buffer = []
    is_page = False
    
    for line in iterator:
        if "<page>" in line:
            is_page = True
            buffer = []
        if is_page:
            buffer.append(line)
        if "</page>" in line and is_page:
            yield "".join(buffer)
            is_page = False

def get_page_generator_from_file(file_path: str):
    """Yields <page>...</page> blocks from a single file."""
    buffer: List[str] = []
    is_page = False
    try:
        file = open(file_path, "r", encoding="utf-8")
        
        for line in file:
            if "<page>" in line:
                is_page = True
                buffer = []
            if is_page:
                buffer.append(line)
            if "</page>" in line and is_page:
                yield "".join(buffer)
                is_page = False
    except (OSError, EOFError, UnicodeDecodeError) as e:
        print(f"⚠️ Skipping file {file_path} due to error: {e}")

    finally:
        if file:
            file.close()

def get_all_part_files(root_dir: str):
    """Recursively find all part-0000[0-7] files."""
    for root, _, files in os.walk(root_dir):
        for fname in files:
            if re.match(r"part-0000[0-7]$", fname):
                yield os.path.join(root, fname)

def merge_pages_in_chunks():
    """Combine all pages into a few large XML files."""
    os.makedirs(MERGED_DIR, exist_ok=True)
    chunk = []
    chunk_counter = 0
    total_pages = 0

    for file_path in get_all_part_files(OUT_DIR):
        print(f"Reading {file_path}...")
        for page in get_page_generator_from_file(file_path, False):
            chunk.append(page)
            total_pages += 1

            if len(chunk) >= CHUNK_SIZE:
                save_chunk(chunk, chunk_counter)
                chunk = []
                chunk_counter += 1

    if chunk:
        save_chunk(chunk, chunk_counter)
        print(f"Saved final chunk #{chunk_counter}")

    print(f"✅ Merged {total_pages} pages into {chunk_counter + 1} XML files.")

def save_chunk(pages: List[str], chunk_counter: int):
    """Save a chunk of filtered pages to the target directory."""
    out_file = os.path.join(MERGED_DIR, f"chunk_{chunk_counter}.xml")
    with open(out_file, "w", encoding="utf-8") as f:
        for page in pages:
            f.write(page)
            if not page.endswith("\n"):
                f.write("\n")
    print(f"✅ Saved {len(pages)} pages to {out_file}")

def process_wiki_dump(file_path: str = WIKI_DUMP, output_path: str = OUT_DIR):
    os.makedirs(output_path, exist_ok=True)
    
    raw_rdd = spark.sparkContext.textFile(WIKI_DUMP)
    pages_rdd = raw_rdd.mapPartitions(extract_pages_from_partition)
    hockey_pages_rdd = pages_rdd.filter(is_about_hockey_player)
    hockey_pages_rdd.coalesce(8).saveAsTextFile(OUT_DIR)
    print(f"✅ Filtered pages saved to {OUT_DIR}")
    spark.stop()

    


if __name__ == "__main__":
    process_wiki_dump()
    merge_pages_in_chunks()