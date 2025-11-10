import time
from pyspark.sql import SparkSession
import os
import bz2
from typing import List
import re


WIKI_DUMP = "./wiki/enwiki-latest-pages-articles.xml.bz2"
OUTPUT_DIR = "./wiki_pages"

filter_wiki = SparkSession.builder \
    .master("local[*]") \
    .appName("Load data") \
    .config("spark.driver.host", "0.0.0.0") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.shuffle.service.enabled", "false") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.network.timeout", "1200s") \
    .config("spark.sql.broadcastTimeout", "1200s") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.sql.execution.pythonUTF8StringEncoding", "true") \
    .getOrCreate()

spark_context = filter_wiki.sparkContext

data_patterns = {
    #"born": r"(?i)\bBorn\s+([^\n]+)",
    "nhl_team": r"(?i)\bNHL\s*team\s+([^\n]+)",
    "national_team": r"(?i)\bNational\s*team\s+([^\n]+)",
    "nhl_draft": r"(?i)\bNHL\s*draft\s+([^\n]+)",
    "playing_career": r"(?i)\bPlaying\s*career\s+([^\n]+)",
}

def is_redirect(page: str) -> bool:
    pattern = r"<redirect.+?/>"
    return re.match(pattern, page) is not None

def is_about_hockey_player(page: str) -> bool:
    if is_redirect(page):
        return False
    
    for pattern in data_patterns.values():
        if re.search(pattern, page):
            return True
    return False

def get_page_generator(path: str):
    with bz2.open(path, "rt", encoding='UTF-8') as file:
        buffer: List[str] = []
        in_page = False         
        for line in file:
            if "<page>" in line:
                in_page = True
                buffer = []
            if in_page:
                buffer.append(line)
            if "</page>" in line and in_page:
                yield "".join(buffer)
                in_page = False

def process_chunk(chunk: List[str], output_path: str, chunk_counter: int):
    rdd = spark_context.parallelize(chunk)
    filtered_rdd = rdd.filter(is_about_hockey_player)
    chunk_output_path = os.path.join(output_path, f"chunk_{chunk_counter}")
    #spark_context._jsc.hadoopConfiguration().set("fs.file.impl.disable.cache", "true")
    #spark_context._jsc.hadoopConfiguration().set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

    filtered_rdd.saveAsTextFile(chunk_output_path)
    print(f"Saved chunk {chunk_counter} to {chunk_output_path}")

def process_wiki_dump_in_chunks(file_path: str, output_path: str, chunk_size: int = 10000):
    chunk_counter = 0
    current_chunk: List[str] = []
    
    for page in get_page_generator(file_path):
        current_chunk.append(page)
        
        if len(current_chunk) >= chunk_size:
            process_chunk(current_chunk, output_path, chunk_counter)
            current_chunk = []
            chunk_counter += 1
    
    if current_chunk:
        process_chunk(current_chunk, output_path, chunk_counter)

    print(f"Processed and saved {chunk_counter + 1} chunks.")
    


if __name__ == '__main__':
    start = time.time()
    process_wiki_dump_in_chunks(WIKI_DUMP, OUTPUT_DIR)
    filter_wiki.stop()
    end = time.time()
    
    print(f"Time to execute: {end - start:.2f}s")