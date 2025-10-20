import string
import re
import csv
import os
import sys

INPUT_DIR = './scraped/players_list'
OUTPUT_DIR = './scraped/players_urls'

def scrape_player_data():
    print(f"Starting player data scraping from directory: {INPUT_DIR}")
    print("---------------------------------------------------------")
    PLAYER_PATTERN = re.compile(
        r'<p class="(?:nhl|non_nhl)"><a href="([^"]+)">([^<]+)</a> \([^)]+\)</p>'
    )

    files_processed = 0
    total_records = 0

    for letter in string.ascii_lowercase:
        input_filepath = os.path.join(INPUT_DIR, f'players_{letter}.html')
        output_filepath = os.path.join(OUTPUT_DIR, f'results_{letter}.tsv')

        player_records = []
        
        try:
            with open(input_filepath, 'r', encoding='utf-8') as file:
                html_content = file.read()
            matches = PLAYER_PATTERN.findall(html_content)
            
            for url, name in matches:
                player_records.append((name.strip(), url.strip()))
            
            if player_records:
                with open(output_filepath, 'w', newline='', encoding='utf-8') as outfile:
                    tsv_writer = csv.writer(outfile, delimiter='\t')
                    tsv_writer.writerow(['Player Name', 'URL'])
                    tsv_writer.writerows(player_records)
                
                print(f"SUCCESS: Processed {input_filepath}. Found {len(player_records)} records and saved to {output_filepath}")
                total_records += len(player_records)
                files_processed += 1
            else:
                print(f"INFO: Processed {input_filepath}. No player records found.")

        except FileNotFoundError:
            print(f"WARNING: Skipping letter '{letter}'. File not found at {input_filepath}")
        except Exception as e:
            print(f"ERROR: Failed to process {input_filepath} due to an unexpected error: {e}", file=sys.stderr)
            # Continue to the next letter even if one fails
            continue

    print("---------------------------------------------------------")
    print(f"Scraping complete. {files_processed} files processed, {total_records} total player records extracted.")


if __name__ == "__main__":
    scrape_player_data()