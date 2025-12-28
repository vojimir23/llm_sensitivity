import requests
from tqdm import tqdm
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import random
from ..config import BASE_URL, USERNAME, API_KEY, CORPUS_NAME, DB_CONFIG
from ..data_store import DataStore

class SentenceFetcher:
    def fetch_sentences(self, expression, num_sentences, min_words, max_words):
        pass

class SketchEngineFetcher(SentenceFetcher):
    def __init__(self, username, api_key, corpus_name, base_url):
        self.username = username
        self.api_key = api_key
        self.corpus_name = corpus_name
        self.base_url = base_url
        self.seen_toknums = set()  # Global set to track seen token numbers

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type((requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout)),
           retry_error_callback=lambda retry_state: {"Lines": None})
    def fetch_data(self, params):
        response = requests.get(self.base_url, params=params, auth=(self.username, self.api_key))
        response.raise_for_status()
        return response.json()

    def fetch_sentences(self, expression_id, expression, num_sentences, min_words, max_words):
        words = expression.split()
        query = " ".join(f'[lc="{word}"]' for word in words) 
        query = f'q{query}'

        page = 1
        pagesize = 20000  # Adjusting the pagesize to 20,000
        collected_sentences = 0
        all_sentences_data = []
        sentences_data = []  # Ensure sentences_data is initialized as an empty list
        excluded_genres = {'reference/encyclopedia', 'news', 'legal'}

        with tqdm(total=num_sentences, desc=f"Collecting sentences for '{expression}'", leave=False) as pbar:
            while collected_sentences < num_sentences:
                params = {
                    "corpname": self.corpus_name,
                    "q": query,
                    "viewmode": "sen",
                    "structs": "s,g",
                    "refs": "doc.website,doc.crawl_date,doc.urldomain,doc.url,doc.title,doc.src,doc.genre,doc.topic",
                    "fromp": page,
                    "pagesize": pagesize,
                    "async": "1",
                    "format": "json",
                }
                try:
                    data = self.fetch_data(params)
                    if 'Lines' not in data:
                        raise requests.exceptions.RequestException("Rate limit hit, retrying after delay")
                except requests.exceptions.RequestException as err:
                    print(f"Error occurred: {err}")
                    time.sleep(60)  # Wait 60 seconds before retrying...
                    continue

                if not data.get('Lines'):
                    # Break the loop if no more sentences are returned
                    break

                new_sentences_collected = 0
                for line in data['Lines']:
                    parts = line.get('Left', []) + line.get('Kwic', []) + line.get('Right', [])
                    sentence = ' '.join(part.get('str', '') for part in parts).replace('<s>', '').replace('</s>', '').strip()
                    word_count = len(sentence.split())
                    token_number = line.get('toknum', 0)

                    # Extract metadata by splitting the key=value pairs
                    metadata = {item.split('=')[0]: item.split('=')[1] if '=' in item else "" for item in line.get('Refs', [])}
                    genre = metadata.get('Genre', '').lower()

                    if token_number in self.seen_toknums or genre in excluded_genres:
                        continue

                    if min_words <= word_count <= max_words:
                        all_sentences_data.append(
                            (expression_id, sentence, token_number, self.corpus_name, "Sketch Engine",
                            metadata.get('Website (e.g. cnn.com)', ''),
                            metadata.get('Title', ''),
                            metadata.get('Crawl date', ''),
                            metadata.get('URL', ''),
                            metadata.get('Topic', ''),
                            genre)
                        )
                        self.seen_toknums.add(token_number)
                        new_sentences_collected += 1

                # If fewer sentences are available, take all collected
                if len(all_sentences_data) >= num_sentences:
                    sentences_data = random.sample(all_sentences_data, num_sentences)
                else:
                    sentences_data = all_sentences_data  # Take all sentences when fewer than requested

                collected_sentences = len(sentences_data)
                pbar.update(new_sentences_collected)

                # Break the loop if no new sentences were collected in this iteration
                if new_sentences_collected == 0:
                    break

        return sentences_data



def main(num_sentences, min_words, max_words):
    datastore = DataStore(DB_CONFIG)
    fetcher = SketchEngineFetcher(USERNAME, API_KEY, CORPUS_NAME, BASE_URL)
    expressions = datastore.fetch_expressions()
    
    overall_pbar = tqdm(total=len(expressions), desc="Overall Progress", leave=True)
    total_sentences_scraped = 0
    
    for expression_id, expression in expressions:
        sentences = fetcher.fetch_sentences(expression_id, expression, num_sentences, min_words, max_words)
        datastore.insert_original_sentences(sentences)
        total_sentences_scraped += len(sentences)
        overall_pbar.update(1)
    
    overall_pbar.close()
    datastore.close()

    print(f"Total sentences scraped: {total_sentences_scraped}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch sentences using Sketch Engine and store in the database.")
    parser.add_argument("num_sentences", type=int, help="Number of sentences per expression")
    parser.add_argument("min_words", type=int, help="Minimum number of words per sentence")
    parser.add_argument("max_words", type=int, help="Maximum number of words per sentence")
    args = parser.parse_args()
    main(args.num_sentences, args.min_words, args.max_words)
