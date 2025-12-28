import sys
import time
import pandas as pd
from tqdm import tqdm
from ..config import BASE_URL, USERNAME, API_KEY, CORPUS_NAME
from .sentence_fetcher import SketchEngineFetcher
from ..data_store import DataStore

def fetch_and_save():
    if len(sys.argv) != 5:
        print("Usage: python make_sentences_and_save <num_sentences> <min_words> <max_words> <excel_file_path>")
        sys.exit(1)

    num_sentences = int(sys.argv[1])
    min_words = int(sys.argv[2])
    max_words = int(sys.argv[3])
    excel_file_path = sys.argv[4]

    fetcher = SketchEngineFetcher(USERNAME, API_KEY, CORPUS_NAME, BASE_URL)
    store = DataStore()

    terms_df = pd.read_excel(excel_file_path)
    terms = terms_df['Term'].tolist()
    categories = terms_df['Category'].tolist()
    sensitivities = terms_df['Sensitivity (grade)'].tolist()

    start_id = store.last_id + 1  

    start_time = time.time()
    for term, category, sensitivity in tqdm(zip(terms, categories, sensitivities), total=len(terms), desc="Fetching sentences for selected terms"):
        sentence_data = fetcher.fetch_sentences(term, num_sentences, min_words, max_words, category, sensitivity, start_id)  # Include start_id in the call
        store.add_entries(sentence_data)
        start_id += len(sentence_data)  

    store.save_data_to_csv("output_sentences.csv")
    print(f"Total elapsed time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    fetch_and_save()