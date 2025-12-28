import json
import sys
import time
import os
from openai import OpenAI
from ..data_store import DataStore
from ..config import DB_CONFIG, DATA_PATH, OPENAI_API_KEY, OPENAI_MODEL
import re

# Path to the prompt file
PROMPT_FILE = 'paraphrase_prompt.txt'

def load_prompt():
    """Loads the prompt template from an external file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    prompt_path = os.path.join(script_dir, PROMPT_FILE)
    with open(prompt_path, 'r') as f:
        return f.read()

# Load the prompt template
prompt_template = load_prompt()
prompt = prompt_template  # Set the prompt variable for later use

# Global function to generate the prompt content
def generate_prompt(original_sentence):
    return prompt_template.format(original_sentence=original_sentence)

max_tokens = 70
temperature = 1

def create_batch_input_file(sentences, num_paraphrases):
    jsonl_output_path = f"{DATA_PATH}/paraphrasing_request.jsonl"

    with open(jsonl_output_path, mode='w', encoding='utf-8') as jsonlfile:
        for sentence_data in sentences:
            original_sentence_id, original_sentence = sentence_data  # Removed 'term'
            for idx in range(num_paraphrases):
                content = generate_prompt(original_sentence)  # Updated function call
                request_data = {
                    "custom_id": f"{original_sentence_id}-{idx}",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": OPENAI_MODEL,
                        "messages": [{
                            "role": "user",
                            "content": content
                        }],
                        "max_tokens": max_tokens,
                        "temperature": temperature,
                    }
                }
                jsonlfile.write(json.dumps(request_data) + '\n')

    print(f"Finished writing JSONL file to {jsonl_output_path}")
    return jsonl_output_path

def submit_batch_and_save_results(jsonl_output_path, datastore):
    client = OpenAI(api_key=OPENAI_API_KEY)
    with open(jsonl_output_path, "rb") as file:
        batch_input_file = client.files.create(file=file, purpose="batch")

    batch = client.batches.create(
        input_file_id=batch_input_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": "Batch paraphrasing job"}
    )
    print("Batch submitted, waiting for completion...")

    batch_id = batch.id
    start_time = time.time()
    timeout = 24 * 60 * 60  # 24 hours

    while time.time() - start_time < timeout:
        batch = client.batches.retrieve(batch_id)
        print(f"Current batch status: {batch.status}")
        if batch.status in ["completed", "failed", "cancelled"]:
            print(f"Batch processing finished with status: {batch.status}")
            if batch.output_file_id:
                content_response = client.files.content(batch.output_file_id)
                content = content_response.read().decode()
                response_file_path = f"{DATA_PATH}/paraphrasing_response.jsonl"
                with open(response_file_path, "w") as output_file:
                    output_file.write(content)
                print(f"Batch results saved to {response_file_path}")

                # Parse content to extract data for database insertion
                paraphrased_data = []
                for line in content.strip().split('\n'):
                    item = json.loads(line)
                    response_body = item['response']['body']
                    choices = response_body['choices'][0]
                    message = choices['message']['content']
                    paraphrased_sentence = message.strip()
                    model = response_body['model']
                    model_system_fingerprint = response_body.get('system_fingerprint', None)
                    if model_system_fingerprint is None:
                        # Handle the case where system_fingerprint might not be present
                        print(f"Warning: system_fingerprint not found for item with custom_id {item.get('custom_id')}")
                        model_system_fingerprint = 'unknown' 
                    original_sentence_id = item['custom_id'].split('-')[0]
                    paraphrased_data.append((original_sentence_id, paraphrased_sentence, model,model_system_fingerprint))

                if paraphrased_data:
                    datastore.insert_paraphrased_sentences(paraphrased_data)
                    print("Data successfully inserted into the datastore.")

            break
        time.sleep(30)  # Check every 30 sec

def main(num_paraphrases):
    datastore = DataStore(DB_CONFIG)
    try:
        sentences = datastore.fetch_original_sentences()
        print("Fetched sentences from datastore.")
        jsonl_output_path = create_batch_input_file(sentences, int(num_paraphrases))
        submit_batch_and_save_results(jsonl_output_path, datastore)
    finally:
        datastore.close()
        print("Datastore connection closed.")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python paraphraser.py <num_paraphrases>")
        sys.exit(1)
    num_paraphrases = sys.argv[1]
    main(num_paraphrases)
