import os
import hashlib
from tqdm import tqdm
from ..data_store import DataStore
from ..config import DB_CONFIG, MODEL

PROMPT_FILE = 'sensitivity_prompt.txt'
temperature = 0

def load_prompt():
    """Loads the prompt template from an external file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    prompt_path = os.path.join(script_dir, PROMPT_FILE)
    with open(prompt_path, 'r') as f:
        return f.read()

def get_model_info():

    if isinstance(MODEL, dict):
        model_for_request = MODEL.get("name")
        model_value = MODEL.get("model")
        return model_for_request, model_value
    elif isinstance(MODEL, str):
        # If MODEL is just a string, both values are the same.
        return MODEL, MODEL
    else:
        # Fallback if somehow it's neither dict nor str:
        model_str = str(MODEL)
        return model_str, model_str

def classify_with_mistral(prompt, llm):
    result = llm.create_chat_completion(
        messages=[
            {"role": "user", "content": prompt}
        ],
        max_tokens=10,
        temperature=temperature
    )
    if "choices" in result and len(result["choices"]) > 0:
        return result["choices"][0]["message"]["content"].strip()
    else:
        print("Error: Local model inference did not return expected output")
        return None


def create_and_classify_input(sentences, datastore, llm, model_system_fingerprint):
    """
    Iterates over the fetched sentences, formats the prompt, gets a classification,
    and saves the result (including the hashed model) to the datastore.
    """
    print("Starting classification...")
    prompt_template = load_prompt()

    # Get model info (we still need model_value for insertion).
    model_for_request, model_value = get_model_info()
    
    for key, sentence in tqdm(sentences, desc="Classifying sentences"):
        formatted_prompt = prompt_template.format(sentence=sentence)
        classification = classify_with_mistral(formatted_prompt, llm)
        if classification is not None:
            # Insert the classification result into the llm_classification table
            # together with the hashed model value in model_system_fingerprint.
            datastore.insert_llm_classification(
                key,
                classification,
                model_value,
                model_system_fingerprint
            )
    print("Classification complete.")

def main():
    datastore = DataStore(DB_CONFIG)
    try:
        # Grab the model info (string), and 2) Hash it exactly once.
        model_for_request, model_value = get_model_info()
        model_system_fingerprint = hashlib.sha256(model_value.encode('utf-8')).hexdigest()

        # Fetch sentences to classify.
        sentences = datastore.fetch_for_llm()
        print("Fetched sentences from datastore.")

        # Initialize the local model using llama-cpp.
        from llama_cpp import Llama
        llm = Llama.from_pretrained(
            repo_id="bartowski/Ministral-8B-Instruct-2410-GGUF",
            filename=MODEL,
        )

        #  Perform classification, passing the hashed model.
        create_and_classify_input(sentences, datastore, llm, model_system_fingerprint)

    finally:
        datastore.close()
        print("Datastore connection closed.")

if __name__ == '__main__':
    main()
