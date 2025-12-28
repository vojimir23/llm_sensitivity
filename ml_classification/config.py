
# Configuration for Sketch Enigne api:
BASE_URL = "https://api.sketchengine.eu/bonito/run.cgi/concordance"
USERNAME = ""
API_KEY = ""
CORPUS_NAME = "preloaded/ententen21_tt31"


# Config for OpenAI API
OPENAI_API_KEY = ""
OPENAI_MODEL = "gpt-4o-mini-2024-07-18"
DATA_PATH = r""


# Config for Mistral

MODEL="Ministral-8B-Instruct-2410-f16.gguf"


DB_CONFIG = {
    'dbname': 'llm_sensitivity', #censorship
    'user': 'postgres',
    'password': '',
    "host": "::1",
    'port': '5432'  # Default port for PostgreSQL
}
