from dotenv import load_dotenv
import os

load_dotenv()

class Settings:
    MONGO_URI = os.getenv("MONGO_URI")
    DB_NAME = os.getenv("DB_NAME", "job_market_db")
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    MODEL_NAME = os.getenv("MODEL_NAME", "llama2")

settings = Settings()