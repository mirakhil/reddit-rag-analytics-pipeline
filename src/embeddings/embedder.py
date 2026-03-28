from langchain_openai import OpenAIEmbeddings
from src.utils.config import config
from src.utils.logger import get_logger

log = get_logger(__name__)

def get_embeddings():
    return OpenAIEmbeddings(
        model=config.openai_embedding_model,
        api_key=config.openai_api_key
    )