from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from src.utils.config import config
from src.utils.logger import get_logger

log = get_logger(__name__)

def chunk_posts(df) -> list:

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=config.chunk_size,
        chunk_overlap=config.chunk_overlap
    )

    docs = []
    
    for _, row in df.iterrows():

        text = f"{row['POST_TITLE']} {row['POST_BODY']}"

        # create metadata dict here
        metadata = {
            "POST_AUTHOR": row["POST_AUTHOR"],
            "LINK_FLAIR_TEXT": row["LINK_FLAIR_TEXT"],
            "POST_ID": str(row["POST_ID"]),
            "SUBREDDIT": str(row["SUBREDDIT"]),
            "POST_URL": str(row["POST_URL"]),
            "type": "post",  # hardcoded
        }

        # split text into chunks using splitter.create_documents()
        chunks = splitter.create_documents([text], metadatas=[metadata])

        # add chunks to docs
        docs.extend(chunks)

    return docs

def chunk_comments(df) -> list:

    docs = []
    for _, row in df.iterrows():

        if row["COMMENT_BODY"] is not None or row["COMMENT_BODY"] != "":

            text = f"{row['COMMENT_BODY']}"

            metadata = {
                "COMMENT_ID": str(row["COMMENT_ID"]),
                "COMMENT_AUTHOR": str(row["COMMENT_AUTHOR"]),
                "COMMENT_PARENT_ID": str(row["COMMENT_PARENT_ID"]),
                "COMMENT_PERMALINK": str(row["COMMENT_PERMALINK"]),
                "SUBREDDIT": str(row["SUBREDDIT"]),
                "POST_ID": str(row["POST_ID"]),
                "type": "comment",
            }
            # for comments just do:
            doc = Document(page_content=text, metadata=metadata)
            docs.append(doc)


    return docs


