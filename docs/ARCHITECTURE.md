# End-to-End Architecture: Reddit RAG Analytics Pipeline

This document sketches a concrete architecture for the target pipeline: **ingest Reddit → transform & store → embed & index → serve via RAG + web UI**, with optional orchestration and cloud/warehouse integration.

**Design choices:**
- **Public Reddit API only** — No Reddit app/OAuth; we use the public JSON API (no PRAW). No client credentials required.
- **Incremental loading** — Ingest and downstream stages are incremental so we avoid re-reading and re-storing the same posts/comments on every run (important for low-traffic subreddits where listing order changes slowly).

---

## 1. Pipeline stages (high level)

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  INGEST     │───▶│  TRANSFORM       │───▶│  INDEX (RAG)    │───▶│  RAG / API       │───▶│  WEB UI         │
│  Reddit API │    │  Curate & Store  │    │  Embed & Store  │    │  Retrieve + LLM  │    │  Streamlit      │
└─────────────┘    └──────────────────┘    └─────────────────┘    └──────────────────┘    └─────────────────┘
       │                     │                        │                       │
       ▼                     ▼                        ▼                       ▼
   data/raw            data/curated            vector store              User queries
   (JSON)              (Parquet / DW)          (FAISS / Chroma)          Answers
```

| Stage | Responsibility | Input | Output | Trigger |
|-------|----------------|-------|--------|---------|
| **1. Ingest** | Pull **new** posts + their comments via public Reddit JSON API; respect rate limits; persist state to support incremental runs | Config (subreddits, limits) + state (known post IDs) | Raw JSON under `data/raw`; updated state | Scheduled (e.g. Airflow) or CLI |
| **2. Transform** | Normalize, **dedupe by post/comment ID**, partition; optionally load to Snowflake; produce analytics-ready tables (incremental append or merge) | Raw JSON (and/or Snowflake raw) | Parquet in `data/curated` and/or Snowflake tables (dbt models) | After ingest or on schedule |
| **3. Index (RAG)** | Chunk text (posts + comments), compute embeddings, **incrementally** build/update vector index (new docs only or merge) | Curated Parquet or Snowflake | Vector store (FAISS/Chroma/etc.) + metadata | After transform or on schedule |
| **4. RAG / API** | Accept question → retrieve relevant chunks → build context → LLM answer | User query + vector store | Answer + optional citations | Per request (API or Streamlit) |
| **5. Web UI** | Chat interface, filters (subreddit, date), display answers and sources | RAG API or direct RAG lib | Rendered chat + links | User in browser |

---

## 2. Proposed folder structure

Keep ingestion under `src/`, add sibling packages for transform, indexing, RAG, and a clear split for config, data, and app entrypoints.

```
reddit-rag-analytics-pipeline/
├── .env                          # Local secrets (Reddit, OpenAI, DB URLs); not committed
├── .cursor/                      # (optional) Cursor rules
├── config/                       # Pipeline and app configuration
│   ├── pipelines/                # Per-pipeline or per-env YAML/TOML (subreddits, limits, tables)
│   └── logging.yaml
├── data/                         # All file-based data (optional if everything goes to Snowflake)
│   ├── raw/                      # Output of ingest (current: posts/, comments/)
│   │   ├── posts/
│   │   ├── comments/
│   │   └── state/                 # Incremental state: {subreddit}_known_posts.json
│   ├── curated/                  # Output of transform (partitioned Parquet)
│   │   └── subreddit=.../date=.../
│   └── index/                    # Local vector index artifacts (if using file-based FAISS)
│       └── faiss_index/
├── docs/
│   └── ARCHITECTURE.md           # This file
├── main.py                       # CLI entrypoint: run ingest | transform | index | serve
├── notebooks/                    # EDA and one-off analysis (e.g. raw_data_analysis.ipynb)
├── pyproject.toml
├── requirements.txt             # Optional; can rely on pyproject.toml + uv
├── src/
│   ├── ingestion/                # Stage 1
│   │   ├── __init__.py
│   │   └── reddits_raw_ingest.py
│   ├── transform/                # Stage 2
│   │   ├── __init__.py
│   │   ├── reddit_to_curated.py  # JSON → Parquet, partition by subreddit/date
│   │   └── (optional) dbt/       # dbt project for Snowflake models
│   ├── indexing/                 # Stage 3
│   │   ├── __init__.py
│   │   ├── chunking.py           # Split posts/comments into chunks
│   │   ├── embeddings.py         # LangChain/OpenAI embeddings
│   │   └── vector_store.py      # Build/update FAISS or Chroma index
│   ├── rag/                      # Stage 4
│   │   ├── __init__.py
│   │   ├── retriever.py          # Load index, similarity search
│   │   └── chain.py              # LangChain chain: retrieve → prompt → LLM
│   └── api/                      # Optional: FastAPI/Flask for RAG (if not only Streamlit)
│       ├── __init__.py
│       └── app.py
├── app/                          # Web UI (Stage 5)
│   ├── __init__.py
│   └── streamlit_app.py
├── tests/
│   ├── unit/
│   └── integration/
├── dags/                         # When you add Airflow
│   └── reddit_pipeline.py
├── docker/                       # When you containerize
│   ├── Dockerfile.ingest
│   ├── Dockerfile.transform
│   └── Dockerfile.app
└── .github/workflows/            # CI/CD (lint, test, build images)
    └── ci.yml
```

- **Config**: Centralize subreddit list, limits, table names, and paths so ingest/transform/index read from one place (and `.env` stays for secrets).
- **Data**: Keep `raw` vs `curated` vs `index` so each stage has a clear contract. If you move fully to Snowflake, `data/curated` might become optional (dbt writes to Snowflake; indexing reads from there or from exported Parquet).
- **Entrypoint**: `main.py` can grow to subcommands: `ingest`, `transform`, `index`, `serve` (and later `run-dag` or similar for Airflow).

---

## 3. Services and components

| Component | Type | Purpose |
|-----------|------|--------|
| **Ingest job** | CLI / Python script (or Airflow task) | Runs `reddits_raw_ingest` (public JSON API only); loads state (known post IDs), fetches only new posts and their comments, writes to `data/raw` and updates state. |
| **Transform job** | CLI / Python script (or Airflow task) | Reads raw → normalizes → writes Parquet and/or runs dbt to build Snowflake tables. Can use PySpark for large scale. |
| **Index job** | CLI / Python script (or Airflow task) | Reads curated data → chunks → embeddings → updates vector store (FAISS file or Chroma/Pinecone etc.). |
| **Vector store** | Service or file artifact | FAISS (file-based), Chroma, Pinecone, or Snowflake vector search. Holds embeddings + metadata (subreddit, post_id, date) for filtering. |
| **RAG service** | Library used by API or Streamlit | Loads retriever + LangChain chain; answers questions using retrieved chunks + LLM. Can be in-process (Streamlit) or a separate FastAPI service. |
| **Web UI** | Streamlit app | Chat input, optional filters, display of answers and source posts/comments. Calls RAG layer (same process or HTTP). |
| **Orchestrator** | Airflow (or Prefect/Dagster) | Schedules ingest → transform → index; optionally triggers on new data. |
| **Warehouse (optional)** | Snowflake | Raw/curated tables; dbt for transformations; optional vector search or export to external vector DB. |

---

## 4. Incremental loading

To avoid re-reading and re-storing the same posts and comments on every run (especially for low-traffic subreddits where the “hot” listing changes slowly):

- **Ingest**
  - **State**: Per-subreddit state file (e.g. `data/raw/state/{subreddit}_known_posts.json`) stores the set of post IDs we have already ingested.
  - **Each run**: Fetch listing (e.g. hot) from the public API; filter out post IDs already in state; fetch comments **only for new posts**; append new posts/comments to run output (timestamped files); merge new post IDs into state and save.
  - **Result**: Raw files may contain only new data per run; transform stage dedupes by `id` when building curated tables.

- **Transform**
  - Read all raw (or only new runs) and **dedupe by post ID / comment ID** when writing curated Parquet (e.g. overwrite partition or merge on key). Downstream only sees each post/comment once.

- **Index (RAG)**
  - Option A: Rebuild index from full curated table periodically.  
  - Option B: Only embed and add **new** documents to the vector store (e.g. Chroma/FAISS upsert by doc ID), so repeated runs don’t duplicate chunks.

---

## 5. Data flow (concise)

1. **Ingest**: Reddit API → `data/raw/posts/*.json`, `data/raw/comments/*.json` (and/or Snowflake raw table).
2. **Transform**: Raw JSON → cleaned, partitioned Parquet under `data/curated/` (and/or dbt → Snowflake `curated` schema).
3. **Index**: Curated tables → text chunks (e.g. post title + body + top comments) → embeddings → vector store; store doc_id, subreddit, date for filtering.
4. **RAG**: User question → embed (or use same encoder) → similarity search → top-k chunks → build prompt → LLM → answer + citations.
5. **UI**: User types in Streamlit → same RAG path → show answer and links to Reddit posts.

---

## 6. Technology mapping (from your README)

| Planned item | Suggested use in this architecture |
|--------------|------------------------------------|
| **Python** | All stages: ingest, transform, indexing, RAG, Streamlit. |
| **PySpark** | Transform stage for large-scale Parquet or reading from Snowflake. |
| **dbt** | Transform stage in Snowflake: raw → staging → curated models. |
| **Snowflake** | Optional warehouse for raw/curated; optional vector search or export to FAISS/Chroma. |
| **LangChain** | RAG: document loaders, text splitters, embeddings, vector store integration, retrieval chain, LLM. |
| **FAISS / Vector DB** | Index stage: store embeddings; RAG stage: similarity search. Chroma/Pinecone if you want a server. |
| **Streamlit** | Web UI (Stage 5). |
| **Airflow** | Orchestrator: DAGs for ingest → transform → index (and optionally dbt). |
| **Docker** | Containerize ingest, transform, index jobs and Streamlit (and Airflow workers). |
| **GitHub Actions** | CI: lint, test; CD: build images, deploy (e.g. to Cloud Run or K8s). |

---

## 7. Implementation order (suggested)

1. **Harden ingest**: Wire `.env` (e.g. `DEFAULT_SUBREDDIT`, `POST_LIMIT`) into `reddits_raw_ingest.py`; use **public JSON API only** (no PRAW); implement **incremental loading** (state file + filter new posts, fetch comments only for new); keep writing to `data/raw`.
2. **Add transform**: `src/transform/reddit_to_curated.py` to read raw JSON → normalized DataFrame → write partitioned Parquet to `data/curated/` (match schema used in `raw_data_analysis.ipynb`).
3. **Unify entrypoint**: `main.py` with subcommands `ingest`, `transform` (e.g. `python main.py ingest`, `python main.py transform`).
4. **Indexing**: `src/indexing/` — chunking, embeddings (LangChain + OpenAI or local model), build FAISS or Chroma from `data/curated` (or from Snowflake export).
5. **RAG**: `src/rag/` — load index, retriever, simple LangChain chain (retrieve → prompt → LLM), return answer + sources.
6. **Streamlit**: `app/streamlit_app.py` — chat UI calling RAG; optional filters (subreddit, date range).
7. **Orchestration**: Add `dags/` with one DAG: ingest → transform → index (and dbt if used).
8. **Containers + CI**: Dockerfiles for ingest, transform, app; GitHub Actions for test and build.

This gives you a concrete end-to-end path from your current ingestion and notebook to a full RAG analytics pipeline with clear stages, folders, and services.
