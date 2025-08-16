# generate_embeddings.py
import os
import time
import pickle
import math
from collections import OrderedDict

import pandas as pd
from tqdm import tqdm
from openai import OpenAI

# ------------- CONFIG -------------
INPUT_CSV = "from_db.csv"            # input from your SQL step
CACHE_PKL = "embeddings_cache.pkl"         # persistent cache (text -> embedding list)
OUTPUT_CSV = "pairs_with_embeddings.csv"   # final output with embedding columns
TMP_CACHE = CACHE_PKL + ".tmp"
MODEL = "text-embedding-3-large"
BATCH_SIZE = 64            # number of texts per API call
SAVE_EVERY = 5             # save cache every X batches
SLEEP_BETWEEN_BATCHES = 0.1
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0        # exponential backoff factor
API_KEY = ""
# ----------------------------------

def load_cache(path):
    if os.path.exists(path):
        with open(path, "rb") as f:
            data = pickle.load(f)
        return data
    return dict()

def atomic_save_cache(cache, path):
    # save to tmp then replace to avoid corruption
    with open(TMP_CACHE, "wb") as f:
        pickle.dump(cache, f)
    os.replace(TMP_CACHE, path)

def chunked(iterable, n):
    it = iter(iterable)
    while True:
        chunk = []
        for _ in range(n):
            try:
                chunk.append(next(it))
            except StopIteration:
                break
        if not chunk:
            break
        yield chunk

def get_embeddings_batch(client, texts):
    # Retry logic per batch
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # client.embeddings.create accepts a list of inputs
            resp = client.embeddings.create(model=MODEL, input=texts)
            embeddings = [r.embedding for r in resp.data]
            if len(embeddings) != len(texts):
                raise RuntimeError("embedding count mismatch")
            return embeddings
        except Exception as e:
            wait = RETRY_BACKOFF ** (attempt - 1)
            print(f"Error getting embeddings (attempt {attempt}/{MAX_RETRIES}): {e}. Retrying in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError("Max retries reached for batch")

def main():
    # 1) Load input pairs CSV
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"{INPUT_CSV} not found. Run SQL retrieval first.")
    df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")  # read as strings, fill NaN with empty

    # 2) Build set of unique texts (strip whitespace)
    need_texts = df["need_description"].astype(str).str.strip().replace("", None)
    res_texts = df["academic_resource_description"].astype(str).str.strip().replace("", None)

    # Gather unique non-empty texts
    unique_texts = OrderedDict()
    for t in pd.concat([need_texts, res_texts]).dropna().unique():
        key = t.strip()
        if key == "":
            continue
        unique_texts[key] = None

    print(f"Unique non-empty texts to consider: {len(unique_texts)}")

    # 3) Load cache
    cache = load_cache(CACHE_PKL)
    print(f"Loaded cache with {len(cache)} entries")

    # Remove already-cached texts from list
    pending_texts = [t for t in unique_texts.keys() if t not in cache]
    print(f"New texts to request embeddings for: {len(pending_texts)}")

    # 4) Initialize OpenAI client
    api_key = API_KEY
    if not api_key:
        raise RuntimeError("Set OPENAI_API_KEY environment variable with your API key.")
    client = OpenAI(api_key=api_key)

    # 5) Process in batches and save cache periodically
    if pending_texts:
        batches = list(chunked(pending_texts, BATCH_SIZE))
        for i, batch in enumerate(tqdm(batches, desc="Requesting embeddings batches")):
            try:
                emb_batch = get_embeddings_batch(client, batch)
            except Exception as e:
                print(f"Fatal error getting embeddings for batch {i}: {e}")
                # Save current cache to disk before exiting
                atomic_save_cache(cache, CACHE_PKL)
                raise

            # store each text embedding in cache
            for t, emb in zip(batch, emb_batch):
                cache[t] = emb

            # periodic save
            if (i + 1) % SAVE_EVERY == 0:
                atomic_save_cache(cache, CACHE_PKL)
                print(f"Saved cache after {i + 1} batches. Cache size {len(cache)}")

            time.sleep(SLEEP_BETWEEN_BATCHES)

        # final save
        atomic_save_cache(cache, CACHE_PKL)
        print(f"All done. Final cache size {len(cache)}")
    else:
        print("No new texts to embed. Using existing cache.")

    # 6) Map embeddings back to the dataframe
    emb_dim = len(next(iter(cache.values()))) if cache else 0
    if emb_dim == 0:
        raise RuntimeError("Embedding dimension detected as 0. Something is wrong with cache.")

    # Prepare columns for need and resource embeddings
    need_cols = [f"need_emb_{i}" for i in range(emb_dim)]
    res_cols = [f"res_emb_{i}" for i in range(emb_dim)]

    # Create dataframes for embeddings (filled with zeros for empty texts)
    need_emb_matrix = []
    res_emb_matrix = []
    missing_count = 0

    print("Mapping embeddings to dataframe rows...")
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Mapping rows"):
        need_text = str(row.get("need_description", "")).strip()
        res_text = str(row.get("academic_resource_description", "")).strip()

        if need_text:
            emb_need = cache.get(need_text)
            if emb_need is None:
                # This should not happen but handle gracefully
                emb_need = [0.0] * emb_dim
                missing_count += 1
        else:
            emb_need = [0.0] * emb_dim

        if res_text:
            emb_res = cache.get(res_text)
            if emb_res is None:
                emb_res = [0.0] * emb_dim
                missing_count += 1
        else:
            emb_res = [0.0] * emb_dim

        need_emb_matrix.append(emb_need)
        res_emb_matrix.append(emb_res)

    print(f"Missing embeddings applied as zeros: {missing_count}")

    # Convert to DataFrame and concat
    need_emb_df = pd.DataFrame(need_emb_matrix, columns=need_cols)
    res_emb_df = pd.DataFrame(res_emb_matrix, columns=res_cols)

    out_df = pd.concat([df.reset_index(drop=True), need_emb_df, res_emb_df], axis=1)

    # 7) Save final CSV (optionally parquet)
    print(f"Saving final CSV to {OUTPUT_CSV} ...")
    out_df.to_csv(OUTPUT_CSV, index=False)
    print("Saved successfully.")


if __name__ == "__main__":
    main()
