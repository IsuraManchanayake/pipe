![Mainpipe Logo](docs/images/Mainpipe.png)

# Mainpipe – Data Prep Pipeline

An end-to-end, containerised Python pipeline that cleans, normalises, and shards the provided `mainpipe_data_v1.jsonl` corpus into an English-focused dataset ready for LLM pre-training experiments. Outputs are JSONL so downstream tokenisers/loaders can plug in directly.

## Quickstart
- Local run: `python main.py --input mainpipe_data_v1.jsonl --output outputs`
- Keep non-English rows: add `--allow-non-english`
- Change shard size: `--shard-size 5000` (default 10k)

Outputs land in `outputs/`:
- `cleaned.jsonl` – fully cleaned, deduped rows with metadata
- `shards/shard_*.jsonl` – evenly sized shards for parallel training IO
- `metrics.json` – drop reasons, length histogram, language counts, shard size used

## Docker
```
docker build -t mainpipe .
docker run --rm -v $(pwd)/mainpipe_data_v1.jsonl:/data/mainpipe_data_v1.jsonl -v $(pwd)/outputs:/app/outputs mainpipe
```

## What the pipeline does
1) **Ingest** raw JSONL streaming (no full-file load).  
2) **Normalise**: HTML unescape, strip control chars, standardise quotes, collapse whitespace/newlines.  
3) **Quality filters** (tunable via CLI): minimum length (chars/tokens), max length guard, ASCII ratio, symbol ratio, long repeat detection, optional English-only gate via lightweight stop-word heuristic.  
4) **Deduplicate** canonicalised text fingerprints (SHA1 of whitespace-collapsed lowercase).  
5) **Export** cleaned rows with metadata (`lang`, lengths, ratios, source) to a monolithic JSONL plus evenly sized shards.  
6) **Inspectability**: `metrics.json` captures input/kept counts, drop reasons, language distribution, length histogram, and shard sizing; progress logs every 50k rows.

## Design choices
- **Stdlib-first**: avoids heavy deps; deterministic, easy to run anywhere (or inside the provided container).  
- **Heuristic language gate**: ASCII ratio + English stop-word hits; `--allow-non-english` keeps multilingual data.  
- **Repeat/boilerplate filter**: catches pathologically repetitive sequences and symbol-heavy noise.  
- **Sharding**: predictable shard sizes to reduce small-file issues during training and enable parallel loaders.

## Scaling notes
- **Distributed execution**: wrap the stream step in Spark/Ray/Dask map jobs reading remote object storage, writing partitioned parquet/JSONL with deterministic seeds.  
- **PII & safety**: plug spaCy/pii-extract-base and Detoxify into the filter stage; emit per-stage hit-rates into `metrics.json` and Prometheus.  
- **Dedup at scale**: swap SHA1 set for MinHash/LSH or external ANN service; run exact dedup per partition + LSH across partitions.  
- **Shuffle/IO**: write larger shards (e.g., 50–200MB), coalesce small files, and compress (zstd). Pre-tokenise to HuggingFace `datasets` arrow for fast loader startup.  
- **Failure modes**: checkpoints per shard, idempotent outputs (content-addressed paths), and retryable workers that skip already-written shards.

## Deliverables recap
- **Pipeline code**: `main.py` (streaming cleaning + sharding)  
- **Container**: `Dockerfile` (Python 3.10 slim, stdlib-only)  
- **Outputs**: run the quickstart to produce `outputs/cleaned.jsonl`, shard directory, and `metrics.json`  
- **Report**: this README summarises decisions, knobs, and scaling plan
