# Mainpipe

An end-to-end data preparation pipeline for LLM pre-training that transforms raw text into high-quality, production-ready training corpora.

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-supported-brightgreen.svg)](https://www.docker.com/)

## Overview

Mainpipe is a containerized Python application that processes JSONL datasets through a sophisticated nine-stage filtering and cleaning workflow. It produces clean, tokenizer-ready output by removing noise, detecting toxicity, and anonymizing personally identifiable information (PII).

### Key Features

- **🎯 Quality-First**: Removes code snippets, duplicates, and low-quality content
- **🛡️ Safety**: Automated toxicity detection and filtering
- **🔒 Privacy**: PII detection and redaction using Microsoft Presidio
- **⚡ Performance**: Multi-threaded processing with configurable worker pools
- **🔧 Modular**: Pluggable pipeline components for flexible customization
- **📊 Observable**: Real-time progress tracking and comprehensive metrics
- **🐳 Production-Ready**: Fully containerized with Docker support

### Performance Highlights

When tested on a 269k-record dataset:
- **Selection Rate**: ~56% of records retained after filtering
- **Processing Speed**: Up to 252k records/sec for preliminary filtering
- **Multi-threading**: 40% reduction in processing time (5hrs → 3hrs with 4 workers)
- **Primary Filter**: Code detection accounts for ~65% of omissions

## Quick Start

### Prerequisites

- Python 3.12+
- 4GB+ RAM (8GB+ recommended for larger datasets)
- Docker (optional, for containerized deployment)

### Installation

Clone the repository and download the sample dataset:

```bash
git clone https://github.com/IsuraManchanayake/pipe.git
cd pipe
wget https://s3.us-east-1.amazonaws.com/mainpipe.maincode.com/mainpipe_data_v1.jsonl
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### Basic Usage

Run with default settings:

```bash
python main.py
```

Run with multiple workers for better performance:

```bash
python main.py --workers 4
```

Process a custom dataset:

```bash
python main.py --input my_data.jsonl --output ./cleaned_data --workers 8
```

### Docker Usage

Build and run with Docker:

```bash
docker build -t mainpipe .
docker run --rm -v "$PWD/outputs:/app/outputs" mainpipe
```

### Run Tests

Run tests with Python's `unittest`.

```bash
python -m unittest discover -s tests -p "*.py"
```

## Pipeline Architecture

Mainpipe implements a nine-stage processing pipeline:

1. **Normalization** - HTML-unescape, standardize quotes, collapse whitespace
2. **Attribute Evaluation** - Compute character/token counts, ASCII ratios
3. **Preliminary Filter** - Remove records below length/quality thresholds
4. **HTML Extraction** - Extract textual content from HTML while removing noise
5. **Code Snippet Filter** - Detect and remove code-heavy content
6. **Deduplication** - SHA-1 fingerprinting for exact duplicate removal
7. **Language Filter** - Keep English-only content (configurable)
8. **Toxicity Filter** - Remove toxic content using Detoxify
9. **Anonymization** - Redact PII using Microsoft Presidio

Each stage is modular and can be independently configured or replaced.

## Configuration

View all available options:

```bash
python main.py --help
```

### Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--input` | Path to input JSONL file | `mainpipe_data_v1.jsonl` |
| `--output` | Output directory | `./outputs` |
| `--workers` | Number of worker threads | `1` |
| `--shard-size` | Records per output shard | `10000` |
| `--min-char-len` | Minimum character length | `100` |
| `--min-token-len` | Minimum token count | `20` |
| `--max-char-len` | Maximum character length | `100000` |
| `--toxicity-threshold` | Toxicity score threshold | `0.7` |
| `--allow-non-english` | Keep non-English content | `False` |

### Example Configurations

Process only English content with strict quality filters:

```bash
python main.py --min-char-len 200 --min-token-len 50 --workers 8
```

Include non-English content with relaxed toxicity threshold:

```bash
python main.py --allow-non-english --toxicity-threshold 0.8
```

Process a limited subset for testing:

```bash
python main.py --input-limit 1000 --debug-info
```

## Input/Output Format

### Input

JSONL file with required fields:

```json
{"text": "Your text content here", "url": "https://example.com/source"}
```

The `url` field is optional but recommended for traceability.

### Output

The pipeline generates three output types in the specified output directory:

**1. cleaned.jsonl** - Successfully processed records:
```json
{"id": 1, "cleaned": "Processed and cleaned text..."}
```

**2. omit_data.jsonl** - Filtered records with reasons:
```json
{"id": 2, "reason": "code_snippet", "original": "Original text..."}
```

**3. shards/** - Cleaned data split into manageable chunks:
```
shards/shard_0.jsonl
shards/shard_1.jsonl
...
```

**4. pipeline_insights.json** - Performance metrics and statistics

## Pipeline Performance

Tested on M1 MacBook Pro with 4 workers:

| Stage | Throughput (rec/sec) | Omit Rate |
|-------|---------------------|-----------|
| Normalization | 4,533 | - |
| Attribute Evaluation | 8,333 | - |
| Preliminary Filter | 252,652 | 3.5% |
| HTML Extractor | 631 | - |
| Code Snippet Filter | 7,864 | 28.5% |
| Deduplication | 12,129 | 5.7% |
| Language Filter | 7,216 | 10.4% |
| Toxicity Filter | 4.8 | 0.7% |
| Anonymizer | 13.3 | - |

## Data Insights

### Filtering Distribution

Out of 269k records in the sample dataset:
- **56% retained** for training
- **44% filtered**, with breakdown:
  - 65% code snippets (Python/JavaScript)
  - 16% non-English
  - 9% duplicates
  - 7% preliminary quality checks
  - 1% toxicity

### PII Protection

- **64% of cleaned records** contain anonymized PII
- Detected entity types: names, emails, locations, phone numbers, IP addresses, pronouns
- All sensitive information replaced with type-specific tokens

### HTML Processing

- **11% of cleaned records** extracted from HTML sources
- Intelligent content extraction preserves structure while removing navigation, scripts, and ads

## Dependencies

Core libraries:
- **[Detoxify](https://github.com/unitaryai/detoxify)** - Toxicity classification
- **[Presidio](https://github.com/microsoft/presidio/)** - PII detection and anonymization
- **[fast-langdetect](https://github.com/LlmKira/fast-langdetect)** - Language identification
- **Python standard library** - Threading, logging, hashing

## Scaling Considerations

For production deployment at scale, consider:

### Throughput Optimization
- GPU-accelerated inference for Detoxify and Presidio
- Batch processing for toxicity detection
- Additional worker processes on multi-core systems

### Distributed Processing
- Apache Spark or Ray for cluster computing
- Strategic partitioning by content hash
- Shuffle optimization for deduplication

### Storage & I/O
- Stream from cloud object storage (S3, GCS)
- GZIP-compressed JSONL output
- Persistent deduplication storage (RocksDB, Bloom filters)

### Monitoring
- Prometheus + Grafana for metrics
- Real-time dashboards for pipeline health
- Alerting for anomaly detection

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Citation

If you use Mainpipe in your research, please cite:

```bibtex
@software{mainpipe2024,
  author = {Manchanayake, Isura},
  title = {Mainpipe: End-to-End Data Pipeline for LLM Pre-training},
  year = {2024},
  url = {https://github.com/IsuraManchanayake/pipe}
}
```

## Acknowledgments

- Microsoft Presidio for PII detection
- Detoxify for toxicity classification
- fast-langdetect for efficient language identification

## Contact

For questions, issues, or suggestions, please open an issue on GitHub or contact the maintainer.

---

**Note**: This pipeline is designed for research and development purposes. Always review filtered content and adjust thresholds based on your specific use case and requirements.