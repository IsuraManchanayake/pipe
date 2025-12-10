import argparse
import json
import logging
from pathlib import Path
from typing import Iterable, Tuple
from itertools import islice

from pipelib.components.core.pipeline import Pipeline
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig, PipelineConfigDefaults
from pipelib.components.filters import CodeSnippetFilter, DedupFilter, LanguageFilter, PreliminaryFilter, ToxicityFilter
from pipelib.components.modifiers import AttributeEvaluationStep, NormalizeModifier, PIIModifier, HTMLExtractorModifier
from pipelib.utils import ensure_dir, count_file_lines


def parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(description="Mainpipe data preparation pipeline")
    parser.add_argument("--input", dest="input_path", default="mainpipe_data_v1.jsonl", help="Path to raw JSONL")
    parser.add_argument("--output", dest="output_dir", default="outputs", help="Directory to store outputs")
    parser.add_argument("--debug-info", action="store_true", default=True, help="Enable debug info mode")
    parser.add_argument('--input-limit', type=int, default=0, help="Limit number of records to process. Set the value to 0 to process all records.")
    parser.add_argument("--workers", type=int, default=PipelineConfigDefaults.WORKERS, help="Number of worker threads for processing")
    parser.add_argument("--shard-size", type=int, default=PipelineConfigDefaults.SHARD_SIZE, help="Number of rows per shard")
    parser.add_argument("--min-char-len", type=int, default=PipelineConfigDefaults.MIN_CHAR_LEN, help="Minimum characters to keep a sample")
    parser.add_argument("--min-token-len", type=int, default=PipelineConfigDefaults.MIN_TOKEN_LEN, help="Minimum tokens to keep a sample")
    parser.add_argument("--max-char-len", type=int, default=PipelineConfigDefaults.MAX_CHAR_LEN, help="Maximum characters to keep a sample")
    parser.add_argument("--toxicity-batch-size", type=int, default=PipelineConfigDefaults.TOXICITY_BATCH_SIZE, help="Toxicity check batch size")
    parser.add_argument("--toxicity-threshold", type=float, default=PipelineConfigDefaults.TOXICITY_THRESHOLD, help="Toxicity threshold")
    parser.add_argument("--allow-non-english", action="store_true", default=not PipelineConfigDefaults.REQUIRE_ENGLISH, help="Keep non-English rows (disabled by default)")
    args = parser.parse_args()

    return PipelineConfig(
        input_path=Path(args.input_path),
        output_dir=Path(args.output_dir),
        debug_info=args.debug_info,
        input_limit=args.input_limit,
        shard_size=args.shard_size,
        min_char_len=args.min_char_len,
        min_token_len=args.min_token_len,
        max_char_len=args.max_char_len,
        workers=max(args.workers, 1),
        require_english=not args.allow_non_english,
        toxicity_threshold=args.toxicity_threshold,
        toxicity_batch_size=args.toxicity_batch_size,
    )


def setup_pipeline(config: PipelineConfig) -> Pipeline:
    pipeline = Pipeline(config)
    pipeline.register_step(NormalizeModifier)
    pipeline.register_step(AttributeEvaluationStep)
    pipeline.register_step(PreliminaryFilter)
    pipeline.register_step(HTMLExtractorModifier)
    pipeline.register_step(CodeSnippetFilter)
    pipeline.register_step(DedupFilter)
    pipeline.register_step(LanguageFilter)
    pipeline.register_step(ToxicityFilter)
    pipeline.register_step(PIIModifier)
    return pipeline


def setup_input(config: PipelineConfig) -> Iterable[Record]:
    if config.input_limit <= 0:
        config.input_limit = count_file_lines(config.input_path)
    with open(config.input_path, 'r', encoding='utf-8') as inf:
        for line in islice(inf, config.input_limit):
            try:
                obj = json.loads(line)
            except json.decoder.JSONDecodeError:
                continue
            text = obj.get('text')
            url = obj.get('url')
            if text:
                yield Record(text, url)


def setup_output(config: PipelineConfig) -> Tuple[Path, Path, Path]:
    output_dir_path = ensure_dir(config.output_dir)
    shard_dir_path = ensure_dir(output_dir_path / 'shards')
    cleaned_path = output_dir_path / 'cleaned.jsonl'
    omit_path = output_dir_path / 'omit_data.jsonl'
    return cleaned_path, shard_dir_path, omit_path


def process_pipeline(pipeline: Pipeline, config: PipelineConfig) -> None:
    records = setup_input(config)
    cleaned_path, shard_dir_path, omit_path = setup_output(config)

    cleaned_handle = open(cleaned_path, 'w', encoding='utf-8')
    omit_handle = open(omit_path, 'w', encoding='utf-8')
    shard_index = 0
    records_written = 0
    shard_written = 0
    shard_handle = open(shard_dir_path / f'shard_{shard_index}.jsonl', 'w', encoding='utf-8')

    def save_record(record: Record) -> None:
        nonlocal records_written, shard_written, shard_index, shard_handle

        record.write_successful_jsonl(cleaned_handle)
        record.write_successful_jsonl(shard_handle)

        records_written += 1
        shard_written += 1
        if records_written % config.shard_size == 0:
            shard_index += 1
            shard_handle.close()
            shard_handle = open(shard_dir_path / f'shard_{shard_index}.jsonl', 'w', encoding='utf-8')
            shard_written = 0

    def on_omit(record: Record) -> None:
        nonlocal omit_handle
        record.write_failed_jsonl(omit_handle)

    pipeline.register_record_write_callback(save_record)
    pipeline.register_omit_callback(on_omit)

    # Pipeline processing
    pipeline.process(records)

    shard_handle.close()
    cleaned_handle.close()
    omit_handle.close()

    insight_path = config.output_dir / 'pipeline_insights.json'
    with open(insight_path, 'w', encoding='utf-8') as insight_handle:
        insights_dict = pipeline.generate_insights()
        json.dump(insights_dict, insight_handle, indent=4)


def main():
    config = parse_args()

    logging.basicConfig(
        level=logging.DEBUG if config.debug_info else logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logger = logging.getLogger(__name__)

    pipeline = setup_pipeline(config)

    logger.info(f'Running pipeline on {config.input_path} -> {config.output_dir}')
    process_pipeline(pipeline, config)


if __name__ == '__main__':
    main()
