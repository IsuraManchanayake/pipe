import time
import threading
import logging
from collections import Counter
from typing import Iterable, Callable

import numpy as np
from numpy.typing import NDArray

from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.step import Step


class Pipeline:
    logger = logging.getLogger(__name__)

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.steps: list[Step] = []
        self.record_write_callback: Callable[[Record], None] = lambda record: None
        self.omit_callback: Callable[[Record], None] = lambda record: None
        self.omit_reasons: Counter[str] = Counter()
        if self.config.debug_info:
            self.step_call_insights: NDArray[tuple[float, int, int]] = np.array([])  # tuples of (total time, number of calls, omits)
            self._insights_lock = threading.Lock()

    def process(self, records: Iterable[Record]) -> Iterable[Record]:
        if self.config.debug_info:
            self.step_call_insights = np.array([(0.0, 0, 0) for _ in self.steps])
        # Run records in parallel if configured, otherwise fall back to serial processing.
        start = time.time()
        processed_records = self._process_parallel(records) if self.config.workers > 1 else map(self._process_record, records)
        for line_no, record in enumerate(processed_records, 1):
            if record.omit:
                self.omit_callback(record)
                self.collect_omit_insights(record)
            else:
                self.record_write_callback(record)

            if line_no % 100 == 0:
                elapsed = time.time() - start
                time_left_seconds = (elapsed / line_no) * (self.config.input_limit - line_no)
                # time_left_minutes = (time_left_seconds / 60) % 60
                # time_left_hours = int(time_left_seconds / 3600)
                self.logger.info('[progress] %d rows seen', line_no)
                self.logger.info('[progress] elapsed time: %s', _duration_string(elapsed))
                self.logger.info('[progress] estimated time left: %s', _duration_string(time_left_seconds))
                # self.logger.info('[progress] Estimated time left: %d hours %d minutes', time_left_hours, time_left_minutes)
                if self.config.debug_info:
                    for step_idx, step in enumerate(self.steps):
                        name = step.__class__.__name__
                        elapsed, calls, omits = self.step_call_insights[step_idx]
                        avg_time = elapsed / calls if calls else 0
                        omit_percentage = (100.0 * omits) / calls if calls else 0
                        self.logger.debug(
                            '[debug] [insights] %s: rate=%.4f Rec/s, omit_percentage=%.2f%%',
                            name,
                            (1 / avg_time) if avg_time else 0.0,
                            omit_percentage,
                        )
                    self.logger.debug('[debug] [insights] omit_reasons=%s', dict(self.omit_reasons))
        return records

    def _process_parallel(self, records: Iterable[Record]) -> Iterable[Record]:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=self.config.workers) as executor:
            for record in executor.map(self._process_record, records, chunksize=64):
                yield record

    def _process_record(self, record: Record) -> Record:
        for step_idx, step in enumerate(self.steps):
            if self.config.debug_info:
                record = self.call_with_insights(step_idx, step.process, record)
            else:
                record = step.process(record)
            if record.omit:
                break
        return record

    def call_with_insights(self, step_idx, func, *args, **kwargs):
        t = time.time()
        res: Record = func(*args, **kwargs)
        elapsed = self.step_call_insights[step_idx][0] + (time.time() - t)
        n_calls = self.step_call_insights[step_idx][1] + 1
        omits = self.step_call_insights[step_idx][2]
        if res.omit:
            omits += 1
        # Protect concurrent writers when running with worker threads.
        if self.config.workers > 1:
            with self._insights_lock:
                self.step_call_insights[step_idx] = (elapsed, n_calls, omits)
        else:
            self.step_call_insights[step_idx] = (elapsed, n_calls, omits)
        return res

    def collect_omit_insights(self, record: Record) -> None:
        self.omit_reasons[record.omit_reason] += 1
        return None

    def generate_insights(self) -> dict:
        insights = {
            'omit_reasons': dict(self.omit_reasons),
            'steps': {},
        }
        for step_idx, step in enumerate(self.steps):
            name = step.__class__.__name__
            elapsed, calls, omits = self.step_call_insights[step_idx]
            avg_time = elapsed / calls if calls else 0
            omit_percentage = (100.0 * omits) / calls if calls else 0
            insights['steps'][name] = {
                'total_time_seconds': elapsed,
                'number_of_calls': int(calls),
                'average_time_per_call_seconds': avg_time,
                'number_of_omits': int(omits),
                'omit_percentage': omit_percentage,
            }
        return insights


    def register_step(self, GenericStep: type[Step]) -> None:
        self.logger.info('Initializing %s...', GenericStep.__name__)
        step = GenericStep(self.config)
        self.logger.info('Initialized %s', GenericStep.__name__)
        self.steps.append(step)

    def register_record_write_callback(self, on_write: Callable[[Record], None]) -> None:
        self.record_write_callback = on_write

    def register_omit_callback(self, on_omit: Callable[[Record], None]) -> None:
        self.omit_callback = on_omit


def _duration_string(seconds: float) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '%dh %02dm %02ds' % (hours, minutes, seconds)
