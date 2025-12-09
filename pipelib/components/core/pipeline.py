import time
from collections import Counter
from typing import Iterable, Callable

import numpy as np
from numpy.typing import NDArray

from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.step import Step


class Pipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.steps: list[Step] = []
        self.record_write_callback: Callable[[Record], None] = lambda record: None
        self.omit_callback: Callable[[Record], None] = lambda record: None
        self.omit_reasons: Counter[str] = Counter()
        if self.config.debug_info:
            self.step_call_insights: NDArray[tuple[float, int, int]] = np.array([])  # tuples of (total time, number of calls, omits)

    def process(self, records: Iterable[Record]) -> Iterable[Record]:
        if self.config.debug_info:
            self.step_call_insights = np.array([(0.0, 0, 0) for _ in self.steps])
        for line_no, record in enumerate(records, 1):
            for step_idx, step in enumerate(self.steps):
                if self.config.debug_info:
                    record = self.call_with_insights(step_idx, step.process, record)
                else:
                    record = step.process(record)
                # No need to add one more branch for checking isinstance(step, Filter) in a critical section.
                if record.omit:
                    self.omit_callback(record)
                    self.collect_omit_insights(record)
                    break
            else:
                self.record_write_callback(record)

            if line_no % 100 == 0:
                print(f'[progress] {line_no} rows seen')
                if self.config.debug_info:
                    for step_idx, step in enumerate(self.steps):
                        name = step.__class__.__name__
                        elapsed, calls, omits = self.step_call_insights[step_idx]
                        avg_time = elapsed / calls if calls else 0
                        omit_percentage = (100.0 * omits) / calls if calls else 0
                        print(f'[debug] [insights] {name}: rate={1/avg_time:.4f}Rec/s, {omit_percentage=:.2f}%')
                    print(f'[debug] [insights] {self.omit_reasons=}')
        return records

    def call_with_insights(self, step_idx, func, *args, **kwargs):
        t = time.time()
        res: Record = func(*args, **kwargs)
        elapsed = self.step_call_insights[step_idx][0] + (time.time() - t)
        n_calls = self.step_call_insights[step_idx][1] + 1
        omits = self.step_call_insights[step_idx][2]
        if res.omit:
            omits += 1
        self.step_call_insights[step_idx] = (elapsed, n_calls, omits)
        return res

    def collect_omit_insights(self, record: Record) -> None:
        self.omit_reasons[record.omit_reason] += 1
        return None

    def register_step(self, step: Step) -> None:
        # print(step.__class__.__name__)
        self.steps.append(step)

    def register_record_write_callback(self, on_write: Callable[[Record], None]) -> None:
        self.record_write_callback = on_write

    def register_omit_callback(self, on_omit: Callable[[Record], None]) -> None:
        self.omit_callback = on_omit
