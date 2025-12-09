import os
import time
from pathlib import Path
from functools import wraps


def ensure_dir(path: str|Path) -> Path:
    os.makedirs(str(path), exist_ok=True)
    return Path(path)


def _make_gen(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024*1024)


def count_file_lines(path: Path):
    f = open(str(path), 'rb')
    f_gen = _make_gen(f.raw.read)
    return sum(buf.count(b'\n') for buf in f_gen)


def timed(func):
    total_time = 0.0
    call_count = 0

    @wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal total_time, call_count
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            total_time += elapsed
            call_count += 1

    def stats():
        avg = total_time / call_count if call_count else 0.0
        return {
            "name": func.__name__,
            "calls": call_count,
            "total_time": total_time,
            "avg_time": avg,
        }

    wrapper.stats = stats
    return wrapper