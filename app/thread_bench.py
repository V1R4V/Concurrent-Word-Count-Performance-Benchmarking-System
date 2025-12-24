import os
import sys
import gzip
import time
import csv
import random
import string
import subprocess
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


NUM_FILES = 256
VOCAB_SIZE = 1000
TOKENS_PER_FILE = 1_000_000
CHUNK_TOKENS = 50_000  # write in chunks to keep memory low
RNG_SEED = 544


def make_vocab(n):
    random.seed(RNG_SEED)
    vocab = []
    for _ in range(n):
        L = random.randint(5, 10)
        vocab.append("".join(random.choice(string.ascii_lowercase) for _ in range(L)))
    return vocab


def generate_inputs(input_dir: Path):
    input_dir.mkdir(parents=True, exist_ok=True)
    sentinel = input_dir / ".done"
    if sentinel.exists():
        return

    vocab = make_vocab(VOCAB_SIZE)
    random.seed(RNG_SEED)

    for i in range(NUM_FILES):
        fn = input_dir / f"f{i:03d}.txt.gz"
        # fast compression to save time/CPU
        with gzip.open(fn, "wt", encoding="utf-8", compresslevel=1) as f:
            remaining = TOKENS_PER_FILE
            while remaining > 0:
                k = min(CHUNK_TOKENS, remaining)
                words = random.choices(vocab, k=k)
                f.write(" ".join(words))
                f.write("\n")
                remaining -= k

    sentinel.touch()


def time_run(python_bin: str, input_dir: Path, threads: int) -> float:
    # Use a temp output file per run; CSV avoids pyarrow overhead
    out_file = Path("/tmp") / f"bench_out_{python_bin.replace('.', '')}_{threads}.csv"
    cmd = [
        python_bin,
        "/app/final.py",
        str(input_dir),
        str(out_file),
        str(threads),
    ]
    t0 = time.perf_counter()
    # Capture output to keep the benchmark log tidy
    subprocess.run(cmd, check=True, text=True, capture_output=True)
    t1 = time.perf_counter()
    return t1 - t0


def write_csv(rows, out_csv: Path):
    with out_csv.open("w", newline="") as fp:
        w = csv.writer(fp)
        w.writerow(["threads", "gil_seconds", "nogil_seconds"])
        for r in rows:
            w.writerow(r)


def make_plot(df: pd.DataFrame, out_svg: Path):
    plt.figure(figsize=(12, 6))
    plt.plot(df["threads"], df["gil_seconds"], marker="o", label="Python 3.13 (with GIL)")
    plt.plot(df["threads"], df["nogil_seconds"], marker="x", linestyle="--", label="Python 3.13 (without GIL)")
    plt.title("Word Count Performance: GIL vs. No-GIL")
    plt.xlabel("Number of Threads")
    plt.ylabel("Execution Time (seconds)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_svg, format="svg")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3.13 thread_bench.py <output_directory>", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(sys.argv[1]).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Generate inputs once under /tmp so repeated runs reuse them
    input_dir = Path("/tmp/thread_bench_inputs")
    generate_inputs(input_dir)

    thread_grid = [1, 2, 4, 8, 16]
    results = []
    for t in thread_grid:
        gil_s = time_run("python3.13", input_dir, t)
        nogil_s = time_run("python3.13-nogil", input_dir, t)
        results.append((t, round(gil_s, 3), round(nogil_s, 3)))

    out_csv = out_dir / "threads.csv"
    write_csv(results, out_csv)

    df = pd.read_csv(out_csv)
    out_svg = out_dir / "threads.svg"
    make_plot(df, out_svg)

    # Minimal confirmation print (files are in mounted /outputs)
    print(f"Wrote {out_csv}")
    print(f"Wrote {out_svg}")


if __name__ == "__main__":
    main()
