import pyarrow.compute as pc
import os
import sys
import time
import gzip
import random
import string
import tempfile
import subprocess
from pathlib import Path

import pyarrow.parquet as pq
import pandas as pd
import pyarrow.feather as paf
import matplotlib.pyplot as plt

PROGRAM = "/app/final.py"   # hardcode final.py as required
FILES = 256                 # number of input .txt.gz files
TOKENS_PER_FILE = 1_000_000 # tokens per file
VOCAB_SIZE = 1000           # size of vocabulary
THREADS = 8                 # threads when producing outputs

def rand_word(rng: random.Random) -> str:
    L = rng.randint(3, 8)
    return "".join(rng.choice(string.ascii_lowercase) for _ in range(L))

def make_vocab(rng: random.Random, n=VOCAB_SIZE):
    return [rand_word(rng) for _ in range(n)]

def generate_inputs(root: Path, rng_seed: int = 544):
    """
    Generate exactly 256 .txt.gz files.
    Each contains 1,000,000 whitespace-separated words from a 1,000-word vocab.
    Stream-write to avoid large memory use.
    """
    root.mkdir(parents=True, exist_ok=True)
    rng = random.Random(rng_seed)
    vocab = make_vocab(rng)

    for i in range(FILES):
        fn = root / f"f{i:03d}.txt.gz"
        with gzip.open(fn, "wt", encoding="utf-8") as f:
            remaining = TOKENS_PER_FILE
            chunk = 10_000  # write in chunks for speed/memory
            while remaining > 0:
                k = min(chunk, remaining)
                words = (rng.choice(vocab) for _ in range(k))
                f.write(" ".join(words))
                f.write("\n")
                remaining -= k

def run_program(input_dir: Path, out_file: Path, threads: int = THREADS):
    cmd = ["python3.13", PROGRAM, str(input_dir), str(out_file), str(threads)]
    subprocess.run(cmd, check=True, capture_output=True, text=True)

def time_read_csv(path: Path) -> float:
    t0 = time.perf_counter()
    s = pd.read_csv(path, usecols=["count"])["count"].sum()
    _ = s
    return time.perf_counter() - t0

def time_read_parquet(path: Path) -> float:
    t0 = time.perf_counter()
    # True column projection via Arrow + mmap
    tbl = pq.read_table(str(path), columns=["count"], memory_map=True)
    _ = pc.sum(tbl["count"]).as_py()
    return time.perf_counter() - t0

def time_read_arrow(path: Path) -> float:
    t0 = time.perf_counter()
    # Memory-map and read ONLY the 'count' column
    tbl = paf.read_table(str(path), memory_map=True, columns=["count"])
    # Sum via Arrow (works on Column/ChunkedArray)
    s = pc.sum(tbl["count"]).as_py()
    _ = s
    return time.perf_counter() - t0


def main():
    if len(sys.argv) != 2:
        print("Usage: python3.13 format_bench.py <output_directory>", file=sys.stderr)
        sys.exit(1)

    outdir = Path(sys.argv[1])
    outdir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_in = Path(tmp) / "inputs"
        generate_inputs(tmp_in)

        csv_path = outdir / "formats.csv.out.csv"
        parquet_path = outdir / "formats.parquet.out.parquet"
        arrow_path = outdir / "formats.arrow.out.arrow"

        run_program(tmp_in, csv_path, THREADS)
        run_program(tmp_in, parquet_path, THREADS)
        run_program(tmp_in, arrow_path, THREADS)

        results = [
            ("csv",     time_read_csv(csv_path)),
            ("parquet", time_read_parquet(parquet_path)),
            ("arrow",   time_read_arrow(arrow_path)),
        ]

    df = pd.DataFrame(results, columns=["format", "read_seconds"])
    df.to_csv(outdir / "formats.csv", index=False)

    plt.figure(figsize=(8, 5))
    plt.bar(df["format"], df["read_seconds"] * 1000.0, alpha=0.6)
    plt.title("Performance of Reading 'count' Column by Format")
    plt.xlabel("Format")
    plt.ylabel("Read Time (milliseconds)")
    plt.tight_layout()
    plt.savefig(outdir / "formats.svg")

if __name__ == "__main__":
    main()
