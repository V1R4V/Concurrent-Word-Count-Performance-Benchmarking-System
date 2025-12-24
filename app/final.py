#!/usr/bin/env python3

import sys
import os
import gzip
import threading
from queue import Queue
from collections import Counter


def worker(q, lock, global_counts, per_file_counts):
    """Processes files from the queue until a sentinel (None) is received."""
    while True:
        filepath = q.get()
        if filepath is None:
            break

        basename = os.path.basename(filepath)
        print(f"start {basename}")

        local_counter = Counter()
        try:
            with gzip.open(filepath, "rt", encoding="utf-8") as f:
                for line in f:
                    local_counter.update(line.lower().split())
        except Exception as e:
            sys.stderr.write(f"Error processing {basename}: {e}\n")

        with lock:
            global_counts.update(local_counter)
            per_file_counts[basename] = local_counter

        print(f"finish {basename}")
        q.task_done()


def main():
    """Main function to orchestrate the word count process."""
    if len(sys.argv) != 4:
        sys.stderr.write(f"Usage: python3 {sys.argv[0]} <input_directory> <output_file> <threads>\n")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_file = sys.argv[2]
    try:
        num_threads_arg = int(sys.argv[3])
        if num_threads_arg <= 0:
            raise ValueError
    except ValueError:
        sys.stderr.write("Error: <threads> must be an integer > 0\n")
        sys.exit(1)

    if not os.path.isdir(input_dir):
        sys.stderr.write(f"Error: Input directory '{input_dir}' not found or is not a directory.\n")
        sys.exit(1)

    output_ext = os.path.splitext(output_file)[1]
    if output_ext not in [".csv", ".parquet", ".arrow"]:
        sys.stderr.write("Error: Output file extension must be .csv, .parquet, or .arrow\n")
        sys.exit(1)

    try:
        files = sorted([
            os.path.join(input_dir, f)
            for f in os.listdir(input_dir)
            if f.endswith(".txt.gz")
        ])
        if not files:
            sys.stderr.write(f"Error: No .txt.gz files found in '{input_dir}'.\n")
            sys.exit(1)
    except OSError as e:
        sys.stderr.write(f"Error reading directory {input_dir}: {e}\n")
        sys.exit(1)

    q = Queue()
    lock = threading.Lock()
    global_counts = Counter()
    per_file_counts = {}

    for f in files:
        q.put(f)

    num_workers = min(num_threads_arg, len(files))
    threads = []
    for _ in range(num_workers):
        t = threading.Thread(
            target=worker,
            args=(q, lock, global_counts, per_file_counts)
        )
        t.start()
        threads.append(t)

    q.join()

    for _ in range(num_workers):
        q.put(None)
    for t in threads:
        t.join()

    all_words = sorted(global_counts.keys())
    file_basenames = sorted([os.path.basename(f) for f in files])

    data = {
        'word': all_words,
        'count': [global_counts[w] for w in all_words]
    }
    for basename in file_basenames:
        file_counter = per_file_counts.get(basename, Counter())
        data[basename] = [file_counter.get(w, 0) for w in all_words]

    import pandas as pd  # lazy import to keep nogil parallelism
    df = pd.DataFrame(data)

    try:
        if output_ext == ".csv":
            df.to_csv(output_file, index=False)
        elif output_ext == ".parquet":
            df.to_parquet(output_file, index=False)
        elif output_ext == ".arrow":
            df.to_feather(output_file)
    except Exception as e:
        sys.stderr.write(f"Error writing output file {output_file}: {e}\n")
        sys.exit(1)


def run():
    """Wrapper to catch all exceptions from main and exit gracefully."""
    try:
        main()
    except Exception as e:
        sys.stderr.write(f"An unexpected error occurred: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    run()
