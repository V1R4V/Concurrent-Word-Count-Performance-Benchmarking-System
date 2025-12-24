import sys
import gzip
import threading
import queue
import os
from collections import Counter
import pandas as pd

def worker(q, lock, global_counts, per_file_counts, output_extension):
    while True:
        item = q.get()
        if item is None:
            q.task_done()
            break

        filepath, basename = item
        print(f"start {basename}")

        local_counter = Counter()
        try:
            with gzip.open(filepath, mode="rt", encoding="utf-8") as f:
                for line in f:
                    for word in line.split():
                        local_counter[word.lower()] += 1
        except Exception as e:
            print(f"Error processing file {basename}: {e}", file=sys.stderr)
            # Even if there's an error, we still need to signal task completion
            q.task_done()
            continue

        with lock:
            global_counts.update(local_counter)
            per_file_counts[basename] = local_counter

        print(f"finish {basename}")
        q.task_done()

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 lite.py <input_directory> <output_file> <threads>", file=sys.stderr)
        sys.exit(1)

    input_directory = sys.argv[1]
    output_file = sys.argv[2]
# BUG! Raw int() ValueError leaks through; no friendly "<threads> must be an integer > 0" message.
    threads = int(sys.argv[3])

    if not os.path.isdir(input_directory):
        print(f"Error: Input directory '{input_directory}' not found.", file=sys.stderr)
        sys.exit(1)

    if threads <= 0:
        print("Error: Number of threads must be greater than 0.", file=sys.stderr)
        sys.exit(1)

    output_extension = os.path.splitext(output_file)[1].lower()
    if output_extension not in ['.csv', '.parquet', '.arrow']:
        print(f"Error: Unsupported output file extension '{output_extension}'. Use .csv, .parquet, or .arrow.", file=sys.stderr)
        sys.exit(1)

    file_paths = []
    for filename in os.listdir(input_directory):
        if filename.endswith(".txt.gz"):
            file_paths.append((os.path.join(input_directory, filename), filename))

    if not file_paths:
        print(f"Error: No .txt.gz files found in '{input_directory}'.", file=sys.stderr)
        sys.exit(1)

    q = queue.Queue()
    lock = threading.Lock()
    global_counts = Counter()
    per_file_counts = {}

    # Adjust number of workers if there are fewer files than threads
    num_workers = min(threads, len(file_paths))
    worker_threads = []
    for _ in range(num_workers):
        t = threading.Thread(target=worker, args=(q, lock, global_counts, per_file_counts, output_extension))
        t.start()
        worker_threads.append(t)

    for filepath, basename in file_paths:
        q.put((filepath, basename))

    q.join()

    for _ in range(num_workers):
        q.put(None)
    for t in worker_threads:
        t.join()

    # Build output DataFrame
    all_words = sorted(global_counts.keys())
    data = []
    for word in all_words:
        row = [word, global_counts[word]]
        for _, basename in file_paths:
            row.append(per_file_counts.get(basename, Counter()).get(word, 0))
        data.append(row)

 # BUG! Per-file columns are taken in os.listdir() order via file_paths and are not sorted so column order is non-deterministic and may not match spec/tests.
    column_names = ["word", "count"] + [basename for _, basename in file_paths]
    df = pd.DataFrame(data, columns=column_names)
    df.sort_values(by="word", ascending=True, inplace=True)

    # Write output
    try:
        if output_extension == '.csv':
            df.to_csv(output_file, index=False)
        elif output_extension == '.parquet':
            df.to_parquet(output_file, index=False)
        elif output_extension == '.arrow':
            df.to_feather(output_file) # Arrow/Feather acceptable for this project
    except Exception as e:
        print(f"Error writing output file '{output_file}': {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)
