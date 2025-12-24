import sys
import os
import gzip
import threading
import queue
from collections import Counter
import pandas as pd

def worker(q, lock, global_counts, per_file_counts):
    """Worker thread function to process files from the queue."""
    while True:
        path = q.get()
        if path is None:
            break

        basename = os.path.basename(path)
        print(f"start {basename}", flush=True)

        local_counter = Counter()
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                for line in f:
                    local_counter.update(word.lower() for word in line.split())
        except Exception as e:
            print(f"Error processing file {basename}: {e}", file=sys.stderr)
            q.task_done()
            continue

        with lock:
            global_counts.update(local_counter)
            per_file_counts[basename] = local_counter

        print(f"finish {basename}", flush=True)
        q.task_done()

def run():
    """Core logic for the word counting application."""
    if len(sys.argv) != 4:
        print("Usage: python3 app/flash.py <input_directory> <output_file> <threads>", file=sys.stderr)
        sys.exit(1)

    input_dir, output_file, threads_str = sys.argv[1:4]

    # Validate arguments
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory '{input_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)

    try:
        num_threads = int(threads_str)
        if num_threads <= 0:
            raise ValueError()
    except ValueError:
        print("Error: Threads must be a positive integer.", file=sys.stderr)
        sys.exit(1)

    output_ext = os.path.splitext(output_file)[1]
    if output_ext not in ['.csv', '.parquet', '.arrow']:
        print("Error: Output file extension must be .csv, .parquet, or .arrow.", file=sys.stderr)
        sys.exit(1)

    # Find files
    files_to_process = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.txt.gz')]
    if not files_to_process:
        print(f"Error: No .txt.gz files found in '{input_dir}'.", file=sys.stderr)
        sys.exit(1)

    # Setup for threading
    q = queue.Queue()
    for f in files_to_process:
        q.put(f)

    lock = threading.Lock()
    global_counts = Counter()
    per_file_counts = {}

    # Start workers
    threads = []
    num_workers = min(num_threads, len(files_to_process))
    for _ in range(num_workers):
        t = threading.Thread(target=worker, args=(q, lock, global_counts, per_file_counts))
        t.start()
        threads.append(t)

    # Wait for all files to be processed
    q.join()

    # Stop workers with sentinel values
    for _ in range(num_workers):
        q.put(None)
    for t in threads:
        t.join()

    # Process results into DataFrame
    all_words = sorted(global_counts.keys())
    file_basenames = sorted(per_file_counts.keys())
    
    columns = ['word', 'count'] + file_basenames
    data = []
    for word in all_words:
        row = {'word': word, 'count': global_counts[word]}
        for basename in file_basenames:
            row[basename] = per_file_counts[basename].get(word, 0)
        data.append(row)

    df = pd.DataFrame(data, columns=columns)
    
    # Write output
    if output_ext == '.csv':
        df.to_csv(output_file, index=False)
    elif output_ext == '.parquet':
        df.to_parquet(output_file, index=False)
    elif output_ext == '.arrow':
        df.to_feather(output_file)

def main():
    """Entry point with top-level error handling."""
    try:
        run()
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
