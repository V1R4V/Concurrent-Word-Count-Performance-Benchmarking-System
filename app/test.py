import os
import gzip
import tempfile
import subprocess
import pandas as pd
import pytest

# Program under test; default to final.py but allow PROGRAM env override
PROGRAM = os.getenv("PROGRAM", "final.py")
PROGRAM_PATH = os.path.join("/app", PROGRAM)  # required invocation path

@pytest.fixture
def run_word_count_program():
    """Run the word count program and capture stdout/stderr."""
    def _run(input_dir, output_file, threads, check_return_code=True):
        cmd = ["python3.13", PROGRAM_PATH, input_dir, output_file, str(threads)]
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if check_return_code:
            res.check_returncode()
        return res
    return _run

@pytest.fixture
def create_input_files():
    """Create a temp dir with three gz files."""
    with tempfile.TemporaryDirectory() as d:
        files = {
            "file1.txt.gz": "hello world hello",
            "file2.txt.gz": "world python",
            "file3.txt.gz": "Python hello Python",
        }
        for name, content in files.items():
            path = os.path.join(d, name)
            with gzip.open(path, "wt", encoding="utf-8") as f:
                f.write(content)
        yield d, sorted(files.keys())

def expected_df():
    """Expected DataFrame (sorted by 'word')."""
    data = {
        "word": ["hello", "python", "world"],
        "count": [3, 3, 2],
        "file1.txt.gz": [2, 0, 1],
        "file2.txt.gz": [0, 1, 1],
        "file3.txt.gz": [1, 2, 0],
    }
    return pd.DataFrame(data).sort_values("word").reset_index(drop=True)

class TestWordCount:
    def test_happy_path_csv(self, run_word_count_program, create_input_files):
        input_dir, _ = create_input_files
        with tempfile.TemporaryDirectory() as outd:
            out = os.path.join(outd, "out.csv")
            run_word_count_program(input_dir, out, 2)
            got = pd.read_csv(out)
            exp = expected_df()
            pd.testing.assert_frame_equal(got, exp, check_dtype=False)

    def test_start_finish_prints(self, run_word_count_program, create_input_files):
        input_dir, basenames = create_input_files
        with tempfile.TemporaryDirectory() as outd:
            out = os.path.join(outd, "out.csv")
            res = run_word_count_program(input_dir, out, 2)
            lines = [ln for ln in res.stdout.strip().split("\n") if ln]
            for base in basenames:
                s = f"start {base}"
                f = f"finish {base}"
                assert s in lines, f"missing: {s}"
                assert f in lines, f"missing: {f}"
                assert lines.index(s) < lines.index(f), f"'start' not before 'finish' for {base}"

    def test_bad_extension_error(self, run_word_count_program, create_input_files):
        input_dir, _ = create_input_files
        with tempfile.TemporaryDirectory() as outd:
            out = os.path.join(outd, "out.BAD")
            res = run_word_count_program(input_dir, out, 1, check_return_code=False)
            assert res.returncode != 0
            err = res.stderr.lower()
            # robust to exact phrasing across implementations
            assert "extension" in err and ("csv" in err or "parquet" in err or "arrow" in err), res.stderr

    def test_empty_input_dir_error(self, run_word_count_program):
        with tempfile.TemporaryDirectory() as inp:
            with tempfile.TemporaryDirectory() as outd:
                out = os.path.join(outd, "out.csv")
                res = run_word_count_program(inp, out, 1, check_return_code=False)
                assert res.returncode != 0
                assert "no .txt.gz files" in res.stderr.lower()

    def test_too_few_args_error(self):
        # Missing 'threads' arg
        cmd = ["python3.13", PROGRAM_PATH, "dummy_in", "dummy_out.csv"]
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        assert res.returncode != 0
        assert "usage" in res.stderr.lower()

    @pytest.mark.parametrize(
        "ext,reader",
        [
            (".csv", pd.read_csv),
            (".parquet", pd.read_parquet),
            (".arrow", pd.read_feather),
        ],
    )
    def test_all_formats(self, run_word_count_program, create_input_files, ext, reader):
        input_dir, _ = create_input_files
        with tempfile.TemporaryDirectory() as outd:
            out = os.path.join(outd, f"out{ext}")
            run_word_count_program(input_dir, out, 2)
            got = reader(out)
            exp = expected_df()
            pd.testing.assert_frame_equal(got, exp, check_dtype=False)

    def test_invalid_threads_values(self, run_word_count_program, create_input_files):
        input_dir, _ = create_input_files
        with tempfile.TemporaryDirectory() as outd:
            out = os.path.join(outd, "out.csv")

            # non-integer
            res = run_word_count_program(input_dir, out, "abc", check_return_code=False)
            assert res.returncode != 0
            assert "thread" in res.stderr.lower()

            # zero
            res = run_word_count_program(input_dir, out, 0, check_return_code=False)
            assert res.returncode != 0
            assert "thread" in res.stderr.lower()

            # negative
            res = run_word_count_program(input_dir, out, -1, check_return_code=False)
            assert res.returncode != 0
            assert "thread" in res.stderr.lower()
