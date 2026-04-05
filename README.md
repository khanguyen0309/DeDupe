# DeDupe

Compare your chunk-level deduplication implementation against a reference binary using shared tests and timed runs.

## Layout

| Path               | Purpose                                                           |
| ------------------ | ----------------------------------------------------------------- |
| `project2/src/`    | Implemented solution                                              |
| `project2_ref/`    | Reference baseline                                                |
| `tests/`           | Input files `test1.txt` … `test20.txt`.                           |
| `config/`          | Chunk counts `config1.txt` … `config20.txt`.                      |
| `generate_data.py` | Generates tests 11–20 (1–10 are reserved for manual files).       |
| `benchmark.py`     | Runs reference and your binary, compares outputs, writes timings. |

Benchmark creates `out/` and `out_ref/` for outputs and `ref_running_time.txt`, `relative_running_time.txt` in this directory.

## Build

Requires GCC and OpenSSL (`-lcrypto`), as in the Makefiles.

```bash
cd project2_ref && make
cd ../project2/src && make
```

On Windows use a GCC toolchain with OpenSSL dev libraries (e.g. MSYS2 / MinGW-w64). The compiled program is often named `project2.exe`; `benchmark.py` resolves that automatically.

## Generate tests and run the benchmark

From the `DeDupe` directory:

```bash
python generate_data.py
python benchmark.py
```

Ensure tests 1–10 and matching configs exist before benchmarking if you rely on those cases (the generator skips 1–10).
