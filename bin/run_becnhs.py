import argparse
import os
import sys
import subprocess
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(description="Cross-platform Message Queue Benchmark Suite")
    parser.add_argument(
        "--max",
        action="store_true", 
        help="Run additional full matrix strategy x profile x topology x capacity"
    )
    parser.add_argument(
        "--all-threads", 
        action="store_true", 
        help="Use all hardware threads (ratio = 1.0)"
    )
    parser.add_argument(
        "--thread-ratio", 
        type=float, 
        default=1.0, 
        help="Thread ratio to use (default: 1.0)"
    )
    parser.add_argument(
        "--repetitions", 
        type=int, 
        default=7, 
        help="Number of repetitions per benchmark (default: 7)"
    )
    parser.add_argument(
        "--march-native",
        action="store_true",
        help="Enable native CPU optimization flags for benchmark build"
    )
    return parser.parse_args()

def main():
    args = parse_args()

    if args.all_threads:
        args.thread_ratio = 1.0

    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent
    out_dir = root_dir / "build" / "benchmark_results"
    report_path = out_dir / "report.html"
    build_dir = root_dir / "build"

    build_dir.mkdir(parents=True, exist_ok=True)
    native_flag = "ON" if args.march_native else "OFF"
    configure_cmd = [
        "cmake",
        "-B",
        str(build_dir),
        "-DCMAKE_BUILD_TYPE=Release",
        f"-DMESSAGE_QUEUE_MARCH_NATIVE={native_flag}",
        str(root_dir),
    ]
    print(f"[INFO] Configuring CMake (MESSAGE_QUEUE_MARCH_NATIVE={native_flag})...")
    try:
        subprocess.run(configure_cmd, check=True)
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        print(f"[ERROR] CMake configuration failed. Ensure CMake is installed: {e}")
        return 1

    print("[INFO] Building benchmark (Release)...")
    try:
        subprocess.run(
            ["cmake", "--build", str(build_dir), "--config", "Release", "--target", "message_queue_benchmark"],
            check=True
        )
    except subprocess.SubprocessError as e:
        print(f"[ERROR] Benchmark build failed: {e}")
        return 1

    exec_ext = ".exe" if os.name == "nt" else ""
    exec_name = f"message_queue_benchmark{exec_ext}"
    
    search_paths = [
        build_dir / "bin" / "Release" / exec_name,
        build_dir / "bin" / "RelWithDebInfo" / exec_name,
        build_dir / "bin" / exec_name,
        build_dir / exec_name,
    ]
    
    bench_path = None
    for path in search_paths:
        if path.exists():
            bench_path = path
            break

    if not bench_path:
        print("[ERROR] Benchmark executable not found in build directories.")
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)

    common_args = [
        f"--benchmark_repetitions={args.repetitions}",
        "--benchmark_min_time=0s",
        "--benchmark_display_aggregates_only=true",
        "--benchmark_out_format=json"
    ]

    print("\n=== Message Queue Benchmark Suite ===")
    print(f"Executable: {bench_path}")
    print(f"Output dir: {out_dir}")
    print("Mode: hardened default suite")
    print(f"Repetitions: {args.repetitions}")
    print(f"Thread ratio: {args.thread_ratio}")
    print(f"Use all threads: {'Yes' if args.all_threads else 'No'}")
    print(f"Native CPU flags: {'Enabled' if args.march_native else 'Disabled'}")
    print(f"Full matrix (--max): {'Enabled' if args.max else 'Skipped'}\n")

    def run_suite(stem, filter_str, label, messages, timeout, no_backoff, capacity):
        env = os.environ.copy()
        env["MQ_BENCH_REPETITIONS"] = str(args.repetitions)
        env["MQ_BENCH_THREAD_RATIO"] = str(args.thread_ratio)
        env["MQ_BENCH_USE_ALL_THREADS"] = "1" if args.all_threads else "0"
        env["MQ_BENCH_MESSAGES"] = str(messages)
        env["MQ_BENCH_TIMEOUT_SEC"] = str(timeout)
        env["MQ_BENCH_NO_BACKOFF"] = str(no_backoff)
        if capacity:
            env["MQ_BENCH_CAPACITY"] = str(capacity)
        else:
            env.pop("MQ_BENCH_CAPACITY", None)

        json_path = out_dir / f"{stem}.json"
        tmp_path = out_dir / f"{stem}.json.tmp"
        if tmp_path.exists():
            tmp_path.unlink()

        cmd = [
            str(bench_path),
            f"--benchmark_filter={filter_str}",
            f"--benchmark_out={tmp_path}",
        ] + common_args

        print(f"[{stem}] {label}")
        print(f"    messages={messages} timeout={timeout}s no_backoff={no_backoff} capacity={capacity or 'default'}")
        
        try:
            subprocess.run(cmd, env=env, check=True)
            tmp_path.replace(json_path)
        except subprocess.SubprocessError:
            if tmp_path.exists():
                tmp_path.unlink()
            print(f"[WARN] Benchmark suite {stem} returned non-zero exit code.")

    run_suite("01_throughput_spsc", "BM_QueueScenario/[0-4]/0/0/1024/100000", "Throughput baseline SPSC balanced no backoff", 500000, 120, 1, 1024)
    run_suite("02_contention_mpsc", "BM_QueueScenario/[0-4]/0/1/1024/100000", "MPSC contention balanced no backoff", 500000, 180, 1, 1024)
    run_suite("03_contention_mpmc", "BM_QueueScenario/[0-4]/0/3/1024/100000", "MPMC contention balanced no backoff", 500000, 180, 1, 1024)
    run_suite("04_overwrite_pressure", "BM_QueueScenario/1/[0-5]/[0-3]/64/100000", "Overwrite pressure small capacity all topologies", 500000, 180, 1, 64)
    run_suite("05_latency_profiles", "BM_QueueScenario/[0-4]/[1-5]/0/1024/100000", "Latency profiles fast batch burst with backoff", 200000, 180, 0, 1024)

    sweep_filter = "BM_QueueScenario/[0-4]/0/0/64/100000|BM_QueueScenario/[0-4]/0/0/1024/100000|BM_QueueScenario/[0-4]/0/0/4096/100000"
    run_suite("06_capacity_sweep", sweep_filter, "Capacity sweep SPSC balanced no backoff", 500000, 120, 1, "")

    if args.max:
        print("\n[INFO] Running full matrix strategy x profile x topology x capacity.")
        run_suite("99_full_matrix", "BM_QueueScenario", "Full matrix profile backoff enabled", 100000, 600, 0, "")

    print("\n=== Building HTML report ===")
    report_script = root_dir / "bin" / "benchmark_report.py"
    if not report_script.exists():
        report_script = script_dir / "benchmark_report.py"

    try:
        subprocess.run(
            [sys.executable, str(report_script), str(out_dir), "-o", str(report_path)],
            check=True
        )
        print(f"\nDone. Report successfully generated: {report_path}")
    except subprocess.SubprocessError:
        print("[ERROR] Failed to generate HTML report.")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())