import os
import subprocess
import re
import time
import filecmp
import gc
import sys

# Configuration Paths
REF_EXEC = "project2_ref/project2"
MY_EXEC = "project2/src/project2"

TEST_DIR = "tests"
CONFIG_DIR = "config"
OUT_REF_DIR = "out_ref"
OUT_MY_DIR = "out"

REF_TIME_FILE = "ref_running_time.txt"
COMPARE_TIME_FILE = "relative_running_time.txt"

NUM_TESTS = 20
NUM_RUNS = 3

def extract_time(stdout):
    match = re.search(r"([0-9]+\.[0-9]+)", stdout)
    return float(match.group(1)) if match else None

def clear_os_cache():
    """Forces Linux to drop file caches from RAM (Requires sudo)"""
    try:
        subprocess.run(["sync"], check=True)
        subprocess.run(["sysctl", "-w", "vm.drop_caches=3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    except subprocess.CalledProcessError:
        pass 

def run_program(executable, test_file, n, output_file, is_root):
    if os.path.exists(output_file):
        os.remove(output_file)
        
    cmd = [executable, test_file, n, output_file]
    times = []
    clean_env = os.environ.copy()
    
    for _ in range(NUM_RUNS):
        if is_root:
            clear_os_cache()
            
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=clean_env)
            t = extract_time(result.stdout)
            if t is not None:
                times.append(t)
        except subprocess.CalledProcessError as e:
            return None 
            
        time.sleep(0.05)
        
    return (sum(times) / NUM_RUNS) if len(times) == NUM_RUNS else None

def main():
    is_root = os.geteuid() == 0 if hasattr(os, 'geteuid') else False
    
    if is_root:
        print("🟢 Running as root: OS Disk Cache WILL be cleared between runs.")
    else:
        print("🟡 Running as normal user: OS Disk Cache will NOT be cleared.")
        print("   (Tip: Run with 'sudo python3 benchmark.py' for extreme accuracy)\n")

    os.makedirs(OUT_REF_DIR, exist_ok=True)
    os.makedirs(OUT_MY_DIR, exist_ok=True)

    print(f"Starting Benchmark: 20 Tests, {NUM_RUNS} runs per test.\n")

    with open(REF_TIME_FILE, 'w') as f_ref, open(COMPARE_TIME_FILE, 'w') as f_comp:
        
        f_ref.write(f"{'Test #':<8} | {'Chunks':<8} | {'Ref Avg Time (s)'}\n")
        f_ref.write("-" * 40 + "\n")
        
        comp_header = f"{'Test #':<8} | {'Output Match':<15} | {'Ref Avg':<12} | {'My Avg':<12} | {'Relative Speed'}"
        f_comp.write(comp_header + "\n")
        f_comp.write("-" * 75 + "\n")
        print(comp_header)
        print("-" * 75)

        for i in range(1, NUM_TESTS + 1):
            test_file = os.path.join(TEST_DIR, f"test{i}.txt")
            config_file = os.path.join(CONFIG_DIR, f"config{i}.txt")
            out_ref_file = os.path.join(OUT_REF_DIR, f"out{i}.txt")
            out_my_file = os.path.join(OUT_MY_DIR, f"out{i}.txt")

            with open(config_file, 'r') as f:
                match = re.search(r'\d+', f.read().strip())
                n = match.group() if match else "1"

            # 1. Run Reference Solution
            ref_avg = run_program(REF_EXEC, test_file, n, out_ref_file, is_root)
            
            # 2. Run My Solution
            my_avg = run_program(MY_EXEC, test_file, n, out_my_file, is_root)

            # 3. Compare Outputs
            output_match = "FAILED/MISSING"
            if os.path.exists(out_ref_file) and os.path.exists(out_my_file):
                if filecmp.cmp(out_ref_file, out_my_file, shallow=False):
                    output_match = "MATCH"
                else:
                    output_match = "MISMATCH"

            # 4. Calculate relative time and Log
            if ref_avg is not None and my_avg is not None:
                # --- NEW FIX: Protect against Division by Zero ---
                if ref_avg > 0:
                    relative_speed = my_avg / ref_avg 
                    speed_string = f"{relative_speed:.2f}x time"
                else:
                    speed_string = "N/A (Ref too fast)"
                # -----------------------------------------------

                f_ref.write(f"Test {i:<3} | {n:<8} | {ref_avg:.6f}\n")
                
                res_str = f"Test {i:<3} | {output_match:<15} | {ref_avg:<12.6f} | {my_avg:<12.6f} | {speed_string}"
                f_comp.write(res_str + "\n")
                print(res_str)
            else:
                f_ref.write(f"Test {i:<3} | Crashed or failed to parse time.\n")
                res_str = f"Test {i:<3} | {output_match:<15} | Error executing one or both programs."
                f_comp.write(res_str + "\n")
                print(res_str)
                
            f_ref.flush()
            f_comp.flush()
            gc.collect()

    print(f"\nDone! See '{REF_TIME_FILE}' and '{COMPARE_TIME_FILE}' for details.")

if __name__ == "__main__":
    main()