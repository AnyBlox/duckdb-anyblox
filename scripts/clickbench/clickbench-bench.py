import os
import json
import sys

TMP_DIR = "/tmp/duckdb-ignition-profiling"
QUERIES = [6, 11, 12, 13, 14, 15, 17, 18, 19, 21, 22, 23, 24, 26, 27, 29, 31, 32, 33, 34, 37, 38, 39, 40]
SAMPLES = 10


def get_profile_json_path(q):
    return f"{TMP_DIR}/clickbench-q{q}.json"


def get_profile_json(q):
    with open(get_profile_json_path(q), 'r') as file:
        return json.load(file)


def sum_scan_cputime(profile):
    total = 0
    if "operator_type" in profile and profile["operator_type"] == "TABLE_SCAN":
      if ("Function" in profile["extra_info"] and profile["extra_info"]["Function"] in ["IGNITION", "PARQUET_SCAN", "READ_PARQUET"])\
         or ("Text" in profile["extra_info"] and profile["extra_info"]["Text"] in ["hits_strings_0", "hits_strings_1", "hits_strings_2", "hits_strings_3"]):
        total += profile["operator_timing"]
    if "children" in profile:
        for child in profile["children"]:
            total += sum_scan_cputime(child)
    return total


if len(sys.argv) != 6:
    print(f"Usage: {sys.argv[0]} DUCKDB_EXE CLICKBENCH_DB QUERIES_DIR THREADS OUT_DIR")
    exit(1)

duckdb_exe = sys.argv[1]
clickbench_db = sys.argv[2]
queries_dir = sys.argv[3]
threads = int(sys.argv[4])
out_dir = sys.argv[5]

os.system(f"mkdir -p {TMP_DIR}")
os.system(f"mkdir -p {out_dir}")

samples = {}

for q in QUERIES:
    input_file = f"{queries_dir}/clickbench-q{q}.sql"
    sql_file_path = f"{TMP_DIR}/input_q{q}.sql"
    os.system(f"cat {input_file} {input_file} {input_file} {input_file} {input_file} > {sql_file_path}")
    os.system(f"echo \"SET threads = {threads};\n\" >> {sql_file_path}")
    os.system(f"echo \"SET enable_profiling = 'json';\n\" >> {sql_file_path}")
    os.system(f"echo \"SET profiling_output = '{get_profile_json_path(q)}';\n\" >> {sql_file_path}")
    os.system(f"cat {input_file} >> {sql_file_path}")
    samples[f"{q}"] = []
    
    for _ in range(SAMPLES):
        os.system(f"cat {sql_file_path} | {duckdb_exe} {clickbench_db}")
        profile = get_profile_json(q)
        latency = profile["latency"]
        scan_cputime = sum_scan_cputime(profile)
        samples[f"{q}"].append([latency, scan_cputime])


with open(f"{out_dir}/clickbench-t{threads}-results.json", 'w') as file:
    json.dump(samples, file)