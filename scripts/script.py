import os
import shutil
import subprocess
import argparse
import datetime
from pathlib import Path
from typing import Dict

from graphs import *


CURRENT_DIR = Path(__file__).parent
ROCKS_DB_DIR = CURRENT_DIR.parent
OUTPUT_LOG = CURRENT_DIR.joinpath("output_log")
WORKLOADS_DIR = CURRENT_DIR.joinpath("workloads")
RESULTS_DIR = CURRENT_DIR.joinpath("results")
GRAPH_DIR = CURRENT_DIR.joinpath("graphs")
K_V_WORKLOAD_GENERATOR_DIR = CURRENT_DIR.joinpath("K-V-Workload-Generator")

WORKLOAD_INFO = CURRENT_DIR.joinpath("workloads_info.txt")


# def initialize():
#     if not WORKLOADS_DIR.exists():
#         WORKLOADS_DIR.mkdir()
#     if not RESULTS_DIR.exists():
#         RESULTS_DIR.mkdir()
#     if not GRAPH_DIR.exists():
#         GRAPH_DIR.mkdir()
#     if not OUTPUT_LOG.exists():
#         OUTPUT_LOG.mkdir()


# def run_command(command):
#     """Run the specified command."""
#     print(f"\n\t\t ######## Running command ########\n{command}")
#     try:
#         p = subprocess.Popen(
#             command,
#             shell=True,
#             stderr=subprocess.PIPE,
#             stdout=subprocess.PIPE,
#             stdin=subprocess.PIPE,
#         )
#         stdout, stderr = p.communicate()
#     except subprocess.CalledProcessError as e:
#         print(f"Error in command: {command}")
#         print(f"Error message: {e.output}")
#         raise
#     # print(stdout.decode("utf-8"))
#     print("Done running command.")


# def remake_rocksdb():
#     """Re-make rocksdb."""
#     print("\n\t\t######## Remaking rocksdb ########")
#     os.chdir(ROCKS_DB_DIR)
#     run_command("make clean")
#     run_command("make -j4 static_lib")
#     os.chdir(CURRENT_DIR)
#     print("Done remaking rocksdb.")


# def remake_simple_example():
#     """Re-make simple_example."""
#     print("\n\t\t######## Remaking simple_example ########")
#     os.chdir(ROCKS_DB_DIR.joinpath("examples"))
#     run_command("make clean")
#     run_command("make simple_example")
#     os.chdir(CURRENT_DIR)
#     print("Done remaking simple_example.")


# def remake_load_generator():
#     """Re-make load_generator."""
#     print("\n\t\t######## Remaking load_generator ########")
#     os.chdir(K_V_WORKLOAD_GENERATOR_DIR)
#     run_command("make clean")
#     run_command("make")
#     os.chdir(CURRENT_DIR)
#     print("Done remaking load_generator.")


# def remove_dir(directory: Path):
#     """Delete all files in the specified directory."""
#     if directory.exists():
#         print(f"Deleting files in directory: {directory}")
#         shutil.rmtree(directory)
#     else:
#         print(f"Directory '{directory}' does not exist.")


# def delete_workloads():
#     """Delete all workloads in the current directory."""

#     for item in os.listdir(WORKLOADS_DIR):
#         workload_dir = WORKLOADS_DIR.joinpath(item)
#         if os.path.isdir(workload_dir) and item.startswith("load_gen_"):
#             workload_path = workload_dir.joinpath("workload.txt")
#             if workload_path.exists():
#                 print(f"Deleting folder: {item}")
#                 remove_dir(workload_dir)


# def run_simple_example(workload_path: Path, output_file_path: Path, args: Dict, foldername: str, range_queries: bool = False):
#     """Run the simple_example program with the given workload file."""

#     shutil.copy(workload_path, "./workload.txt")

#     if not range_queries:
#         temp_file = RESULTS_DIR.joinpath("temp.csv")
#         temp_file.touch()
#         run_command(f"../examples/simple_example {temp_file} >> {output_file_path}")
#         temp_file.unlink()
#         return

#     file_modifier = f"_{foldername}_rc_off"
#     vanilla_range_stats = RESULTS_DIR.joinpath(f"rangeQTime{file_modifier}.csv")
#     vanilla_range_stats.touch()
#     run_command(f"../examples/simple_example {vanilla_range_stats} --rc-off >> {output_file_path}")
#     range_queries_individual_line_plot(vanilla_range_stats, foldername)

#     if not args.rc_off:
#         file_modifier = f"_{foldername}_rc_on"
#         query_driven_range_stats = RESULTS_DIR.joinpath(f"rangeQTime{file_modifier}.csv")
#         query_driven_range_stats.touch()
#         run_command(f"../examples/simple_example {query_driven_range_stats} >> {output_file_path}")
#         range_queries_individual_line_plot(query_driven_range_stats, foldername)

#         range_queries_comparison_line_plot(query_driven_range_stats, vanilla_range_stats, foldername)
#         range_queries_comparison_histogram(query_driven_range_stats, vanilla_range_stats, foldername)

#     print("Done running simple_example.")


# def run_workload(args: Dict, workload_dir: Path, folder_name: str):
#     """Run the simple_example program with the given workload file."""

#     workload_path = workload_dir.joinpath("workload.txt")
#     timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#     output_file = OUTPUT_LOG.joinpath(f"output_{timestamp}.txt")

#     graph_sub_folder = GRAPH_DIR.joinpath(folder_name)
#     graph_sub_folder.mkdir(exist_ok=True)
#     range_queries = 0

#     if args.split_workload:
#         iud_workload_path = workload_dir.joinpath("iud_workload.txt")
#         s_workload_path = workload_dir.joinpath("s_workload.txt")
#         # range_query_stats_file = RESULTS_DIR.joinpath(f"rangeQTime{file_modifier}.csv")
    
#         with open(iud_workload_path, "w") as iud_f, open(s_workload_path, "w") as s_f, open(workload_path, "r") as f:
#             for line in f.readlines():
#                 if line.startswith("I") or line.startswith("U") or line.startswith("D"):
#                     iud_f.write(line)
#                 elif line.startswith("S"):
#                     range_queries += 1
#                     s_f.write(line)
        
#         run_simple_example(iud_workload_path, output_file, args, folder_name)
#         run_simple_example(s_workload_path, output_file, args, folder_name, range_queries=range_queries > 0)

#     else:
#         run_simple_example(workload_path, output_file, args, folder_name, range_queries=range_queries > 0)


# def run_load_generator(inserts: int, updates: int, deletes: int, range_queries: int, selectivity: float, directory: Path):
#     """Run the load_generator program with the given parameters."""
#     print("\n\t\t######## Running load_generator ########")
#     os.chdir(directory)
#     load_generator_command = f"{K_V_WORKLOAD_GENERATOR_DIR}/load_gen -I {inserts} -U {updates} -D {deletes} -S {range_queries} -Y {selectivity}"
#     run_command(load_generator_command)
#     os.chdir(CURRENT_DIR)
#     print("Done running load_generator.")


# def main(args):
#     """Run the main script."""

#     initialize()

#     if args.del_workload:
#         delete_workloads()

#     if args.remake:
#         remake_rocksdb()
#         remake_simple_example()
#         remake_load_generator()

#     lines = list()
#     with open(WORKLOAD_INFO, "r") as f:  # format of line -> I 400 U 60 D 10 S 5 Y 0.01
#         lines = f.readlines()

#     for line in lines:  # format of line -> I 400 U 60 D 10 S 5 Y 0.01
#         tokens = line.split()
#         num_inserts = tokens[tokens.index("I") + 1]
#         num_udpates = tokens[tokens.index("U") + 1]
#         num_deletes = tokens[tokens.index("D") + 1]
#         num_range_queries = tokens[tokens.index("S") + 1]
#         selectivity = tokens[tokens.index("Y") + 1]

#         folder_name = f"I{num_inserts}_U{num_udpates}_D{num_deletes}_S{num_range_queries}_Y{selectivity}"
#         workload_dir = WORKLOADS_DIR.joinpath(f"load_gen_I{num_inserts}_U{num_udpates}_D{num_deletes}_S{num_range_queries}_Y{selectivity}")
#         workload_txt_file = workload_dir.joinpath("workload.txt")

#         if not workload_dir.exists():
#             workload_dir.mkdir(parents=True, exist_ok=True)
        
#         if workload_txt_file.exists():
#             print(f"Workload already exists : {workload_dir}")
#         else:
#             run_load_generator(num_inserts, num_udpates, num_deletes, num_range_queries, selectivity, workload_dir)

#         run_workload(args=args, workload_dir=workload_dir, folder_name=folder_name)

#     if not args.keep_database:
#         print("Cleaning up /tmp/cs561_project1 database files")
#         remove_dir(Path("/tmp/cs561_project1"))
#     else:
#         print("Keeping /tmp/cs561_project1 database files")


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="CS561 Project 1")
#     parser.add_argument(
#         "--keep-database",
#         action="store_true",
#         help="Keep previous files in /tmp/cs561_project1 database",
#     )
#     parser.add_argument(
#         "--del-workload",
#         action="store_true",
#         help="Delete all previously generated workloads",
#     )
#     parser.add_argument(
#         "--remake",
#         action="store_true",
#         help="Run make clean in rocksdb directories",
#     )
#     parser.add_argument(
#         "--split-workload",
#         action="store_true",
#         help="Run simple example separately for IUD and S workloads",
#     )
#     parser.add_argument(
#         "--rc-off", action="store_true", default=False, help="Turn off Range Compaction"
#     )

#     args = parser.parse_args()
#     main(args)


def gen_line(fd):
    while line := fd.readline():
        yield line

workload_dir = Path(__file__).parent
iud_workload_path = workload_dir.joinpath("iud_workload.txt")
s_workload_path = workload_dir.joinpath("s_workload.txt")
workload_path = K_V_WORKLOAD_GENERATOR_DIR.joinpath("workload.txt")

# if not iud_workload_path.exists():
#     iud_workload_path.touch()

# if not s_workload_path.exists():
#     s_workload_path.touch()

with open(iud_workload_path, "w") as iud_f, open(s_workload_path, "w") as s_f, open(workload_path, "r") as f:
    for line in gen_line(f):
        if line.startswith("I") or line.startswith("U") or line.startswith("D"):
            iud_f.write(line)
        elif line.startswith("S"):
            s_f.write(line)
