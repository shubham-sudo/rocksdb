import os
import shutil
import subprocess
import argparse
import datetime

def run_make_command(directory):
    current_dir = os.getcwd()
    os.chdir(directory)
    subprocess.run("make -j4", shell=True, check=True)
    os.chdir(current_dir)

def run_make_clean_command(directory):
    current_dir = os.getcwd()
    os.chdir(directory)
    subprocess.run("make clean", shell=True, check=True)
    os.chdir(current_dir)

def delete_files_in_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    else:
        print(f"Directory '{directory}' does not exist.")

def delete_workloads():
    current_dir = os.getcwd()
    for item in os.listdir(current_dir):
        if os.path.isdir(item) and item.startswith('load_gen_'):
            workload_path = os.path.join(item, 'workload.txt')
            if os.path.exists(workload_path):
                print(f"Deleting folder: {item}")
                shutil.rmtree(item)

def run_simple_example(workload_path, out_f, args, output_file, timestamp):
    shutil.copy(workload_path, "./rocksdb/examples/workload.txt")
    shutil.copy(workload_path, "./workload.txt")

    if args.rc_off:
        simple_example_command = "./rocksdb/examples/simple_example --rc-off"
    else:
        simple_example_command = "./rocksdb/examples/simple_example"
    print(f"Running simple_example: {simple_example_command}")
    try:
        p = subprocess.Popen(simple_example_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        stdout, stderr = p.communicate()
    except subprocess.CalledProcessError as e:
        print(f"Error in command: {simple_example_command}")
        print(f"Error message: {e.output}")
        raise

    out_f.write("\n####################################################################################################\n")
    out_f.write(f"\nRunning load: {workload_path}\n")
    out_f.write("\n####################################################################################################\n")
    out_f.write(stdout.decode("utf-8"))


    output_filtered_file = f"output_filtered_{timestamp}.txt"
    subprocess.run(f'grep -E "rocksdb.compaction.times.micros|#######################|Running load|\s+[[:digit:]]+\s+[[:digit:]]+\s+[[:digit:]]+|^--------------------$|Level Statistics|Level, $|rocksdb.bytes.written|rocksdb.number.db.seek |rocksdb.number.db.next|rocksdb.db.iter.bytes.read|rocksdb.no.file.opens|" {output_file} > {output_filtered_file}', shell=True, check=True)
    p.communicate()

def main(args):
    input_file = "workloads_info.txt"
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"output_{timestamp}.txt"

    if args.make_clean:
        print("Running make clean in all directories")
        run_make_clean_command("./rocksdb")

    if args.make or args.make_clean:
        print("Running make in all directories")
        run_make_command("./rocksdb")
        run_make_command("./rocksdb/examples")
        run_make_command("./K-V-Workload-Generator")

    with open(input_file, "r") as f:
        lines = f.readlines()

    with open(output_file, "w") as out_f:
        for line in lines:
            tokens = line.split()
            i_value = tokens[tokens.index("I") + 1]
            u_value = tokens[tokens.index("U") + 1]
            d_value = tokens[tokens.index("D") + 1]
            s_value = tokens[tokens.index("S") + 1]
            Y_value = tokens[tokens.index("Y") + 1]

            directory_name = f"./load_gen_I{i_value}_U{u_value}_D{d_value}_S{s_value}_Y{Y_value}"
            os.makedirs(directory_name, exist_ok=True)

            existing_workload_path = os.path.join(directory_name, "workload.txt")
            if os.path.exists(existing_workload_path):
                print(f"Workload already exists in {directory_name}. Copying workload.txt to rocksdb folder.")
            else:
                load_gen_command = f"./K-V-Workload-Generator/load_gen -I {i_value} -U {u_value} -D {d_value} -S {s_value} -Y {Y_value}"
                print(f"Running load_gen: {load_gen_command}")
                try:
                    p = subprocess.Popen(load_gen_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
                    p.communicate()
                except subprocess.CalledProcessError as e:
                    print(f"Error in command: {load_gen_command}")
                    print(f"Error message: {e.output}")
                    raise

                print("Copying workload.txt to rocksdb folder and the new directory")
                shutil.copy("./workload.txt", "./rocksdb/examples/workload.txt")
                shutil.copy("./workload.txt", existing_workload_path)

            if args.split_workload:
                iud_workload_path = os.path.join(directory_name, "IUD_workload.txt")
                s_workload_path = os.path.join(directory_name, "S_workload.txt")
                with open(existing_workload_path, "r") as workload_f, \
                     open(iud_workload_path, "w") as iud_f, \
                     open(s_workload_path, "w") as s_f:
                    for workload_line in workload_f.readlines():
                        if workload_line.startswith(("I", "U", "D")):
                            iud_f.write(workload_line)
                        elif workload_line.startswith("S"):
                            s_f.write(workload_line)

                run_simple_example(iud_workload_path, out_f, args, output_file, timestamp)
                run_simple_example(s_workload_path, out_f, args, output_file, timestamp)
            else:
                run_simple_example(existing_workload_path, out_f, args, output_file, timestamp)

            if not args.keep_tmp:
                print("Deleting previous files from tmp")
                delete_files_in_directory("/tmp/cs561_project1")
            else:
                print("Keeping previous files in tmp")
            timestamp2 = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_img = f"output_{timestamp2}.png"
            p = subprocess.Popen(f"python stats.py {output_img}", shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

    os.makedirs("output_log", exist_ok=True)
    shutil.move(output_file, f"output_log/{output_file}")
    shutil.move(f"output_filtered_{timestamp}.txt", f"output_log/output_filtered_{timestamp}.txt")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CS561 Project 1")
    parser.add_argument("--keep-tmp", action="store_true", help="Keep previous files in /tmp/cs561_project1")
    parser.add_argument("--del-workload", action="store_true", help="Delete all previously generated workloads")
    parser.add_argument("--make-clean", action="store_true", help="Run make clean in rocksdb directories")
    parser.add_argument("--make", action="store_true", help="Run make in all directories")
    parser.add_argument("--split-workload", action="store_true", help="Run simple example separately for IUD and S workloads")
    parser.add_argument("--rc-off", action="store_true", help="Turn off Range Compaction")
    args = parser.parse_args()
    if args.del_workload:
        delete_workloads()
    main(args)
