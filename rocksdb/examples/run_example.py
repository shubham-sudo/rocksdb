import os
import shutil
import subprocess
import argparse

def main(args):
    input_file = "workloads_info.txt"
    output_file = "output.txt"

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

            # Create a directory based on load_gen_command
            directory_name = f"./load_gen_I{i_value}_U{u_value}_D{d_value}_S{s_value}_Y{Y_value}"
            os.makedirs(directory_name, exist_ok=True)

            # Check if the workload.txt file already exists in the directory
            existing_workload_path = os.path.join(directory_name, "workload.txt")
            if os.path.exists(existing_workload_path):
                print(f"Workload already exists in {directory_name}. Copying workload.txt to rocksdb folder.")
                shutil.copy(existing_workload_path, "./rocksdb/examples/workload.txt")
                shutil.copy(existing_workload_path, "./workload.txt")
            else:
                # Execute load_gen with the given parameters
                load_gen_command = f"./K-V-Workload-Generator/load_gen -I {i_value} -U {u_value} -D {d_value} -S {s_value} -Y {Y_value}"
                print(f"Running load_gen: {load_gen_command}")
                try:
                    p = subprocess.Popen(load_gen_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
                    p.communicate()
                except subprocess.CalledProcessError as e:
                    print(f"Error in command: {load_gen_command}")
                    print(f"Error message: {e.output}")
                    raise

                # Copy workload.txt to the rocksdb folder and the new directory
                print("Copying workload.txt to rocksdb folder and the new directory")
                shutil.copy("./workload.txt", "./rocksdb/examples/workload.txt")
                shutil.copy("./workload.txt", existing_workload_path)
            # Execute simple_example and store output
            simple_example_command = "./rocksdb/examples/simple_example"
            try:
                print(f"Running simple_example: {simple_example_command}")
                current_dir = os.getcwd()
                os.chdir("./rocksdb/examples")
                p = subprocess.Popen(simple_example_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
                stdout, stderr = p.communicate()
                os.chdir(current_dir)
            except subprocess.CalledProcessError as e:
                print(f"Error in command: {simple_example_command}")
                print(f"Error message: {e.output}")
                raise

            # Write output to output.txt
            out_f.write("\n####################################################################################################\n")
            out_f.write(f"\nRunning load: -I {i_value} -U {u_value} -D {d_value} -S {s_value} -Y {Y_value}\n")
            out_f.write("\n####################################################################################################\n")
            out_f.write(stdout.decode("utf-8"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CS561 Project 1")
    parser.add_argument("--keep-tmp", action="store_true", help="Keep previous files in /tmp/cs561_project1")
    args = parser.parse_args()
    main(args)
