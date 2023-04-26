import sys
import csv
import os
import numpy as np
import glob
import matplotlib.pyplot as plt

def calculate_stats(values):
    mean = np.mean(values)
    median = np.median(values)
    minimum = np.min(values)
    maximum = np.max(values)
    stddev = np.std(values)
    variance = np.var(values)
    percentile_95 = np.percentile(values, 95)
    percentile_99 = np.percentile(values, 99)
    return mean, median, minimum, maximum, stddev, variance, percentile_95, percentile_99

def read_stats(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)
        stats = [float(i) for i in next(reader)]
    return headers, stats

# Create the output folder if it doesn't exist
output_folder = "output_stats"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

print("Processing files...")
# Find all CSV files starting with 'rangeQTime'
for i_filename in glob.glob('rangeQTime*.csv'):
    print("Processing file: ", i_filename)
    # Read float values from the file
    with open(i_filename, 'r') as file:
        reader = csv.reader(file)
        y_values = [float(i) for i in list(reader)[0]]
    
    # Calculate mean, median, min, max, standard deviation, variance, 95th and 99th percentiles
    stats = calculate_stats(y_values)

    # Create a new output filename with the "stats" modifier and save it in the "output_stats" folder
    o_filename = os.path.join(output_folder, i_filename.replace('.csv', '_stats.csv'))

    # Save the output to the new file
    with open(o_filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Mean', 'Median', 'Min', 'Max', 'Std', 'Var', '95th', '99th'])
        writer.writerow(stats)
print("Making graphs...")
# Find all CSV files with "_stats" in the name
for false_stats_file in glob.glob(os.path.join(output_folder, '*rc_off_False*_stats.csv')):
    true_stats_file = false_stats_file.replace("rc_off_False", "rc_off_True")

    if os.path.exists(true_stats_file):
        # Read statistics from "rc_off_False" file
        headers, false_stats = read_stats(false_stats_file)

        # Read statistics from "rc_off_True" file
        _, true_stats = read_stats(true_stats_file)

        # Set up the bar graph
        x = np.arange(len(headers))
        width = 0.35

        fig, ax = plt.subplots()
        rects1 = ax.bar(x - width / 2, false_stats, width, label='RangeDrivenCompaction')
        rects2 = ax.bar(x + width / 2, true_stats, width, label='Vanilla')

        # Add labels, title and custom x-axis tick labels
        ax.set_ylabel('Time (Seconds)')
        ax.set_title('Comparison of Statistics for RangeDrivenCompaction and Vanilla')
        ax.set_xticks(x)
        ax.set_xticklabels(headers)
        ax.legend()

        # Save the bar graph in the "output_stats" folder
        graph_filename = os.path.basename(false_stats_file).replace('_stats.csv', '_comparison.png')
        plt.savefig(os.path.join(output_folder, graph_filename))
        plt.close(fig)
