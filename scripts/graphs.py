import csv
from pathlib import Path
from typing import Generator
from matplotlib import pyplot as plt
import numpy as np


RESULTS_DIR = Path(__file__).parent.joinpath("graphs")


def __save_figure(filename, foldername: str):
    """Saves the current figure to the given file name"""

    if not RESULTS_DIR.exists():
        RESULTS_DIR.mkdir()
    
    foldername = foldername.strip("_").split("_rc_off")[0]

    filename = RESULTS_DIR.joinpath(foldername).joinpath("graph_" + filename.name)

    if not filename.parent.exists():
        filename.parent.mkdir(parents=True)

    plt.savefig(filename)


def __read_file(file_path: Path) -> Generator:
    """Reads a CSV file and return a generator of the values"""

    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist")

    val_generator = None
    
    with open(file_path, "r") as file:
        reader = csv.reader(file)
        val_generator = map(float, list(reader)[0])

    return val_generator


def __calculate_statistics(values) -> tuple:
    """Calculates the mean, median, min, max, standard deviation, 
    variance, 95th and 99th percentiles of the given values"""

    mean = np.mean(values)
    median = np.median(values)
    minimum = np.min(values)
    maximum = np.max(values)
    stddev = np.std(values)
    variance = np.var(values)
    _95th_percent = np.percentile(values, 95)
    _99th_percent = np.percentile(values, 99)

    return (mean, median, minimum, maximum, stddev, variance, _95th_percent, _99th_percent)


def range_queries_individual_line_plot(file_path: Path, foldername: str):
    """Plot individual graph for RangeQueries"""

    y_val_generator = __read_file(file_path)
    y_values = list(y_val_generator)
    x_values = list(range(len(y_values)))
    plt.plot(x_values, y_values)
    plt.xlabel("Query Number")
    plt.ylabel("Time (Seconds)")
    plt.title(file_path.stem)
    __save_figure(file_path.with_suffix(".png"), foldername)
    plt.clf()


def range_queries_comparison_line_plot(file_path: Path, vanilla_file_path: Path, foldername: str):
    """Plot comparison graph for RangeQueries"""

    y_val_generator = __read_file(file_path)
    y_val_generator2 = __read_file(vanilla_file_path)
    
    y_values = list(y_val_generator)
    y_values2 = list(y_val_generator2)
    x_values = list(range(len(y_values)))
    plt.plot(x_values, y_values)
    plt.plot(x_values, y_values2)
    plt.legend(["Query Driven Compaction", "Vanilla"], loc="upper right")
    plt.xlabel("Query Number")
    plt.ylabel("Time (Seconds)")
    plt.title(file_path.stem + " v/s rc_off")

    parent_folder = file_path.parent
    filename = "comparison_" + file_path.stem + "_off" + ".png"
    new_path = parent_folder.joinpath(filename)

    __save_figure(new_path, foldername)
    plt.clf()


def range_queries_comparison_histogram(file_path: Path, vanilla_file_path: Path, foldername: str):
    """Plot comparison histogram for RangeQueries"""

    y_val_generator = __read_file(file_path)
    y_val_generator2 = __read_file(vanilla_file_path)
    
    stats1 = __calculate_statistics(list(y_val_generator))
    stats2 = __calculate_statistics(list(y_val_generator2))

    labels = ["mean", "median", "min", "max", "stddev", "variance", "95th", "99th"]
    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, stats1, width, label="Query Driven Compaction")
    rects2 = ax.bar(x + width/2, stats2, width, label="Vanilla")

    ax.set_ylabel("Time (Seconds)")
    ax.set_title("Comparison of Range Queries")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    parent_folder = file_path.parent
    filename = "histogram_" + file_path.stem + "_off" + ".png"
    new_path = parent_folder.joinpath(filename)

    __save_figure(new_path, foldername)
    plt.clf()
