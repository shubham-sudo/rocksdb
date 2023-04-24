import sys
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
import numpy as np

# read the filename from the command line arguments
i_filename = sys.argv[1]
o_filename = sys.argv[2]
title = sys.argv[3]
if(len(sys.argv) > 4):
    i2_filename = sys.argv[4]

# open the CSV file and read the y-values into a list
with open(i_filename, 'r') as file:
    reader = csv.reader(file)
    y_values = list(reader)[0]

if(len(sys.argv) > 4):
    with open(i2_filename, 'r') as file:
        reader = csv.reader(file)
        y2_values = list(reader)[0]

# create a list of x values (in this example, we'll just use the index of each value in the list)
x_values = list(range(len(y_values)))
#sorted_data = sorted(data, key=lambda x: x[1])

#print("X Values: ", x_values)
#print(y_values)
new_y_values = [float(i) for i in y_values]
if(len(sys.argv) > 4):
    new_y2_values = [float(i) for i in y2_values]

# create a line graph
plt.plot(x_values, new_y_values)
if(len(sys.argv) > 4):
    plt.plot(x_values, new_y2_values)
    plt.legend(['RangeDrivenCompaction', 'Vanilla'], loc='upper left')
#plt.yticks(plt.yticks()[0][::len(y_values)//5])
#plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
plt.ylim(0, 1)
# add axis labels and a title
plt.xlabel('Query Number')
plt.ylabel('TIme (Seconds)')
plt.title(title)

# save the graph with a filename based on the command line argument
plt.savefig(o_filename)
