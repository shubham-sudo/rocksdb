import sys
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter

# read the filename from the command line arguments
filename = sys.argv[1]

# open the CSV file and read the y-values into a list
with open('rangeQTime.csv', 'r') as file:
    reader = csv.reader(file)
    y_values = list(reader)[0]

# create a list of x values (in this example, we'll just use the index of each value in the list)
x_values = list(range(len(y_values)))

# create a line graph
plt.plot(x_values, y_values)
plt.yticks(plt.yticks()[0][::len(y_values)//5])
plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
# add axis labels and a title
plt.xlabel('X-axis label')
plt.ylabel('Y-axis label')
plt.title('Title of the graph')

# save the graph with a filename based on the command line argument
plt.savefig(filename)
