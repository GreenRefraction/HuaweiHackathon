import matplotlib.pyplot as plt
import sys
import csv
import numpy as np

def load_csv(filename):
    with open(filename, 'r') as handle:
        reader = csv.reader(handle, delimiter=',', quotechar='|')
        n_cores = 0

        schedule = []
        make_span = 0
        for row in reader:
            if len(row[0].split(' ')) != 3:
                break
            n_cores += 1
            schedule.append([])
            for event in row:
                if event == "": break
                event = event.split(' ')
                task_id = int(event[0])
                task_start_time = int(event[1])
                task_finish_time = int(event[2])
                make_span = max(make_span, task_finish_time)
                schedule[n_cores - 1].append((task_id, task_start_time, task_finish_time))
    return schedule, make_span

if __name__ == '__main__':
    filename = sys.argv[1]
    schedule, makespan = load_csv(filename)
    n_cores = len(schedule)
    # Declaring a figure "gnt"
    fig, gnt = plt.subplots()

    # Setting Y-axis limits
    ymax = 48
    gnt.set_ylim(0, ymax)

    # Setting X-axis limits
    gnt.set_xlim(0, makespan)

    # Setting labels for x-axis and y-axis
    gnt.set_xlabel('seconds since start')
    gnt.set_ylabel('Processor')

    # Setting ticks on y-axis
    dy = ymax/n_cores
    gnt.set_yticks([15, 25, 35])
    # Labelling tickes of y-axis
    gnt.set_yticklabels(['1', '2', '3'])

    # Setting graph attribute
    gnt.grid(True)

    # Declaring a bar in schedule
    gnt.broken_barh([(40, 50)], (30, 10), facecolors =('tab:orange'))

    # Declaring multiple bars in at same level and same width
    gnt.broken_barh([(110, 10), (150, 10)], (10, 10),
                            facecolors ='tab:blue')

    gnt.broken_barh([(10, 50), (100, 20), (130, 10)], (20, 10),
                                        facecolors =('tab:red'))

    plt.show()