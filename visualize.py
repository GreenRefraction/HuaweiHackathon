from os import read
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
                task_start_time = int(event[1])/1000
                task_finish_time = int(event[2])/1000
                make_span = max(make_span, task_finish_time)
                schedule[n_cores - 1].append((task_id, task_start_time, task_finish_time))
    return schedule, make_span

if __name__ == '__main__':
    filename = "output_sample.csv"
    schedule, makespan = load_csv(filename)
    n_cores = len(schedule)
    # Declaring a figure "gnt"
    fig, gnt = plt.subplots()

    # Setting Y-axis limits
    ymax = 48
    gnt.set_ylim(0, ymax)
    print(makespan)

    # Setting X-axis limits
    gnt.set_xlim(0, makespan)

    # Setting labels for x-axis and y-axis
    gnt.set_xlabel('seconds since start')
    gnt.set_ylabel('Processor')

    # Setting ticks on y-axis
    dy = ymax/n_cores
    gnt.set_yticks([dy*i+dy/2 for i in range(n_cores)])
    # Labelling tickes of y-axis
    gnt.set_yticklabels([str(i) for i in range(n_cores)])

    # Setting graph attribute
    gnt.grid(True)
    for i, processor_event_list in enumerate(schedule):
        for (task_id, task_start_time, task_finish_time) in processor_event_list:
            eet = task_finish_time - task_start_time
            gnt.broken_barh([(task_start_time, eet)], (i*dy, dy-1))
            x_c = task_start_time + eet/4
            y_c = i*dy + dy/2
            gnt.text(x_c, y_c, str(task_id))
    plt.show()
    
