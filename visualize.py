from os import read
import matplotlib.pyplot as plt
import sys
import csv
import numpy as np
import Debug

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

    processor_list, dag_list, env = Debug.main("testcases/test5.json","answer5.csv", 8)
    makespan = Debug.calc_make_span(processor_list)
    n_cores = len(processor_list)
    schedule = []
    for processor in processor_list:
        schedule.append(processor.execution_history)
    
    dag_types = []
    failing_dag_idx = None
    for i, dag in enumerate(dag_list):
        if dag._type not in dag_types:
            dag_types.append(dag._type)
        if dag._failed:
            failing_dag_idx = i

    n_dags = len(dag_types)
    type_color_map = plt.cm.get_cmap("hsv", n_dags)
    id_color_map = plt.cm.get_cmap("hsv", len(dag_list))
    print(type_color_map(0))
    print(n_dags)
    #schedule, makespan = load_csv(filename)
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
    gnt.set_yticks([dy*i+dy/2 for i in range(n_cores)])
    # Labelling tickes of y-axis
    gnt.set_yticklabels([str(i) for i in range(n_cores)])


    index_to_norm_type = [[dag_types.index(dag_list[dag_list.index(filter(lambda d: entry[0] in map(lambda t: int(t.name[4:]), d.task_list), dag_list).__next__())]._type) for entry in processor.execution_history] for processor in processor_list]
    index_to_norm_id = [[dag_list.index(filter(lambda d: entry[0] in map(lambda t: int(t.name[4:]), d.task_list), dag_list).__next__()) for entry in processor.execution_history] for processor in processor_list]
    

    # Setting graph attribute
    #gnt.grid(True)
    for p_id, processor_execution_history in enumerate(schedule):
        for task_index, (task_id, task_start_time, task_finish_time) in enumerate(processor_execution_history):
            eet = task_finish_time - task_start_time
            dag = dag_list[index_to_norm_id[p_id][task_index]]
            fc = id_color_map(index_to_norm_id[p_id][task_index])
            if dag._failed:
                fc = 'black'
            ec = type_color_map(index_to_norm_type[p_id][task_index])
            
            gnt.broken_barh([(task_start_time, eet)], (p_id*dy, dy-1), facecolor=ec, edgecolor=fc, linewidth=1)
            x_c = task_start_time + eet/4
            y_c = p_id*dy + dy/2
            gnt.text(x_c, y_c, str(task_id))
    
    for i, dag in enumerate(dag_list):
        deadline_line_color = id_color_map(i) 
        if dag._failed: deadline_line_color = 'black'
        t = dag.arrival_time + dag.deadline
        plt.plot([t,t], [-10, ymax+10], color=deadline_line_color, linewidth=2)
    
    for time_stamp in env.time_stamp_history:
        plt.plot([time_stamp, time_stamp], [-20, ymax+20], color='black', linestyle='dashed', alpha=0.5)
    plt.show()
    
