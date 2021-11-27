import json
import time
import csv
from DAG import DAG
from Task import Task
from Processor import Processor, TODO
from Environment import Environment



def load_from_json(file_name) -> list[DAG]:
    with open(file_name, 'r') as f:
        data = json.load(f)
        dags:list[DAG] = []
        for dag_name in data.keys():
            dags.append(DAG(dag_name, data[dag_name]))
        
        for dag in dags:
            for task in dag.task_list:
                task.add_dag(dag)
        return dags

def scheduler(processor_list:list[Processor], upcomming_tasks:list[TODO], t) -> None:
    # Start any task that is available
    for p_id, processor in enumerate(processor_list):
        # try to schedule the first task 
        if len(upcomming_tasks) != 0:
            to_sched = upcomming_tasks[0]
        else:
            return
        success = processor.start(to_sched, t)
        if success:
            #print(t, p_id, to_sched.task.name)
            pop_task_from_list(to_sched, upcomming_tasks, t, p_id)

def pop_task_from_list(task_to_remove:TODO, upcomming_tasks:list[TODO], t:int, p_id:int):
    # before we delete the task[idx] we want to append the children of that task to the upcomming tasks list
    for (child, ict) in task_to_remove.task.children:
        for upc_todo in upcomming_tasks:
            if upc_todo.task == child:
                upc_todo.min_start_time = max(upc_todo.min_start_time, task_to_remove.finish_time)
                upc_todo.pref_p.add(p_id)
                break
        else:
            upcomming_tasks.append(TODO(child, task_to_remove.finish_time, p_id))
    upcomming_tasks.remove(task_to_remove)

def worst_case(dag_list):
    sum = 0
    for dag in dag_list:
        for task in dag.task_list:
            sum += task.EET
            for _, ict in task.children:
                sum += ict
    return sum

def calc_make_span(processor_list:list[Processor]):
    T_max = 0
    for processor in processor_list:
        last_event = processor.execution_history[-1] if len(processor.execution_history) > 0 else (0,0,0)
        T_max = max(last_event[2], T_max)
    return T_max

def utility_func(makespan, worst_case, PN_std):
    return 1 / (10 * (makespan / worst_case) + PN_std)

def calc_std_deviation(processor_list:list[Processor], end_time):
    PN_list = []
    for processor in processor_list:
        PN_list.append(processor.utilization_time/end_time)
    N = len(processor_list)
    PN_mean = sum(PN_list)/N
    PN_std = 0
    for PN_utilization in PN_list:
        PN_std += PN_utilization**2 / N
    PN_std -= PN_mean**2
    return PN_std

def output_csv(processor_list:list[Processor], dag_list:list[DAG], elapsed_time, filename):
    std_dev = None
    with open(filename, 'w', newline='\n') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        p_count = 1
        makespan = calc_make_span(processor_list)
        for processor in processor_list:
            p_count += 1
            # spamwriter.writerow([p_count, processor.execution_history])
            spamwriter.writerow([" ".join([str(e) for e in entry]) for entry in processor.execution_history])
            std_dev = calc_std_deviation(processor_list, makespan)
        worst_case_val = worst_case(dag_list)
        spamwriter.writerow([makespan])
        spamwriter.writerow([std_dev])
        spamwriter.writerow([utility_func(makespan, worst_case_val, std_dev)])
        spamwriter.writerow([elapsed_time])
    

def main():
    dag_list: list[DAG] = load_from_json('testcases/test1.json')
    print(len(dag_list))
    dag_list = dag_list[:33]
    #dag_list: list[DAG] = load_from_json('sample.json')
    
    # something that keeps track of what we've done
    # initialze a empty schedule, the history
    # schedule = Schedule()

    # Initialize the environment
    processor_list = [Processor(i) for i in range(8)]  
 
    env = Environment(dag_list, processor_list)

    start_time = time.time_ns()
    
    while len(env.dag_arrival) > 0 or len(env.upcomming_tasks) != 0:  # Only works when the dags isn't repopulated
        scheduler(processor_list, env.upcomming_tasks, env.time_stamp)
        env.step()

    stop_time = time.time_ns()
    exec_time_scheduler = (stop_time - start_time)//1e6  # CHECK? rounding error?
    print("Execution time:", exec_time_scheduler)
    output_csv(processor_list, dag_list, exec_time_scheduler, "output_sample.csv")

    return processor_list, dag_list


if __name__ == '__main__':
    main()