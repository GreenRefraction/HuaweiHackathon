import json
import time
import csv
import math
from copy import copy, deepcopy

from DAG import DAG
from Task import Task
from Processor import Processor
from Environment import Environment
from State import State, WaitForNewIncommingDAGAction, ScheduleTaskAction, WaitForProcessorToFinishAction


class FailedToScheduleException(Exception):
    pass

def load_from_json(file_name) -> list[DAG]:
    with open(file_name, 'r') as f:
        data = json.load(f)
        dags: list[DAG] = []
        for dag_name in data.keys():
            dags.append(DAG(dag_name, data[dag_name]))

        for dag in dags:
            for task in dag.task_list:
                task.add_dag(dag)
        return dags


# - We if we have finished a task, we want to prioritize its children with the largest ict
# - We want to utilize caching for tasks with large EET, i.e, same type of tasks should be scheduled on the same core within 4 scheduled tasks

def heuristic_scheduler(env:Environment, time):
    idle_processors = list(filter(lambda proc: proc.is_idle, env.processor_list))
    if len(idle_processors) == 0:
        return 
    idle_processor_id_set = set([proc.id for proc in idle_processors])
    env.upcomming_tasks.sort(key=lambda task: heuristic(task, time, idle_processors), reverse=True)
    # print([t.task.name for t in upcomming_tasks])
    # print("Is p idle?", [p.is_idle for p in processor_list])

    for upcomming_task in env.upcomming_tasks.copy():
        # here we only continue if atleast one processor is available
        # but maybe extracting only the available processors will make 
        # the program even faster
        # idle_processors = list(filter(lambda proc: proc.is_idle, processor_list))
        if len(idle_processor_id_set) == 0:
            return 
        # print("trying task", todo.task.name)
        success = False
        
        # Here we create a set of processor ids which should be prioritized according to the tasks
        # largest ict
        p_priority = sorted(list(upcomming_task.pref_p),key=lambda t: t[2] + t[1] - time, reverse=True)
        ict_priority = set([p_id for p_id, _, _ in p_priority]).intersection(idle_processor_id_set)

        # Here we create a set of processor ids which should be prioritized according to the tasks
        # cache hits
        cache_priority = set()
        for p_id, proc in enumerate(env.processor_list):
            if upcomming_task._type in [cached_task._type for cached_task in proc.cache] and p_id in idle_processor_id_set:
                cache_priority.add(p_id)

        if upcomming_task.dag.child_depth < 4: # four is related to the cache size
            success = prio_scheduling(upcomming_task, ict_priority, cache_priority, idle_processor_id_set, env, time)
        else:
            # here we try schedule upcomming_task with priority cache then ict then rest
            success = prio_scheduling(upcomming_task, cache_priority, ict_priority, idle_processor_id_set, env, time)

    return

def prio_scheduling(upcomming_task:Task, set1:set[int], set2:set[int], total_set:set[int], env:Environment, time):
    success = False
    remaining = total_set.difference(set1).difference(set2)
    success, p_id_scheduled = try_schedule_on(upcomming_task, set1.intersection(set2), time, env)
    if success:
        total_set.remove(p_id_scheduled)
        return success
    success, p_id_scheduled = try_schedule_on(upcomming_task, set1.difference(set2), time, env)
    if success:
        total_set.remove(p_id_scheduled)
        return success
    success, p_id_scheduled = try_schedule_on(upcomming_task, set2.difference(set1), time, env)
    if success:
        total_set.remove(p_id_scheduled)
        return success
    success, p_id_scheduled = try_schedule_on(upcomming_task, remaining, time, env)
    if success:
        total_set.remove(p_id_scheduled)
        return success
    return success

def try_schedule_on(upcomming_task:Task, processor_set:set[Processor], time:int, env:Environment):
    success = False
    for p_id in processor_set:
        success = env.processor_list[p_id].start(upcomming_task, time)
        if success:
            pop_task_from_list(upcomming_task, env.upcomming_tasks, time, p_id)
            return success, p_id
    return success, -1

def heuristic(task: Task, time: int, processor_list:list[Processor]):
    # Time until deadline
    h0 = -(task.dag_deadline - time)
    # Max posible communication penalty (ict)
    h1 = 0
    for p_id, ict, ft in task.pref_p:
        h1 = max(h1, ft + ict - time)
    #h1 /= task.max_ict
    # execution time left of the dags longest path from todo.task 
    # h2 = task.norm_effective_depth  # normalized with the deadline of dag
    h2 = task.EET_depth
    
    # h3 should be the time gain if we are able to find a cache hit
    cache_hits = 0
    time_save = int(0.1*task.EET)
    for proc in processor_list:
        cache_hits += int(task._type in [cached_task._type for cached_task in proc.cache])
    h3 = 0
    if task.child_depth < 4:
        # now multiply time_save with the complement of cache_hits
        # giving high priority if there are only few cache hits
        h3 = time_save * (len(processor_list) - cache_hits)
    h = h0 + h1 + h2 + h3
    return h
    # heuristic(todo) = alpha * (dag.deadline) + beta * todo.EET


def pop_task_from_list(task_to_remove: Task, upcomming_tasks: list[Task], t: int, p_id: int):
    # before we delete the task[idx] we want to append the children of that task to the upcomming tasks list
    for (child, ict) in task_to_remove.children:
        for upcomming_task in upcomming_tasks:
            if upcomming_task == child:
                upcomming_task.min_start_time = max(
                    upcomming_task.min_start_time, task_to_remove.finish_time)
                upcomming_task.pref_p.add((p_id, ict, task_to_remove.finish_time))
                break
        else:
            child.min_start_time = task_to_remove.finish_time
            child.pref_p.add((p_id, ict, task_to_remove.finish_time))
            upcomming_tasks.append(child)
    upcomming_tasks.remove(task_to_remove)


def worst_case(dag_list: list[DAG]):
    sum = 0
    for dag in dag_list:
        for task in dag.task_list:
            sum += task.EET
            for _, ict in task.children:
                sum += ict
    return sum


def calc_make_span(processor_list: list[Processor]):
    T_max = 0
    for processor in processor_list:
        last_finish_time = 0
        if len(processor.execution_history) != 0:
            last_finish_time = processor.execution_history[-1][2]
        T_max = max(last_finish_time, T_max)
    return T_max


def utility_func(makespan, worst_case, PN_std):
    return 1 / (10 * (makespan / worst_case) + PN_std)


def calc_std_deviation(processor_list: list[Processor], end_time):
    ut = [p.utilization_time for p in processor_list]
    norm_ut = list(map(lambda u: u/end_time, ut))
    mean_ut = sum(norm_ut) / len(norm_ut)
    s = sum(map(lambda xi: (xi - mean_ut)**2, norm_ut))
    # s /= len(norm_ut)-1
    s /= len(norm_ut)

    return math.sqrt(s)

def sdf_scheduler(processor_list: list[Processor], upcomming_tasks: list[Task], t):
    has_scheduled = False
    upcomming_tasks.sort(
        key=lambda task: task.dag.deadline)
    # upcomming_tasks.sort(key=heuristic(todo))

    # Start any task that is available
    for p_id, processor in enumerate(processor_list):
        # try to schedule the first task
        for todo in upcomming_tasks:
            success = processor.start(todo, t)
            if success:
                has_scheduled = success
                # print(t, p_id, to_sched.task.name)
                pop_task_from_list(todo, upcomming_tasks, t, p_id)
                break
    return has_scheduled

def sdf_scheduler2(current_state:State):
    available_schedule_actions:list[ScheduleTaskAction] = list(filter(lambda a: type(a) == ScheduleTaskAction, current_state.available_actions))
    if len(available_schedule_actions) == 0:
        return current_state.available_actions[0]
    available_schedule_actions.sort(key=lambda a: a.task.dag_deadline)
    return available_schedule_actions[0]

def output_csv(processor_list: list[Processor], dag_list: list[DAG], elapsed_time, filename):
    std_dev = None
    with open(filename, 'w', newline='\n') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        p_count = 1
        makespan = calc_make_span(processor_list)
        for processor in processor_list:
            p_count += 1
            # spamwriter.writerow([p_count, processor.execution_history])
            spamwriter.writerow([" ".join([str(e) for e in entry])
                                 for entry in processor.execution_history])
            std_dev = calc_std_deviation(processor_list, makespan)
        worst_case_val = worst_case(dag_list)
        spamwriter.writerow([makespan])
        spamwriter.writerow([std_dev])
        spamwriter.writerow([utility_func(makespan, worst_case_val, std_dev)])
        spamwriter.writerow([int(elapsed_time)])



def main(input_filename: str, output_filename: str, n_processors: int = 8):
    dag_list: list[DAG] = load_from_json(input_filename)


    # something that keeps track of what we've done
    # initialze a empty schedule, the history
    # schedule = Schedule()

    # Initialize the environment
    processor_list = [Processor(i) for i in range(n_processors)]

    env = Environment(dag_list, processor_list)
    print("Final Deadline:", env.last_deadline)
    print("Largest ICT", env.max_ict)
    try:
        start_time = time.time_ns()
        # Only works when the dags isn't repopulated
        while len(env.dag_arrival) > 0 or len(env.upcomming_tasks) != 0:
            # print("-"*30)
            # print("Env time stamp", env.time_stamp)
            heuristic_scheduler(env, env.time_stamp)
            env.step()
        stop_time = time.time_ns()
        # CHECK? rounding error?
        exec_time_scheduler = (stop_time - start_time)//1e6
        print("Execution time:", exec_time_scheduler)
        output_csv(processor_list, dag_list,
                   exec_time_scheduler, output_filename)
    except FailedToScheduleException:
        # Here we failed the scheduling task
        pass

    return processor_list, dag_list, env


def dfs_search(root:State, current_best_child:State=None) -> State:
    if root.is_terminal:
        if root.is_failed:
            return current_best_child
        else:
            return root
    # If the root is not terminal then continue searching
    for action in root.available_actions:
        child = root.take_action(action)
        terminal_state = dfs_search(child, current_best_child)
        if terminal_state.is_terminal and not terminal_state.is_failed:
            return terminal_state
        if current_best_child is None:
            current_best_child = terminal_state
            break
        elif terminal_state.make_span < current_best_child.make_span:
            current_best_child = terminal_state
    return current_best_child

def iterative_search(root:State):
    stack:list[State] = list()
    stack.append(root)
    terminal_state = None
    while True:
        current = stack[-1]
        # our real time scheduler will determine what decision to be made
        best_action = sdf_scheduler2(current)
        new_state = current.take_action(best_action)
        stack.append(new_state)
        if new_state.is_terminal:
            if current.is_failed:
                # we want to go back one step in the stack
                stack.pop()
            else:
                # We can just break the loop and return this state
                terminal_state = new_state
                break        
    return terminal_state

if __name__ == '__main__':
    dag_list = load_from_json("testcases/test1.json")
    dag_list = sorted(dag_list, key=lambda d: d.arrival_time)[:3]
    processor_list = [Processor(i) for i in range(8)]
    
    root_state = State(dag_list, processor_list, 0)
    start_time = time.time_ns()
    terminal_state = iterative_search(root_state)
    elapsed_time = time.time_ns() - start_time
    elapsed_time /= 1e6
    print(terminal_state.make_span, elapsed_time)
    quit()
   
    testcases = [f"test{i}.json" for i in range(1, 13)]


    for i, test in enumerate(testcases):
        #if i != 11: continue
        print("-"*20)
        processor_list, dag_list, _ = main("testcases/"+test,
                                           f"answer{i+1}.csv",
                                           n_processors=8 if i < 6 else 6)
        make_span = calc_make_span(processor_list)
        print(f"Case {i+1}")
        # print([processor.utilization_time/make_span for processor in processor_list])
        # print()
