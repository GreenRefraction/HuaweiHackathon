import json
import time
import csv
from DAG import DAG
from Task import Task

class TODO:
    def __init__(self, task, min_start_time, pref_p_id):
        self.done = False
        self.task:Task = task
        self.min_start_time:int = min_start_time
        self.finish_time:int = None
        self.pref_p = set()
        if pref_p_id is not None:
            self.pref_p.add(pref_p_id)


class Processor:

    current_running_task:Task = None
    is_idle:bool = None
    finish_time_of_running_task:int = None
    utilization_time:int = None

    cache_size = 4
    cache:list[Task] = list()

    # This set contains the answer of a task which is 
    # idk what to call this
    answers:set[Task] = None
    # some sort of history of what tasks that were scheduled
    execution_history:list[tuple[int, int, int]] = None

    def __init__(self, id:int) -> None:
        self.id:int = id
        self.cache = list()
        self.utilization_time = 0

        self.answers = set()
        self.is_idle = True

        self.execution_history = list()

    def step(self, t, dt):
        """Take a step forward in time for T seconds"""
        if self.is_idle:
            return
        if self.finish_time_of_running_task <= t + dt:
            # append to the cache
            #print(t, self.current_running_task.name, "Finished")
            self.cache.append(self.current_running_task)
            self.current_running_task.is_complete = True
            if len(self.cache) > self.cache_size:
                self.cache.pop(0)
            # finish the current task
            self.current_running_task = None
            self.is_idle = True
            self.finish_time_of_running_task = None

    def start(self, todo:TODO, t) -> bool:
        """attempt to start a new task"""

        if not self.is_idle:
            return False
        # check if the parents of the task is finished
        
        if todo.min_start_time > t:
            return False
        for (parent, ict) in todo.task.parents:
            if parent.is_complete != True:
                return False
        # First check the communication time
        ict_list:list[int] = []
        pay_the_fee:bool = False
        for (parent, ict) in todo.task.parents:
            ict_list.append(ict)
            if parent not in self.answers:
                # now we need to pay the ict fee
                pay_the_fee = True
            else:
                # remove the parent from the answers set
                self.answers.remove(parent)
        # the else statement will only trigger if the task is an entry task
        # i.e it has not parent, which means we dont pay the fee
        ict = max(ict_list) if len(ict_list) != 0 else 0

        self.current_running_task = todo.task
        self.is_idle = False

        # if task has the same _type we can reduce the EET by 10%
        eet = todo.task.EET
        if todo.task._type in [cached_task._type for cached_task in self.cache]:
            eet = int(eet * 0.9) # CHECK? rounding error?
            #print(t, "new instance", eet, todo.task.dag_id)
        
        self.finish_time_of_running_task = t + eet
        if pay_the_fee:
            self.finish_time_of_running_task += ict
        todo.finish_time = self.finish_time_of_running_task

        # call on the execution history
        task_id = int(todo.task.name[4:])
        self.execution_history.append((task_id, todo.finish_time - eet , todo.finish_time))

        # add the eet to utilization_time
        self.utilization_time += eet + ict if pay_the_fee else eet
        return True


class Environment:

    def __init__(self, dag_list:list[DAG], processor_list:list[Processor]) -> None:
        self.dag_list   :list[DAG] = dag_list
        self.dag_arrival:list[DAG] = sorted(dag_list, key = lambda d: d.arrival_time)
        self.upcomming_tasks:list[Task] = [] 
        self.time_stamp:int = 0
        while self.dag_arrival[0].arrival_time <= self.time_stamp:
            self.upcomming_tasks.extend([TODO(t, self.time_stamp, None) for t in self.dag_arrival[0].entry_tasks])
            self.dag_arrival.pop(0)
        
        self.processor_list:list[Processor] = processor_list


    def step(self, dt):
        for processor in self.processor_list:
            processor.step(self.time_stamp, dt)

        # this code reintroduces dags with a certain period
        while len(self.dag_arrival) != 0 and self.dag_arrival[0].arrival_time <= self.time_stamp+dt:
            self.upcomming_tasks.extend([TODO(t, self.time_stamp, None) for t in self.dag_arrival[0].entry_tasks])
            #self.dag_arrival[0].arrival_time += self.dag_arrival[0].deadline
            #self.dag_arrival.append(self.dag_arrival[0])
            self.dag_arrival.pop(0)
            #self.dag_arrival.sort(key = lambda d: d.arrival_time)  # Can be done faster
        self.time_stamp += dt


def load_from_json(file_name) -> list[DAG]:
    with open(file_name, 'r') as f:
        data = json.load(f)
        dags = []
        for dag_name in data.keys():
            dags.append(DAG(dag_name, data[dag_name]))
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

def output_csv(processor_list:list[Processor], elapsed_time, filename):
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
    
        # print(processor.execution_history)
        # f.write(processor.execution_history)
    # print(calc_make_span())
    # print(calc_std_deviation())
    # print(utility_func())
    # print(exec_time_scheduler)
        worst_case_val = worst_case(dag_list)
        spamwriter.writerow([makespan])
        spamwriter.writerow([std_dev])
        spamwriter.writerow([utility_func(makespan, worst_case_val, std_dev)])
        spamwriter.writerow([elapsed_time])
    

if __name__ == '__main__':
    dag_list: list[DAG] = load_from_json('sample.json')
    
    # something that keeps track of what we've done
    # initialze a empty schedule, the history
    # schedule = Schedule()

    # Initialize the environment
    processor_list = [Processor(i) for i in range(3)]  
 
    t = 0
    env = Environment(dag_list, processor_list)
    start_time = time.time_ns()
    while len(env.dag_arrival) > 0 or len(env.upcomming_tasks) != 0:  # Only works when the dags isn't repopulated
        scheduler(processor_list, env.upcomming_tasks, env.time_stamp)
        # take a step in the environment
        env.step(1)
    stop_time = time.time_ns()
    exec_time_scheduler = (stop_time - start_time)//1e6  # CHECK? rounding error?
    print(exec_time_scheduler, stop_time - start_time, stop_time, start_time)
    output_csv(processor_list, exec_time_scheduler, "output_sample.csv")
