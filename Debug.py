import json
from DAG import DAG
from Task import Task

def load_from_json(file_name) -> list[DAG]:
    with open(file_name, 'r') as f:
        data = json.load(f)
        dags = []
        for dag_name in data.keys():
            dags.append(DAG(dag_name, data[dag_name]))
        return dags

class Schedule:
    pass

class TODO:
    def __init__(self, task, min_start_time, pref_p):
        self.done = False
        self.task = task
        self.min_start_time = min_start_time
        self.pref_p = pref_p

class Processor:

    current_running_task:TODO = None
    is_idle:bool = None
    finish_time_of_running_task:int = None

    cache_size = 4
    cache:list[Task] = None
    # some sort of history of what tasks that were scheduled
    def __init__(self, id:int) -> None:
        self.id:int = id
        self.cache = list()

    def step(self, t, dt):
        """Take a step forward in time for T seconds"""
        if self.is_idle:
            return
        if self.finish_time_of_running_task < t + dt:
            # finish the current task
            self.current_running_task = None
            self.is_idle = True
            self.finish_time_of_running_task = None
            # append to the cache
            self.cache.append(self.current_running_task)
            if len(self.cache) > self.cache_size:
                del self.cache[0]

    def start(self, task:Task, t) -> bool:
        """Start a new task"""

        # First check the communication time
        # if task.parents
            # ict = 0
        # else
            # ict = parent.children[task]


        self.current_running_task = task
        self.is_idle = False

        # if task has the same _type we can reduce the EET by 10%
        eet = task.EET
        if task._type in [cached_task._type for cached_task in self.cache]:
            eet = int(eet * 0.9)
        
        self.finish_time_of_running_task = t + eet

def scheduler(processor_list:list[Processor], upcomming_tasks:list[TODO], t) -> None:
    # Start any task that is available
    for processor in processor_list:
        # try to schedule the first task 
        success = processor.start(upcomming_tasks[0], t)
        if success: 
            pop_task_from_list(0, upcomming_tasks)

def pop_task_from_list(idx, upcomming_tasks:list[Task]):
    # before we delete the task[idx] we want to append the children of that task to the upcomming tasks list
    for (child, ict) in upcomming_tasks[idx].children:
        upcomming_tasks.append(child)
    del upcomming_tasks[idx]

class Environment:

    def __init__(self, dag_list, processor_list) -> None:
        self.dag_list   :list[DAG] = dag_list
        self.dag_arrival:list[DAG] = sorted(dag_list, key = lambda d: d.arrival_time)
        self.upcomming_tasks:list[Task] = []
        
        self.processor_list:list[Processor] = processor_list

        self.time_stamp:int = 0

    def step(self, dt):
        for processor in self.processor_list:
            processor.step(self.time_stamp, dt)

        # this code reintroduces dags with a certain period
        while self.dag_arrival[0].arrival_time <= self.time_stamp:
            self.upcomming_tasks.extend(self.dag_arrival[0].entry_tasks)
            #self.dag_arrival[0].arrival_time += self.dag_arrival[0].deadline
            #self.dag_arrival.append(self.dag_arrival[0])
            self.dag_arrival.remove(0)
            #self.dag_arrival.sort(key = lambda d: d.arrival_time)  # Can be done faster


if __name__ == '__main__':
    dag_list: list[DAG] = load_from_json('sample.json')
    
    # something that keeps track of what we've done
    # initialze a empty schedule, the history
    # schedule = Schedule()

    # Initialize the environment
    processor_list = [Processor(i) for i in range(3)]    
    env = Environment(dag_list, processor_list)

    while True:
        scheduler(processor_list, env.upcomming_tasks)        
        # take a step in the environment
        env.step(1)
    # parse to csv
