from DAG import DAG
from Processor import TODO, Processor
from Task import Task


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


    def step(self):
        dt = self.calc_next_time_step()
        
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
        #print(self.time_stamp)
    def get_next_arrival_time(self) -> int:
        if len(self.dag_arrival) != 0:
            return self.dag_arrival[0].arrival_time
        else:
            return None
    def calc_next_time_step(self):
        # The first scenario is if all the processors are not available
        # i.e they are all busy, we can skip forward to the 
        all_processors_are_busy = True
        min_finish_time = 1e100
        for processor in self.processor_list:
            if processor.is_idle:
                # if one processor is idle, then all processors are not busy
                all_processors_are_busy = False
            else:            
                min_finish_time = min(processor.finish_time_of_running_task, min_finish_time)
        next_arrival_time = self.get_next_arrival_time()

        if all_processors_are_busy:
            # step to the time when the first running task is finished
            dt = min_finish_time - self.time_stamp
            return dt
        elif next_arrival_time is not None:
            # the processors are not busy then we can check the arrival time of upcomming dags
            next_time_step = min(next_arrival_time, min_finish_time)
            dt = next_time_step - self.time_stamp
            return dt
        else:
            # by default we take 1 timestep
            return 1