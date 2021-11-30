from DAG import DAG
from Task import Task
from Processor import Processor


class FailedToScheduleException(Exception):
    pass


class Environment:

    def __init__(self, dag_list: list[DAG], processor_list: list[Processor]) -> None:
        self.dag_list: list[DAG] = dag_list
        self.dag_arrival: list[DAG] = sorted(
            dag_list, key=lambda d: d.arrival_time)

        self.processing_dag_list: list[DAG] = list()

        self.upcomming_tasks: list[Task] = []
        self.time_stamp: int = 0
        self.check_for_arriving_dags()

        self.processor_list: list[Processor] = processor_list

        self.counter = [0, 0, 0, 0]
        self.time_stamp_history: list[int] = [0]
        
        self.last_deadline = 0
        self.max_ict = 0
        for dag in dag_list:
            self.last_deadline = max(self.last_deadline, dag.deadline)
            for task in dag.task_list:
                for (child, ict) in task.children:
                    self.max_ict = max(self.max_ict, ict)
        
        for dag in dag_list:
            for task in dag.task_list:
                task.max_ict = self.max_ict
        


    def check_for_arriving_dags(self) -> None:
        while len(self.dag_arrival) != 0 and self.dag_arrival[0].arrival_time <= self.time_stamp:
            # print(self.dag_arrival[0].arrival_time, self.time_stamp)
            for arriving_task in self.dag_arrival[0].entry_tasks:
                arriving_task.min_start_time = self.time_stamp
                self.upcomming_tasks.append(arriving_task)
                # self.upcomming_tasks.extend(
                #     [TODO(t, self.time_stamp, None) for t in self.dag_arrival[0].entry_tasks])
            dag_to_process = self.dag_arrival.pop(0)
            self.processing_dag_list.append(dag_to_process)

    def step(self):
        dt = self.calc_next_time_step()
        self.time_stamp += dt
        self.time_stamp_history.append(self.time_stamp)

        for processor in self.processor_list:
            processor.step(self.time_stamp)

        # keep the upcomming_tasks list up to date
        self.check_for_arriving_dags()

        # Now we want to check if we fail the task

        # this list contains dags that can be removed from
        dags_to_remove: list[DAG] = list()
        for dag in self.processing_dag_list:
            if dag.is_complete:
                dags_to_remove.append(dag)
        # remove the completed dags from the currently running dags
        for dag in dags_to_remove:
            self.processing_dag_list.remove(dag)

        for dag in self.processing_dag_list:
            if dag.deadline < self.time_stamp:
                print("Failed")
                # print(dag)
                print(self.time_stamp)
                not_comp = list(
                    filter(lambda t: not t.is_complete, dag.task_list))
                print(len(not_comp))
                print("parents", sum([len(t.parents) for t in not_comp]))
                print("children", sum([len(t.children) for t in not_comp]))
                dag._failed = True
                raise FailedToScheduleException()
        # i.e we fail to process a dag before the next instance
        # of itself arrives

        # print(self.time_stamp)

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
                min_finish_time = min(
                    processor.finish_time_of_running_task, min_finish_time)
        next_arrival_time = self.get_next_arrival_time()

        if all_processors_are_busy:
            # step to the time when the first running task is finished
            dt = min_finish_time - self.time_stamp
            self.counter[0] += 1
            return dt
        # if there is something arriving in the future
        elif next_arrival_time is not None:
            # the processors are not busy then we can check the arrival time of upcomming dags
            # jump to the closest dag arival time or task finish time
            next_time_step = min(next_arrival_time, min_finish_time)
            dt = next_time_step - self.time_stamp
            self.counter[1] += 1
            return dt
        # if there is something arriving in the future
        elif next_arrival_time is None:
            # the processors are not busy then we can check the arrival time of upcomming dags
            # jump to the closest dag arival time or task finish time
            next_time_step = min_finish_time
            dt = next_time_step - self.time_stamp
            self.counter[2] += 1
            return dt
        else:
            # by default we take 1 timestep
            # print(len(self.upcomming_tasks), self.time_stamp)
            print("If we shouldn't arive here")
            self.counter[3] += 1
            return 1

