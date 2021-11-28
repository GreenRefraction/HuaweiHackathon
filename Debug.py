import json
import time
import csv
import math
import numpy as np
# from DAG import DAG
# from Processor import Processor, TODO
# from Environment import Environment


class FailedToScheduleException(Exception):
    pass


class Task:
    name = None
    dag = None
    EET = None
    _type = None
    children = None
    parents = None

    is_complete: bool = None

    def __init__(self, name, dag, json_task_data: dict) -> None:
        self.name = name
        self.EET = json_task_data['EET']
        self._type = json_task_data['Type']

        self.children: list[(Task, int)] = list()
        self.parents: list[(Task, int)] = list()  # Needed?

        self.is_complete: bool = False
        self.dag = dag

        self.finish_time = None

    # from the list of parents, find the max eet for this task
    # and that would be the starting time for this task.

    def add_child(self, task, weight):
        self.children.append((task, weight))

    def add_parent(self, task, weight):
        self.parents.append((task, weight))

    def add_dag(self, dag):
        self.dag = dag

    def tick(self):
        self.is_complete = True

    def __str__(self) -> str:
        return json.dumps(str(self.__dict__), default=lambda o: o.name if type(o) == Task else str(o.__dict__))


class DAG:
    name = None

    def __init__(self, name, json_dag_data: dict) -> None:
        self.name = name

        self.task_list: list[Task] = list()
        self.entry_tasks: list[Task] = list()  # Tasks without parents
        self.exit_tasks: list[Task] = list()

        self._type: int = json_dag_data['Type']
        self.arrival_time: int = json_dag_data['ArrivalTime']
        self.deadline: int = json_dag_data['Deadline']

        self.is_complete: bool = False
        self._failed = False

        name_to_task: dict[str, Task] = dict()
        task_names = list(
            filter(lambda s: s[:4] == 'Task', json_dag_data.keys()))
        # Create all tasks
        for task_name in task_names:
            task = Task(task_name, self, json_dag_data[task_name])
            name_to_task[task_name] = task

        # Connect children and parents
        for task_name in task_names:
            children = json_dag_data[task_name]['next']
            for child_name in children.keys():
                ict = children[child_name]
                name_to_task[task_name].add_child(
                    name_to_task[child_name], ict)
                name_to_task[child_name].add_parent(
                    name_to_task[task_name], ict)

        # Find all tasks without parents
        for task_name in task_names:
            task = name_to_task[task_name]
            if not len(task.parents) > 0:
                self.entry_tasks.append(task)
            if not len(task.children) > 0:
                self.exit_tasks.append(task)

        self.task_list = list(name_to_task.values())

    def tick(self) -> None:
        """set is_complete to True if all of the tasks in this dag are complete"""
        for task in self.exit_tasks:
            if not task.is_complete:
                return
        self.is_complete = True

    def __str__(self) -> str:
        return json.dumps(self.__dict__, default=lambda o: str(o) if type(o) == Task else o.__dict__, indent=4)


class TODO:
    def __init__(self, task, min_start_time, pref_p_id):
        self.done = False
        self.task: Task = task
        self.min_start_time: int = min_start_time
        self.finish_time: int = None
        self.pref_p: set[int] = set()
        if pref_p_id is not None:
            self.pref_p.add(pref_p_id)
        self.dag_deadline: int = task.dag.deadline


class Processor:

    current_running_task: Task = None
    is_idle: bool = None
    finish_time_of_running_task: int = None
    utilization_time: int = None

    cache_size = 4
    cache: list[Task] = list()

    # This set contains the answer of a task which is
    # idk what to call this
    answers: set[Task] = None
    # some sort of history of what tasks that were scheduled
    execution_history: list[tuple[int, int, int]] = None

    def __init__(self, id: int) -> None:
        self.id: int = id
        self.cache = list()
        self.utilization_time = 0

        self.answers = set()
        self.is_idle = True

        self.execution_history = list()

    def step(self, t):
        """Take a step to time t"""
        if self.is_idle:
            return
        if self.finish_time_of_running_task <= t:
            # finish the current task
            self.finish()

    def finish(self):
        """Finish the task that is currently running"""
        # add to answers
        self.answers.add(self.current_running_task)

        # append to the cache
        self.cache.append(self.current_running_task)
        if len(self.cache) > self.cache_size:
            self.cache.pop(0)

        # tick the current task
        self.current_running_task.tick()
        # and tick the dag
        self.current_running_task.dag.tick()

        self.current_running_task = None
        self.is_idle = True
        self.finish_time_of_running_task = None

    def start(self, todo: TODO, t) -> bool:
        """attempt to start a new task"""

        if not self.is_idle:
            return False
        # check if the parents of the task is finished

        if todo.min_start_time > t:
            return False
        for (parent, ict) in todo.task.parents:
            if not parent.is_complete:
                return False
        # First check the communication time
        ict_list: list[int] = []
        pay_the_fee: bool = False
        for (parent, ict) in todo.task.parents:
            if parent not in self.answers:  # cleaning might speed up things, it will sure as hell save memory
                # now we need to pay the ict fee
                pay_the_fee = True
                ict_list.append(max(0, parent.finish_time - t + ict))
                # ict_list.append(ict)
            # else:
                # remove the parent from the answers set
                # self.answers.remove(parent)  Not desired because one task can have several childs on the same CPU
        # the else statement will only trigger if the task is an entry task
        # i.e it has not parent, which means we dont pay the fee
        ict = max(ict_list) if len(ict_list) != 0 else 0
        # This needs to be changed to replicate
        # (parentFinishTime + communicationTime * (processors of parent and child are different)

        self.current_running_task = todo.task
        self.is_idle = False

        # if task has the same _type we can reduce the EET by 10%
        eet = todo.task.EET
        if todo.task._type in [cached_task._type for cached_task in self.cache]:
            eet = int(eet * 0.9)  # CHECK? rounding error?
            # print(t, "new instance", eet, todo.task.dag_id)

        self.finish_time_of_running_task = t + eet
        if pay_the_fee:
            self.finish_time_of_running_task += ict
        todo.finish_time = self.finish_time_of_running_task
        todo.task.finish_time = todo.finish_time  # TODO MARK CHECK THING

        # call on the execution history
        task_id = int(todo.task.name[4:])
        self.execution_history.append(
            (task_id, todo.finish_time - eet, todo.finish_time))

        # add the eet to utilization_time
        self.utilization_time += eet + ict if pay_the_fee else eet
        return True


class Environment:

    def __init__(self, dag_list: list[DAG], processor_list: list[Processor]) -> None:
        self.dag_list: list[DAG] = dag_list
        self.dag_arrival: list[DAG] = sorted(
            dag_list, key=lambda d: d.arrival_time)

        self.processing_dag_list: list[DAG] = list()

        self.upcomming_tasks: list[Task] = []
        self.time_stamp: int = 0
        while len(self.dag_arrival) != 0 and self.dag_arrival[0].arrival_time <= self.time_stamp:
            # print(self.dag_arrival[0].arrival_time, self.time_stamp)
            self.upcomming_tasks.extend(
                [TODO(t, self.time_stamp, None) for t in self.dag_arrival[0].entry_tasks])
            self.dag_arrival.pop(0)

        self.processor_list: list[Processor] = processor_list

        self.counter = [0, 0, 0, 0]
        self.time_stamp_history: list[int] = [0]

    def step(self, has_scheduled):
        dt = self.calc_next_time_step(has_scheduled)
        self.time_stamp += dt
        self.time_stamp_history.append(self.time_stamp)

        for processor in self.processor_list:
            processor.step(self.time_stamp)

        # keep the upcomming_tasks list up to date
        while len(self.dag_arrival) != 0 and self.dag_arrival[0].arrival_time <= self.time_stamp:
            # print(self.dag_arrival[0].arrival_time, self.time_stamp)
            self.upcomming_tasks.extend(
                [TODO(t, self.time_stamp, None) for t in self.dag_arrival[0].entry_tasks])
            dag_to_process = self.dag_arrival.pop(0)
            self.processing_dag_list.append(dag_to_process)

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
            if dag.arrival_time + dag.deadline < self.time_stamp:
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

    def calc_next_time_step(self, has_scheduled):
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

def sdf_scheduler(processor_list: list[Processor], upcomming_tasks: list[TODO], t):
    has_scheduled = False
    upcomming_tasks.sort(
        key=lambda todo: todo.task.dag.arrival_time + todo.task.dag.deadline)
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


def heuristic_scheduler(processor_list: list[Processor], upcomming_tasks: list[TODO], t):
    has_scheduled = False
    upcomming_tasks.sort(key=heuristic)

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


def heuristic(todo: TODO):

    return todo.task.dag.arrival_time + todo.task.dag.deadline
    # heuristic(todo) = alpha * (dag.deadline) + beta * todo.EET


def sdf_p_scheduler(processor_list: list[Processor], upcomming_tasks: list[TODO], t) -> None:
    has_scheduled = False
    upcomming_tasks.sort(
        key=lambda todo: todo.task.dag.arrival_time + todo.task.dag.deadline)
    # Start any task that is available

    for todo in upcomming_tasks:
        success = False
        for p_id in todo.pref_p:
            success = processor_list[p_id].start(todo, t)
            has_scheduled = has_scheduled or success
            if success:
                # print(t, p_id, to_sched.task.name)
                pop_task_from_list(todo, upcomming_tasks, t, p_id)
                break
        if success:
            continue
        for p_id in set(range(len(processor_list))).difference(todo.pref_p):
            success = processor_list[p_id].start(todo, t)
            has_scheduled = has_scheduled or success
            if success:
                # print(t, p_id, to_sched.task.name)
                pop_task_from_list(todo, upcomming_tasks, t, p_id)
                break
    return has_scheduled


def rbfs_scheduler(processor_list: list[Processor], upcomming_tasks: list[TODO], t, real_start_time):
    # the goal here is to use a heuristic to evaluate each action that the scheduler is taking
    # maybe combine this with a bfs search, but idk if this is thesible

    # initialize a matrix (n_processors, n_tasks) of heuristic values Q
    #
    # foreach processor:
    # if processor is not idle:
    # Q[processor, a] = -infinity
    # continue
    # foreach task:
    # Q[processor, task] = heuristic(processor, todo)
    # then the optimal action according to Q would be
    # (processor, todo) = argmax(Q)
    # once we've taken that action the Q function would have to be reevaluated
    #

    pass


def pop_task_from_list(task_to_remove: TODO, upcomming_tasks: list[TODO], t: int, p_id: int):
    # before we delete the task[idx] we want to append the children of that task to the upcomming tasks list
    for (child, ict) in task_to_remove.task.children:
        for upc_todo in upcomming_tasks:
            if upc_todo.task == child:
                upc_todo.min_start_time = max(
                    upc_todo.min_start_time, task_to_remove.finish_time)
                upc_todo.pref_p.add(p_id)
                break
        else:
            upcomming_tasks.append(
                TODO(child, task_to_remove.finish_time, p_id))
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
    # N = len(processor_list) - 1
    # mean = 0
    # for processor in processor_list:
    #     pn = processor.utilization_time/end_time
    #     # print(pn)
    #     mean += pn/(N + 1)
    # std = 0
    # for processor in processor_list:
    #     pn = processor.utilization_time/end_time
    #     std += ((pn - mean)**2)/N
    # return math.sqrt(std)
    """
    s² = (sum (xi - x_mean)²)/(n-1)
    """
    ut = [p.utilization_time for p in processor_list]
    norm_ut = list(map(lambda u: u/end_time, ut))
    mean_ut = sum(norm_ut) / len(norm_ut)
    s = sum(map(lambda xi: (xi - mean_ut)**2, norm_ut))
    # s /= len(norm_ut)-1
    s /= len(norm_ut)

    return math.sqrt(s)
    # ut = [p.utilization_time for p in processor_list]
    # norm_ut = list(map(lambda u: u/end_time, ut))
    # return np.std(norm_ut)


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
    print(len(dag_list))

    # something that keeps track of what we've done
    # initialze a empty schedule, the history
    # schedule = Schedule()

    # Initialize the environment
    processor_list = [Processor(i) for i in range(n_processors)]

    env = Environment(dag_list, processor_list)
    try:
        start_time = time.time_ns()
        # Only works when the dags isn't repopulated
        while len(env.dag_arrival) > 0 or len(env.upcomming_tasks) != 0:

            env.step(sdf_scheduler(processor_list,
                                     env.upcomming_tasks,
                                     env.time_stamp))
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


if __name__ == '__main__':

    """dag_list = load_from_json("sample.json")
    execution_history0 = [(0, 0, 10),(3, 43, 53),(1000, 60, 69),(1003, 99, 108)]
    execution_history1 = [(1, 11, 31), (1001, 70, 88)]
    execution_history2 = [(2, 12, 42), (1002, 71, 98)]

    processor_list = [Processor(i) for i in range(3)]
    processor_list[0].execution_history = execution_history0
    processor_list[0].utilization_time = 10+10+9+9
    processor_list[1].execution_history = execution_history1
    processor_list[1].utilization_time = 20+18
    processor_list[2].execution_history = execution_history2
    processor_list[2].utilization_time = 30+27

    make_span = calc_make_span(processor_list)

    worst_case_makespan = worst_case(dag_list)
    pn_std = calc_std_deviation(processor_list, make_span)
    utility = utility_func(make_span, worst_case_makespan, pn_std)
    print(pn_std)
    print(utility)
    quit()"""

    testcases = [f"test{i}.json" for i in range(1, 13)]

    for i, test in enumerate(testcases):
        processor_list, dag_list, _ = main("testcases/"+test,
                                           f"answer{i+1}.csv",
                                           n_processors=8 if i < 6 else 6)
        make_span = calc_make_span(processor_list)
        print(f"Case {i+1}")
        # print([processor.utilization_time/make_span for processor in processor_list])
        # print()
