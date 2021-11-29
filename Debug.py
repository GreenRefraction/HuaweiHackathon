import json
import time
import csv
import math
from copy import copy, deepcopy
from types import new_class

# from DAG import DAG
# from Processor import Processor, TODO
# from Environment import Environment


class FailedToScheduleException(Exception):
    pass


class Task:
    def __init__(self, name, dag, json_task_data: dict) -> None:
        self.name = name
        self.EET = json_task_data['EET']
        self._type = json_task_data['Type']

        self.children: list[(Task, int)] = list()
        self.parents: list[(Task, int)] = list()
        self.n_children:int = 0

        self.is_complete: bool = False
        self.dag = dag

        self.finish_time = None
        self.is_complete: bool = None

        # attributes about the depth from self
        self.EET_depth: int = None
        self.norm_EET_depth: int = None
        self.child_depth = None

        # attributes from TODO class
        self.min_start_time: int = None
        self.finish_time: int = None
        # p_id ict finish_time
        self.pref_p: set[tuple[int, int, int]] = set()
        self.dag_deadline: int = self.dag.deadline
        self.dag_period: int = self.dag.period
        self.max_ict = None


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

    def calc_effective_depth(self):
        if self.EET_depth is not None:
            return self.EET_depth
            
        self.EET_depth = self.EET
        
        max_child_depth = 0
        for (c, _) in self.children:
            max_child_depth = max(max_child_depth, c.calc_effective_depth())

        self.EET_depth += max_child_depth
        self.norm_EET_depth = self.EET_depth / self.dag_deadline

        return self.EET_depth

    def calc_child_depth(self):
        if self.child_depth is not None:
            return self.child_depth
        
        max_depth = 0
        for (c, _) in self.children:
            max_depth = max(max_depth, c.calc_child_depth())
        
        self.child_depth = max_depth + 1
        return self.child_depth

    def __str__(self) -> str:
        return json.dumps(str(self.__dict__), default=lambda o: o.name if type(o) == Task else str(o.__dict__))


class DAG:
    def __init__(self, name, json_dag_data: dict) -> None:
        self.name = name

        self.task_list: list[Task] = list()
        self.entry_tasks: list[Task] = list()  # Tasks without parents
        self.exit_tasks: list[Task] = list()

        self._type: int = json_dag_data['Type']
        self.arrival_time: int = json_dag_data['ArrivalTime']
        self.period: int = json_dag_data['Deadline']
        self.deadline: int = self.period + self.arrival_time

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
        
        # Calculate task depth and child depth
        self.EET_depth: int = 0
        self.child_depth: int = 0
        for task in self.entry_tasks:
            task.n_children = len(task.children)
            task.calc_effective_depth()
            task.calc_child_depth()
            self.EET_depth = max(task.EET_depth, self.EET_depth)
            self.child_depth = max(task.child_depth, self.child_depth)
        self.task_list = list(name_to_task.values())

    def tick(self) -> None:
        """set is_complete to True if all of the tasks in this dag are complete"""
        for task in self.exit_tasks:
            if not task.is_complete:
                return
        self.is_complete = True

    def __str__(self) -> str:
        return json.dumps(self.__dict__, default=lambda o: str(o) if type(o) == Task else o.__dict__, indent=4)


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

    def can_start(self, task:Task, t) -> bool:

        if not self.is_idle:
            return False
        # check if the parents of the task is finished

        # if todo.task.name == "Task5023":
        #     print("min start time 5023", todo.min_start_time)

        if task.min_start_time > t:
            return False
        for (parent, ict) in task.parents:
            # if todo.task.name == "Task5023":
            #     print(f"parrent {parent.name} to 5023 done:", parent.is_complete)
            if not parent.is_complete:
                return False
        return True

    def start(self, task: Task, t) -> bool:
        """attempt to start a new task"""
        if not self.can_start(task, t):
            return False
        # First check the communication time
        ict_list: list[int] = []
        pay_the_fee: bool = False
        for (parent, ict) in task.parents:
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

        self.current_running_task = task
        self.is_idle = False

        # if task has the same _type we can reduce the EET by 10%
        eet = task.EET
        if task._type in [cached_task._type for cached_task in self.cache]:
            eet = int(eet * 0.9)  # CHECK? rounding error?
            # print(t, "new instance", eet, todo.task.dag_id)

        self.finish_time_of_running_task = t + eet
        if pay_the_fee:
            self.finish_time_of_running_task += ict
        task.finish_time = self.finish_time_of_running_task

        # call on the execution history
        task_id = int(task.name[4:])
        self.execution_history.append(
            (task_id, task.finish_time - eet, task.finish_time))

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


class Action:
    # A which contains the specifics of an action
    def __init__(self, dt) -> None:
        self.dt = dt
        pass

class WaitForProcessorToFinishAction(Action):
    def __init__(self, processor:Processor, time_stamp) -> None:
        self.processor = processor
        self.finish_time = processor.finish_time_of_running_task
        dt = self.finish_time - time_stamp
        super(WaitForProcessorToFinishAction, self).__init__(dt)
    
    def __str__(self) -> str:
        return f"Wait for processor: {self.processor.id} which finishes in {self.dt}"

class ScheduleTaskAction(Action):
    def __init__(self, task:Task, processor_id:int, time_stamp:int) -> None:
        self.processor_id = processor_id
        self.task = task
        super(ScheduleTaskAction, self).__init__(0)
    def __str__(self) -> str:
        return f"Schedule task: {str(self.task.name)} on processor: {self.processor_id}"

class NoArrivingDAGsException(Exception):
    pass

class WaitForNewIncommingDAGAction(Action):
    def __init__(self, incomming_dags:list[DAG], time_stamp:int) -> None:

        self.soonest_arrival_time = 1e100
        for dag in incomming_dags:
            self.soonest_arrival_time = min(self.soonest_arrival_time, dag.arrival_time)
        if len(incomming_dags) == 0:
            raise NoArrivingDAGsException()
        dt = self.soonest_arrival_time - time_stamp
        super(WaitForNewIncommingDAGAction, self).__init__(dt)
    
    def __str__(self) -> str:
        return f"Wait until {self.soonest_arrival_time}"

class State:
    def __init__(self, dag_list:list[DAG], processor_list:list[Processor], time_stamp) -> None:
        # The state contains information about the current state at time t
        # it tells you which processors are running what task and which 
        # tasks are in the buffer. Assume that this constructor is only called once
        # and its for the root node. otherwise when creating children we utilize deepcopy

        # A mapping of action to state along with a value of each action
        self.children: dict[Action, State] = None
        self.action_value: dict[Action, float] = dict()
        
        # The time
        self.time_stamp = time_stamp
        # A list of buffering Dags
        self.processing_dags: list[DAG] = list()
        self.dag_list_sorted = sorted(dag_list, key=lambda dag: dag.arrival_time)
        self.incomming_dags: list[DAG] = sorted(dag_list, key=lambda dag: dag.arrival_time)
        # A list of buffering Tasks
        self.buffering_tasks: list[Task] = list()
        self.check_for_arriving_dags()

        # A list of all available Actions
        self.available_actions: list[Action] = None
        # A list of all processors
        self.processors: list[Processor] = processor_list

        # make_span is the true value, the accompanied boolean indicates that its
        # a terminal state
        self.make_span = None
        self.is_terminal = False
        # heuristic is a value which tells you how valuable it is to be in 
        # this state
        self.heuristic = None

    def explore_available_actions(self):
        if self.available_actions is not None:
            return
        # append all the incomming dags and upcomming tasks in their respective containers
        self.check_for_arriving_dags()
        # We now have a list of currently processing dags
        # Now we want to evaluate all the available actions
        # Which means we can either wait until a processor finshes a task
        running_processors = list(filter(lambda p: not p.is_idle, self.processors))
        idle_processors = list(filter(lambda p: p.is_idle, self.processors))
        # with idle_processors we can get a list of all actions of type ScheduleTaskAction
        self.available_actions: list[Action] = list()
        for processor in idle_processors:
            for task in self.buffering_tasks:
                schedule_task_action = ScheduleTaskAction(task, processor.id, self.time_stamp)
                self.available_actions.append(schedule_task_action)

        # With all the running processors we can evaluate all actions
        # of type WaitForProcessorAction
        for processor in running_processors:
            wait_for_processor_action = WaitForProcessorToFinishAction(processor, self.time_stamp)
            self.available_actions.append(wait_for_processor_action)

        # Now calculate all the actions of type WaitForNewIncommingDAGAction
        if len(self.dag_list_sorted) !=  0:
            wait_for_dag_action = WaitForNewIncommingDAGAction(self.dag_list_sorted, self.time_stamp)
            if wait_for_dag_action.dt != 0:
                self.available_actions.append(wait_for_dag_action)

    def explore_new_children(self):
        if self.children is not None or self.is_terminal:
            return

        # This function adds all the available new states that we can transition
        # to for every available action
        self.children:dict[Action, State] = dict()
        for action in self.available_actions:
            #print(action)
            new_state = self.calc_new_child(action)
            #print(new_state)
            new_state.heuristic = state_heuristic(new_state, action)
            self.children[action] = new_state

    def check_for_arriving_dags(self) -> None:
        while len(self.incomming_dags) != 0 and self.incomming_dags[0].arrival_time <= self.time_stamp:
            for arriving_task in self.incomming_dags[0].entry_tasks:
                arriving_task.min_start_time = self.time_stamp
                self.buffering_tasks.append(arriving_task)
            dag_to_process = self.incomming_dags.pop(0)
            self.processing_dags.append(dag_to_process)
    
    def copy(self):
        new_state = State(self.dag_list_sorted, deepcopy(processor_list), self.time_stamp)
        new_state.buffering_tasks = copy(self.buffering_tasks)
        for id, processor in enumerate(new_state.processors):
            processor.current_running_task = self.processors[id].current_running_task
            processor.answers = self.processors[id].answers.copy()
        return new_state

    def calc_new_child(self, action:Action):
        # Return a new child from self by taking the Action; action
        new_state:State = None
        if type(action) == WaitForProcessorToFinishAction or type(action) == WaitForNewIncommingDAGAction:
            new_state = self.copy()
            new_state.available_actions = None
            new_state.children = None
            new_state.time_stamp += action.dt
            for processor in new_state.processors:
                processor.step(new_state.time_stamp)
            
            
        elif type(action) == ScheduleTaskAction:
            action:ScheduleTaskAction = action
            new_state = self.copy()
            new_state.available_actions = None
            new_state.children = None
            new_state.check_for_arriving_dags()
            success = new_state.processors[action.processor_id].start(action.task, new_state.time_stamp)
            pop_task_from_list(action.task, new_state.buffering_tasks, new_state.time_stamp, action.processor_id)
            
        else:
            # Default, dunno what to do here
            raise Exception("ABORT")
       
        # keep the upcomming_tasks list up to date
        new_state.check_for_arriving_dags()

        # this list contains dags that can be removed from
        dags_to_remove: list[DAG] = list()
        for dag in new_state.processing_dags:
            if dag.is_complete:
                dags_to_remove.append(dag)
        # remove the completed dags from the currently running dags
        for dag in dags_to_remove:
            new_state.processing_dags.remove(dag)

        # Now we want to check if we fail the problem, i.e we fail 
        # to process a dag before the next instance of itself arrives
        for dag in new_state.processing_dags:
            if dag.deadline < self.time_stamp:
                print("Failed")
                # print(dag)
                print(new_state.time_stamp)
                not_comp = list(
                    filter(lambda t: not t.is_complete, dag.task_list))
                print(len(not_comp))
                print("parents", sum([len(t.parents) for t in not_comp]))
                print("children", sum([len(t.children) for t in not_comp]))
                dag._failed = True
                raise FailedToScheduleException()
        # Now we want to check if new_state is a terminal state and if
        # it is then set new_state.make_span = calc_make_span(new_state)

        if len(self.processing_dags) == 0 and len(self.incomming_dags) == 0:
            new_state.make_span = calc_make_span(new_state.processors)
            new_state.is_terminal = True
        return new_state
    
    def __str__(self) -> str:
        id_list = list()
        task_list = list()
        for processor in self.processors:
            if not processor.is_idle:
                id_list.append(processor.id)
                task_list.append(processor.current_running_task.name)
        return f"time: {self.time_stamp}, running processors:\n{str(id_list)}\n{str(task_list)}\n"

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

def state_heuristic(state:State, action:Action):
    """Evaluates a heuristic value of the state and the action which
    lead to that state"""
    soonest_deadline = 1e100

    for dag in state.processing_dags:
        for task in dag.task_list:
            soonest_deadline = min(soonest_deadline, task.dag_deadline)
    
    return -soonest_deadline

def dfs_search(root:State) -> State:
    if root.is_terminal:
        return root
    print('-'*40)
    print(root)
    
    # If the root is not terminal then continue searching
    min_make_span = 1e100
    min_child = None
    root.explore_available_actions()
    root.explore_new_children()
    for action, child in root.children.items():
        print(action)
        child.explore_available_actions()
        child.explore_new_children()
        print("available actions", len(child.available_actions))
        print(len(child.buffering_tasks))
        input()
        terminal_state = dfs_search(child)
        if terminal_state.make_span < min_make_span:
            min_child = terminal_state
            min_make_span = terminal_state.make_span
    return min_child

if __name__ == '__main__':
    dag_list = load_from_json("sample.json")
    processor_list = [Processor(i) for i in range(3)]
    #dag_list = sorted(dag_list, key=lambda d: d.arrival_time)
    root_state = State(dag_list, processor_list, 0)
    root_state.explore_available_actions()
    root_state.explore_new_children()
    print('-'*40)
    print(root_state)
    print("available actions", len(root_state.available_actions))
    print("buffer size:", len(root_state.buffering_tasks))
    action0 = root_state.available_actions[0]
    child0 = root_state.children[action0]
    child0.explore_available_actions()
    child0.explore_new_children()
    print('-'*40)
    print(child0)
    print("available actions", len(child0.available_actions))
    print("buffer size:", len(child0.buffering_tasks))
    for action in child0.available_actions:
        print(action)
    

    action00 = child0.available_actions[0]
    print(action00)
    print('-'*40)
    child00 = child0.children[action00]
    #print(child00)
    child00.explore_available_actions()
    child00.explore_new_children()
    print(child00)
    print("available actions", len(child00.available_actions))
    for action in child00.available_actions:
        print(action)
    print("buffer size:", len(child00.buffering_tasks))
    print(child00.buffering_tasks)

    quit()
    terminal_state = dfs_search(root_state)

    quit()
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
        #if i != 11: continue
        print("-"*20)
        processor_list, dag_list, _ = main("testcases/"+test,
                                           f"answer{i+1}.csv",
                                           n_processors=8 if i < 6 else 6)
        make_span = calc_make_span(processor_list)
        print(f"Case {i+1}")
        # print([processor.utilization_time/make_span for processor in processor_list])
        # print()
