from DAG import DAG
from Task import Task
from Processor import Processor
from copy import copy, deepcopy

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
        return f"Wait for processor: {self.processor.id} which finishes in {self.dt} with task {self.processor.current_running_task.name}"

class ScheduleTaskAction(Action):
    def __init__(self, task:Task, processor_id:int, time_stamp:int) -> None:
        self.processor_id = processor_id
        self.task:Task = task
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


class FailedToScheduleException(Exception):
    pass

class State:
    # The state contains information about the current state at time t
    # it tells you which processors are running what task and which 
    # tasks are in the buffer. Assume that the constructor is only called once
    # and its for the root node. otherwise when creating children we utilize deepcopy
    def __init__(self, dag_list_sorted:list[DAG], processor_list:list[Processor], time_stamp) -> None:
        # A mapping of action to state along with a value of each action
        self.children: dict[Action, State] = dict()
        self.action_value: dict[Action, float] = dict()
        self.parent: State = None
        
        # The time
        self.time_stamp = time_stamp
        # A list of buffering DAGs
        self.processing_dags: list[DAG] = list()
        self.incomming_dags: list[DAG] = dag_list_sorted
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
        self.explore_available_actions()

    def explore_available_actions(self):
        if self.available_actions is not None:
            return
        # We now have a list of currently processing dags
        # Now we want to evaluate all the available actions
        # Which means we can either wait until a processor finshes a task
        running_processors = list(filter(lambda p: not p.is_idle, self.processors))
        idle_processors = list(filter(lambda p: p.is_idle, self.processors))
        # with idle_processors we can get a list of all actions of type ScheduleTaskAction
        self.available_actions: list[Action] = list()
        for processor in idle_processors:
            for todo_task in self.buffering_tasks:
                can_start = processor.can_start(todo_task, self.time_stamp)
                if can_start:
                    schedule_task_action = ScheduleTaskAction(todo_task, processor.id, self.time_stamp)
                    self.available_actions.append(schedule_task_action)

        # With all the running processors we can evaluate all actions
        # of type WaitForProcessorAction
        for processor in running_processors:
            wait_for_processor_action = WaitForProcessorToFinishAction(processor, self.time_stamp)
            self.available_actions.append(wait_for_processor_action)

        # Now calculate all the actions of type WaitForNewIncommingDAGAction
        if len(self.incomming_dags) !=  0:
            wait_for_dag_action = WaitForNewIncommingDAGAction(self.incomming_dags, self.time_stamp)
            if wait_for_dag_action.dt > 0 and len(self.available_actions) == 0:
                self.available_actions.append(wait_for_dag_action)

    def check_for_arriving_dags(self) -> None:
        while len(self.incomming_dags) != 0 and self.incomming_dags[0].arrival_time <= self.time_stamp:
            for arriving_todoTask in self.incomming_dags[0].entry_tasks:
                self.buffering_tasks.append(arriving_todoTask)
            dag_to_process = self.incomming_dags.pop(0)
            self.processing_dags.append(dag_to_process)

    def pop_task(self, task_to_remove: Task, processor_id:int):
        # before we delete task_to_remove we want to append the children of that task to the upcomming tasks list
        self.buffering_tasks = list(filter(lambda todo: todo != task_to_remove, self.buffering_tasks))
        for child in task_to_remove.children:
            if child not in self.buffering_tasks:
                self.buffering_tasks.append(child)
                if child.min_start_time is None:
                    child.min_start_time = task_to_remove.finish_time
                else:
                    child.min_start_time = min(child.min_start_time, task_to_remove.finish_time)

    def take_action(self, action:Action):
        # Return a new child from self by taking the Action; action
        new_state:State = None
        if type(action) == WaitForProcessorToFinishAction or type(action) == WaitForNewIncommingDAGAction:
            new_state = deepcopy(self)
            new_state.time_stamp += action.dt
            for processor in new_state.processors:
                processor.step(new_state.time_stamp)
            new_state.available_actions = None
            new_state.explore_available_actions()

        elif type(action) == ScheduleTaskAction:
            action:ScheduleTaskAction = action
            new_state = deepcopy(self)
            task = filter(lambda t: t.name==action.task.name, new_state.buffering_tasks).__next__()
            new_state.processors[action.processor_id].start(task, new_state.time_stamp)
            new_state.pop_task(task, action.processor_id)
            new_state.available_actions = None
            new_state.explore_available_actions()
            #new_state.pop_task_from_list(action.task, action.processor_id)
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
        action_list = []
        for action in self.available_actions:
            action_list.append(str(action))
        return f"time: {self.time_stamp}, running processors:\n"+\
            f"{str(id_list)}\n"+\
            f"{str(task_list)}\n"+\
            f"Actions:{action_list}"


def state_heuristic(state:State, action:Action):
    """Evaluates a heuristic value of the state and the action which
    lead to that state"""
    soonest_deadline = 1e100
    for dag in state.processing_dags:
        for task in dag.task_list:
            soonest_deadline = min(soonest_deadline, task.dag_deadline)
    return -soonest_deadline
