from Task import Task, TODOTask

class Processor:

    current_running_todoTask: TODOTask = None
    is_idle: bool = None
    finish_time_of_running_task: int = None
    utilization_time: int = None

    cache_size = 4
    cache: list[Task] = list()

    # This set contains the answer of a task which is
    # idk what to call this
    answers: set[TODOTask] = None
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
        self.answers.add(self.current_running_todoTask)

        # append to the cache
        self.cache.append(self.current_running_todoTask.task)
        if len(self.cache) > self.cache_size:
            self.cache.pop(0)

        # tick the current task
        self.current_running_todoTask.tick()
        # and tick the dag
        self.current_running_todoTask.todoDAG.tick()
        self.current_running_todoTask.prefered_processor = self.id
        self.current_running_todoTask = None
        self.is_idle = True
        self.finish_time_of_running_task = None
    def can_start(self, todo:TODOTask, t) -> bool:

        if not self.is_idle:
            return False
        # check if the parents of the task is finished

        # if todo.task.name == "Task5023":
        #     print("min start time 5023", todo.min_start_time)
        if todo.min_start_time > t:
            return False
        for parent in todo.parents:
            # if todo.task.name == "Task5023":
            #     print(f"parrent {parent.name} to 5023 done:", parent.is_complete)
            if not parent.is_complete:
                return False
        return True

    def start(self, todoTask: TODOTask, t) -> bool:
        """attempt to start a new task"""
        if not self.can_start(todoTask, t):
            return False
        # First check the communication time
        ict_list: list[int] = []
        pay_the_fee: bool = False
        for parent in todoTask.parents:
            if parent not in self.answers:  # cleaning might speed up things, it will sure as hell save memory
                # now we need to pay the ict fee
                pay_the_fee = True
                ict = parent.task.ict_to_children[todoTask.task.name]
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

        self.current_running_todoTask = todoTask
        self.is_idle = False

        # if task has the same _type we can reduce the EET by 10%
        eet = todoTask.task.EET
        if todoTask.task._type in [cached_task._type for cached_task in self.cache]:
            eet = int(eet * 0.9)  # CHECK? rounding error?
            # print(t, "new instance", eet, todo.task.dag_id)

        self.finish_time_of_running_task = t + eet
        if pay_the_fee:
            self.finish_time_of_running_task += ict
        todoTask.finish_time = self.finish_time_of_running_task

        # call on the execution history
        task_id = int(todoTask.task.name[4:])
        self.execution_history.append(
            (task_id, todoTask.finish_time - eet, todoTask.finish_time))

        # add the eet to utilization_time
        self.utilization_time += eet + ict if pay_the_fee else eet
        return True
