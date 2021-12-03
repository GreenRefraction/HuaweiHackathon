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
        self.dag_deadline:int = task.dag.deadline

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

    def step(self, t):
        """Take a step to time t"""
        if self.is_idle:
            return
        if self.finish_time_of_running_task <= t:
            # finish the current task
            self.finish()

            
    def finish(self):
        """Finish the task that is currently running"""
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
        # check for mutual exclusion
        
        
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
        self.utilization_time += eet
        return True