import json

class Task:
    def __init__(self, name, dag, json_task_data: dict) -> None:
        self.name = name
        self.EET = json_task_data['EET']
        self._type = json_task_data['Type']

        self.children: list[Task] = list()
        self.parents: list[Task] = list()
        self.ict_to_parents: dict[str, int] = dict()
        self.ict_to_children: dict[str, int] = dict()
        self.n_children:int = 0

        self.dag = dag

        # attributes about the depth from self
        self.EET_depth: int = None
        self.norm_EET_depth: int = None
        self.child_depth = None

        # p_id ict finish_time
        self.dag_deadline: int = self.dag.deadline
        self.dag_period: int = self.dag.period

    # from the list of parents, find the max eet for this task
    # and that would be the starting time for this task.

    def add_child(self, task, ict):
        self.ict_to_children[task.name] = ict
        self.children.append(task)

    def add_parent(self, task, ict):
        self.parents.append(task)
        self.ict_to_parents[task.name] = ict

    def add_dag(self, dag):
        self.dag = dag

    def tick(self):
        self.is_complete = True
        self.dag.tick()

    def calc_effective_depth(self):
        if self.EET_depth is not None:
            return self.EET_depth
            
        self.EET_depth = self.EET
        
        max_child_depth = 0
        for child in self.children:
            max_child_depth = max(max_child_depth, child.calc_effective_depth())

        self.EET_depth += max_child_depth
        self.norm_EET_depth = self.EET_depth / self.dag_deadline

        return self.EET_depth

    def calc_child_depth(self):
        if self.child_depth is not None:
            return self.child_depth
        
        max_depth = 0
        for child in self.children:
            max_depth = max(max_depth, child.calc_child_depth())
        
        self.child_depth = max_depth + 1
        return self.child_depth

    def __str__(self) -> str:
        return json.dumps(str(self.__dict__), default=lambda o: o.name if type(o) == Task else str(o.__dict__))

