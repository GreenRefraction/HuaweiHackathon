import json


class Task:
    name = None
    dag = None
    EET = None
    _type = None
    children = None
    parents = None

    is_complete:bool = None

    def __init__(self, name, dag, json_task_data: dict) -> None:
        self.name = name
        self.EET = json_task_data['EET']
        self._type = json_task_data['Type']

        self.children: list[(Task, int)] = list()
        self.parents: list[(Task, int)] = list()  # Needed?

        self.is_complete:bool = False
        self.dag = dag

        self.dag = None

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
        return json.dumps(str(self.__dict__), default=lambda o:o.name if type(o) == Task else str(o.__dict__))
