import json

class Task:
    name = None
    EET = None
    _type = None
    children = None
    parents = None
    dag_id = None

    def __init__(self, name, json_task_data: dict) -> None:
        self.name = name
        self.EET = json_task_data['EET']
        self._type = json_task_data['Type']

        self.children: list[(Task, int)] = list()
        self.parents: list[(Task, int)] = list()  # Needed?

    def add_child(self, task, weight):
        self.children.append((task, weight))

    def add_parent(self, task, weight):
        self.parents.append((task, weight))

    def __str__(self) -> str:
        return json.dumps(str(self.__dict__), default=lambda o:o.name if type(o) == Task else str(o.__dict__))
