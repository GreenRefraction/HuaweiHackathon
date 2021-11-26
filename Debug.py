import json


class Task:
    name = None
    EET = None
    instance = None
    children = None

    def __init__(self, name, json_task_data: dict) -> None:
        self.name = name
        self.EET = json_task_data['EET']
        self.instance = json_task_data['Type']
        
        self.children:dict[Task, int] = dict()
        self.parents :list[Task] = list()

    def __str__(self) -> str:
        return json.dumps(str(self.__dict__), default=lambda o:o.name if type(o) == Task else str(o.__dict__))

def wire_tasks(parent:Task, child:Task, ict:int):
    parent.children[child] = ict
    child.parents.append(parent)

class DAG:
    name = None

    def __init__(self, name, json_dag_data: dict) -> None:
        self.name = name

        self.task_list: list[Task] = list()

        self.instance: int = json_dag_data['Type']
        self.arrival_time: int = json_dag_data['ArrivalTime']
        self.deadline: int = json_dag_data['Deadline']

        # The following 2 for loops could probably be unified into a recursive thing
        name_to_task = dict()
        for task_name in json_dag_data:
            if task_name[:4] == "Task":
                task = Task(task_name, json_dag_data[task_name])
                self.task_list.append(task)
                name_to_task[task_name] = task

        for task in self.task_list:
            for next_task_name in json_dag_data[task.name]['next']:
                child = name_to_task[next_task_name]
                ict = json_dag_data[task.name]['next'][next_task_name]
                wire_tasks(task, child, ict)

    def __init__task_recursive(self, task_name, json_dag_data):
        raise DeprecationWarning()
        task = Task(task_name, json_dag_data[task_name])
        for child_name in json_dag_data[task_name]['next']:
            child = self.__init__task_recursive(child_name, json_dag_data)
            task.add_child(child)
        self.task_list.append(task)
        return task

    def __str__(self) -> str:
        return json.dumps(self.__dict__, default=lambda o: str(o) if type(o) == Task else o.__dict__, indent=4)


handle = open("sample.json")
data = json.load(handle)
dag_list:list[DAG] = [DAG(name, data[name]) for name in data]
dag0 = dag_list[0]
print(dag0)