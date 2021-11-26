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

        self.children: list[(Task, int)] = list()
        self.parents: list[(Task, int)] = list()  # Needed?

    def add_child(self, task, weight):
        self.children.append((task, weight))

    def add_parent(self, task, weight):
        self.parents.append((task, weight))

    def __str__(self) -> str:
        return json.dumps(self.__dict__, default=lambda o: o.__dict__)


class DAG:
    name = None

    def __init__(self, name, json_dag_data: dict) -> None:
        self.name = name

        self.task_list: list[Task] = list()
        self.entry_tasks: list[Task] = list()  # Tasks without parents

        self.instance: int = json_dag_data['Type']
        self.arrival_time: int = json_dag_data['ArrivalTime']
        self.deadline: int = json_dag_data['Deadline']

        name_to_task = dict()
        task_names = list(
            filter(lambda s: s[:4] == 'Task', json_dag_data.keys()))
        # Create all tasks
        for task_name in task_names:
            task = Task(task_name, json_dag_data[task_name])
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

        self.task_list = list(name_to_task.values())

    def __init__task_recursive(self, task_name, json_dag_data):
        raise DeprecationWarning()
        task = Task(task_name, json_dag_data[task_name])
        for child_name in json_dag_data[task_name]['next']:
            child = self.__init__task_recursive(child_name, json_dag_data)
            task.add_child(child)
        self.task_list.append(task)
        return task

    def __str__(self) -> str:
        return json.dumps(self.__dict__, default=lambda o: o.__dict__, indent=4)


def load_from_json(file_name) -> list[DAG]:
    with open(file_name, 'r') as f:
        data = json.load(f)
        dags = []
        for dag_name in data.keys():
            dags.append(DAG(dag_name, data[dag_name]))
        return dags


if __name__ == '__main__':
    dag_list: list[DAG] = load_from_json('testcases/test1.json')
    print(dag_list[0].entry_tasks[0].children)
    print(dag_list[1].entry_tasks[0].children)
