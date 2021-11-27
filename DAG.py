from Task import Task
import json

class DAG:
    name = None

    def __init__(self, name, json_dag_data: dict) -> None:
        self.name = name

        self.task_list: list[Task] = list()
        self.entry_tasks: list[Task] = list()  # Tasks without parents
        self.exit_tasks:list[Task] = list()

        self._type: int = json_dag_data['Type']
        self.arrival_time: int = json_dag_data['ArrivalTime']
        self.deadline: int = json_dag_data['Deadline']

        self.is_complete:bool = False
        self._failed = False

        name_to_task:dict[str, Task] = dict()
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
            if task.is_complete == False:
                return
        self.is_complete = True
    def __str__(self) -> str:
        return json.dumps(self.__dict__, default=lambda o: str(o) if type(o) == Task else o.__dict__, indent=4)