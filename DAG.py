from Task import Task, TODOTask
import json

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

        # Find all tasks without parents or children
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

    def __str__(self) -> str:
        return json.dumps(self.__dict__, default=lambda o: str(o) if type(o) == Task else o.__dict__, indent=4)
