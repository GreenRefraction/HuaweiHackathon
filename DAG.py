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

class TODODAG:
    def __init__(self, dag:DAG) -> None:
        self.dag = dag
        self.is_complete = False

        self.todo_list:list[TODOTask] = list()
        for task in dag.task_list:
            todo = TODOTask(task, self, None)
            self.todo_list.append(todo)

        # connect all the parents and children
        for todo1 in self.todo_list:
            for todo2 in self.todo_list:
                if todo1.task in todo2.task.parents:
                    todo1.children.append(todo2)
                    todo2.parents.append(todo1)
                elif todo2.task in todo2.task.parents:
                    todo2.children.append(todo1)
                    todo1.parents.append(todo2)
        
        self.entry_todo:list[TODOTask] = list()
        self.exit_todo:list[TODOTask] = list()
        for todo in self.todo_list:
            if len(todo.parents) == 0:
                self.entry_todo.append(todo)
                todo.min_start_time = dag.arrival_time
            if len(todo.children) == 0:
                self.exit_todo.append(todo)

        self.arrival_time = dag.arrival_time
        self.deadline = dag.deadline
        self.period = dag.period

    def tick(self) -> None:
        """set is_complete to True if all of the tasks in this dag are complete"""
        for todo in self.exit_todo:
            if not todo.is_complete:
                return
        self.is_complete = True
