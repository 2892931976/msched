import os
import json
import logging
import uuid
from collections import defaultdict
from kazoo.recipe.watchers import ChildrenWatch


class Scheduler:
    def __init__(self, zk, root):
        self.zk = zk
        self.root = root

    def watch(self):
        node = os.path.join(self.root, 'signal')
        ChildrenWatch(self.zk, node, self.run)

    def get_task_info(self, task_id):
        node = os.path.join(self.root, 'tasks', task_id)
        data, _ = self.zk.get(node)
        return json.loads(data.decode('utf-8'))

    def get_targets(self, task_id):
        node = os.path.join(self.root, 'tasks', task_id, 'targets')
        return self.zk.get_children(node)

    def get_target_status(self, task_id, target):
        node = os.path.join(self.root, 'tasks', task_id, 'targets', target)
        data, _ = self.zk.get(node)
        return data.decode('utf-8')

    def get_targets_status(self, task_id):
        targets = self.get_targets(task_id)
        status = ((target, self.get_target_status(task_id, target)) for target in targets)
        result = defaultdict(set)
        for s in status:
            result[s[1]].add(s[0])
        return result, targets

    def copy_task_to_agent(self, task_id, target, task):
        target_node = os.path.join(self.root, 'tasks', task_id, 'targets', target)
        lock = self.zk.Lock(target_node)
        with lock:
            data = json.dumps(task)
            node = os.path.join(self.root, 'agents', target, 'tasks', task_id)
            try:
                self.zk.create(node, data.encode('utf-8'))
            except Exception as e:
                logging.error(e)
                self.zk.set(target_node, b'F')
                self.zk.set(os.path.join(self.root, 'signal', task_id, uuid.uuid4().bytes))

    def set_task_status(self, task_id, status):
        node = os.path.join(self.root, 'callback', task_id)
        self.zk.create(node, status.encode())

    def run(self, tasks):
        for task_id in tasks:
            self.schedule(task_id)

    def schedule(self, task_id):
        node = os.path.join(self.root, 'tasks', task_id)
        lock = self.zk.Lock(node)
        with lock:
            task = self.get_task_info(task_id)
            status, targets = self.get_targets_status(task_id)
            fail_rate = len(status.get('F')) / len(targets)
            if fail_rate > task.get('fail_rate', 0):
                self.set_task_status(task_id, 'F')
                return True

            count = task.get('concurrent', 1) - (len(status.get('R')) + len(status['W']))
            can_schedule_count = min(count, len(status['N']))
            if can_schedule_count > 0:
                schedule_list = list(status['N'])[:can_schedule_count - 1]
                for target in schedule_list:
                    self.copy_task_to_agent(task_id, target, task)
            if len(status['N']) + len(status['W']) + len(status['R']) == 0:
                self.set_task_status(task_id, 'S')
        return True

if __name__ == '__main__':
    from kazoo.client import KazooClient
    task = {
        'job_id': 'test'
    }
    zk = KazooClient()
    zk.start()
    zk.ensure_path('/msched/agents/test/tasks')
    zk.ensure_path('/msched/tasks/test')
    zk.set('/msched/tasks/test', json.dumps(task).encode())
    zk.ensure_path('/msched/tasks/test/targets/test')
    zk.set('/msched/tasks/test/targets/test', b'N')
    zk.ensure_path('/msched/signal/test')
    sched = Scheduler(zk, 'msched')
    sched.watch()