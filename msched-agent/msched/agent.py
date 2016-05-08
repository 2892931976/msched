import os
import io
import json
import uuid
import datetime
import threading
import random
import zipfile
import requests
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from .command import Command


class Listener:
    def __init__(self, hosts, root, workspace='/tmp'):
        self.zk = KazooClient(hosts=hosts)
        self.root = root
        self.workspace = os.path.abspath(workspace)
        self.tasks = []
        self.event = threading.Event()
        self.hostname = os.uname().nodename

    def get_task(self, task_id):
        node = os.path.join(self.root, 'tasks', task_id, 'targets', self.hostname)
        lock_node = os.path.join(node, 'lock')
        lock = self.zk.Lock(lock_node, self.hostname.encode())
        with lock:
            data, _ = self.zk.get(node)
        return json.dumps(data.decode())

    def set_status(self, task_id, status):
        node = os.path.join(self.root, 'tasks', task_id, 'targets', self.hostname)
        lock_node = os.path.join(node, 'lock')
        lock = self.zk.Lock(lock_node, self.hostname.encode())
        with lock:
            self.zk.set(node, status.encode())
        signal_node = os.path.join(self.root, 'signal', task_id)
        self.zk.set(signal_node, uuid.uuid4().bytes)

    def get_job_server_list(self):
        node = os.path.join(self.root, 'job_server')
        return [self.zk.get(os.path.join(node, x))[0] for x in self.zk.get_children(node)]

    def schedule(self, task_id):
        task = self.get_task(task_id)
        job_server = random.choice(self.get_job_server_list())
        # http://xxx.xxx.xx.xxx/packages/
        # magedu/test-job
        # http://xxx.xxx.xx.xxx/packages/magedu/test-job.zip
        url = '{0}/{1}.zip'.format(job_server, task_id['job_id'])
        response = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        workspace = os.path.join(self.workspace, task_id)
        os.chdir(workspace)
        z.extractall()
        os.chmod('./run.sh', 0o755)
        cmd = Command('run.sh', workspace, timeout=task.get('timeout', 0))
        self.set_status(task_id, 'R')
        cmd.exec()
        # TODO copy output stream to log server
        cmd.wait()
        if cmd.success:
            self.set_status(task_id, 'S')
        else:
            self.set_status(task_id, 'F')

    def run(self):
        while not self.event.is_set():
            if len(self.tasks) > 0:
                self.schedule(self.tasks.pop(0))
            else:
                self.event.wait(1)

    def watch(self, tasks):
        new_tasks = set(tasks).difference(self.tasks)
        self.tasks.extend(new_tasks)
        return not self.event.is_set()

    def start(self):
        self.zk.start()
        node = os.path.join(self.root, 'agents', self.hostname)
        self.zk.ensure_path(node)
        tasks_node = os.path.join(node, 'tasks')
        self.zk.ensure_path(tasks_node)
        self.zk.create(os.path.join(node, 'alive'), str(datetime.datetime.now().timestamp()).encode(), ephemeral=True)
        ChildrenWatch(self.zk, tasks_node, self.watch)

    def shutdown(self):
        self.event.set()

    def join(self):
        self.event.wait()
