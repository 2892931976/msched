import os
import threading
from kazoo.exceptions import NodeExistsError, NoNodeError


class Lock:
    def __init__(self, zk, path, identify=''):
        self.zk = zk
        self.path = os.path.join(path, 'LOCK')
        self.identify = identify
        self.event = threading.Event()

    def acquire(self, block=True, timeout=None):
        try:
            self.zk.create(self.path, self.identify.encode('utf-8'), ephemeral=True)
            return True
        except NodeExistsError:
            if block:
                self.event.wait(timeout)
                return self.acquire(False)

    def release(self):
        try:
            self.zk.delete(self.path)
        except NoNodeError:
            pass

    def __enter__(self):
        self.acquire(True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()