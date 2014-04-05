
from __future__ import absolute_import
from . import mdqueue_priorities as priorities

class Type:
    GROMACS = 0




class TaskQueue(object):
    def __init__(self, scheduling):
        "docstring"
        self._scheduling = scheduling
        self._pq = PriorityQueue('max')

    def add(self, task, priority):
        self._pq.add(task, priority=priority)








