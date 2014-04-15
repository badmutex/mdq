
import heapq

class PriorityQueue(object):
    def __init__(self, sorting='min'):
        """
        Construct a priority queue.

        Params:
          - sorting :: str = {'min' | 'max'}
        """
        self._sorting = sorting.lower()
        self._h = []

    def add(self, obj, priority=0):
        "Add an element to the queue"
        if self._sorting == 'min':
            prio = priority
        elif self._sorting == 'max':
            prio = -priority
        else: raise ValueError('Invalid sorting scheme: %s' % self._sorting)
        heapq.heappush(self._h, (prio, obj))

    def pop(self):
        _, obj = heapq.heappop(self._h)
        return obj
