
from . import decorator

class WorkQueue(decorator.WorkQueue):

    def __init__(self, q, generations=1):
        super(WorkQueue, self).__init__(q)
        self._generations = generations
        self._generators = dict() # tag -> generator

    def submit(self, generator):
        task = generator()
        self._generators[task.tag] = generator
        return self._q.submit(task)

    def wait(self, *args, **kws):
        task = self._q.wait(*args, **kws)
        if task and task.result == 0:
            self._submit_next(task)
            return None
        else:
            return task

    def _submit_next(self, task):
        gen = self._generators[task.tag]
        if gen.generation < self._generations:
            gen.incr()
            new = gen()
            return self._q.submit(new)
        else:
            return None
