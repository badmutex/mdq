import work_queue as ccl

import collections
import uuid


class Unique(object):
    def __init__(self):
        self._uuid = uuid.uuid1()

    @property
    def uuid(self): return str(self._uuid)

class Generator(object):
    def generate(self):
        raise NotImplemented

class Processor(object):
    def process(self, task):
        yield task


class Fount(Unique, Generator):
    """
    Fount :: Stream a
    """
    def __iter__(self):
        for task in self.generate():
            yield task

class Stream(Unique, Processor):
    """
    Stream :: Stream a -> Stream b
    """
    def __init__(self, source):
        super(Stream, self).__init__()
        self._fountain = source

    def __iter__(self):
        for task in self._fountain:
            for result in self.process(task):
                if result is None: continue
                yield result


class WorkQueueStream(Stream):
    """
    WorkQueueStream :: Task t => Stream t -> Stream t
    """
    def __init__(self, q, source, timeout=5):
        super(WorkQueueStream, self).__init__(source)
        self._q = q
        self._timeout = timeout
        self._table = dict() # Task t => uuid -> t

    @property
    def wq(self): return self._q

    def __len__(self):
        """
        Returns the current number of tasks in the queue
        """
        return len(self._table)

    def submit(self, taskable):
        task = taskable.to_task()
        self._table[task.uuid] = taskable
        return self.wq.submit(task)

    def empty(self):
        return len(self._table) <= 0

    def wait(self):
        result = self.wq.wait(self._timeout)
        if result:
            taskable = self._table[result.uuid]
            taskable.update_task(result)
            del self._table[result.uuid]
            return taskable

    def __iter__(self):

        for t in self._fountain:
            self.submit(t)

        while not self.empty():
            self.wq.replicate()
            r = self.wait()
            if r:
                for result in self.process(r):
                    if result is None: continue
                    yield result

class GenerationalWorkQueueStream(WorkQueueStream):
    """
    GenerationalWorkQueueStream :: Extendable t, Taskable t => Stream t -> Stream t

    Run tasks for a given number of generations using the `.extend` method
    """
    def __init__(self, *args, **kws):
        gens = kws.pop('generations', 1)
        super(GenerationalWorkQueueStream, self).__init__(*args, **kws)
        self._generations = gens
        self._count       = collections.defaultdict(lambda:0) # uuid -> int

    def process(self, task):
        self._count[task.uuid] += 1
        gen = self._count[task.uuid]
        if gen < self._generations:
            task.extend()
            self.submit(task)
            yield None
        else:
            del self._count[task.uuid]
            yield task

class Sink(Unique, Processor):
    """
    Sink :: Stream a -> b
    """
    def __init__(self, source):
        super(Sink, self).__init__()
        self._source = source

    @property
    def result(self):
        """
        The accumulated result
        """
        return None

    def consume(self, obj):
        """
        Consume each result from upstream.
        """
        pass

    def __iter__(self):
        for t in self._source:
            for r in self.process(t):
                yield r

    def __call__(self):
        """
        Pull all the results
        """
        for obj in self:
            self.consume(obj)

        return self.result
