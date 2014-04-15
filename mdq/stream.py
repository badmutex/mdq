from pxul.logging import logger

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

class Downstream(object):
    @property
    def upstream(self):
        raise NotImplemented

class Fount(Unique, Generator):
    """
    Fount :: Stream a
    """
    def __iter__(self):
        for task in self.generate():
            yield task

class Stream(Unique, Processor, Downstream):
    """
    Stream :: Stream a -> Stream b
    """
    def __init__(self, source):
        super(Stream, self).__init__()
        self._fountain = source

    @property
    def upstream(self):
        return iter(self._fountain)

    def __iter__(self):
        for task in self.upstream:
            for result in self.process(task):
                if result is None: continue
                yield result

def run_stream(iterable, stream_cls, extra_args=None, extra_kws=None):
    extra_args = extra_args or list()
    extra_kws  = extra_kws  or dict()
    stream = stream_cls(iterable, *extra_args, **extra_kws)
    sink = Sink(stream)
    return sink()


class IPersistableTaskStream(Stream):
    """
    Persistable a => Stream a -> Stream a
    """
    def __init__(self, source, persist):
        super(IPersistableTaskStream, self).__init__(source)
        self._persist = persist

    def process(self): raise NotImplemented

class PersistTaskStream(IPersistableTaskStream):
    def process(self, taskable):
        logger.debug('PersistTaskStream: processing %s' % taskable.digest)
        self._persist[taskable.digest] = taskable
        yield taskable

class ResumeTaskStream(IPersistableTaskStream):
    def process(self, taskable):
        logger.debug('ResumeTaskStream: processing %s' % taskable.digest)
        if taskable.digest in self._persist:
            t = self._persist[taskable.digest]
        else:
            t = taskable
        yield t

class WorkQueueStream(Stream):
    """
    WorkQueueStream :: Persistable t, Task t => Stream t -> Stream t
    """
    def __init__(self, q, source, timeout=5, persist_to=None):
        super(WorkQueueStream, self).__init__(source)
        self._q = q
        self._timeout = timeout
        self._table = dict() # Task t => uuid -> t
        self._persist_to = persist_to

    @property
    def wq(self): return self._q

    def __len__(self):
        """
        Returns the current number of tasks in the queue
        """
        return len(self._table)

    def _persist(self, taskable):
        if self._persist_to is not None:
            logger.debug('%-15s' % 'Persisting', taskable.uuid)
            run_stream((lambda: (yield taskable))(),
                       PersistTaskStream,
                       extra_args = [self._persist_to])

    def submit(self, taskable):
        logger.debug('%-15s' % 'Submitting', taskable.uuid)
        task = taskable.to_task()
        self._table[task.uuid] = taskable
        return self.wq.submit(task)

    def empty(self):
        return len(self._table) <= 0

    def wait(self):
        result = self.wq.wait(self._timeout)
        if result:
            logger.info1('%-15s' % 'Received', result.uuid)
            taskable = self._table[result.uuid]
            taskable.update_task(result)
            del self._table[result.uuid]
            self._persist(taskable)
            return taskable

    def __iter__(self):

        for t in self.upstream:
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
    GenerationalWorkQueueStream :: Persistable t, Extendable t, Taskable t => Stream t -> Stream t

    Run tasks for a given number of generations using the `.extend` method
    """
    def __init__(self, *args, **kws):
        gens = kws.pop('generations', 1)
        super(GenerationalWorkQueueStream, self).__init__(*args, **kws)
        self._generations = gens
        self._count       = collections.defaultdict(lambda:0) # uuid -> int

    @property
    def upstream(self):
        # upstream tasks may have been resumed so update the local
        # count and filter out the tasks  that have already finished
        for task in super(GenerationalWorkQueueStream, self).upstream:
            self._count[task.uuid] = task.generation
            if self._is_submittable(task):
                logger.info('%-15s' % 'Continuing', task.uuid, 'from generation', self._gen(task))
                yield task

    def _gen(self, task):
        return self._count[task.uuid]

    def _incr(self, task):
        self._count[task.uuid] += 1

    def _is_submittable(self, task):
        return self._gen(task) < self._generations

    def process(self, task):
        if self._is_submittable(task):
            logger.info('%-15s' % 'Extending', task.uuid, 'to generation', self._gen(task)+1)
            self._incr(task)
            task.extend()
            self.submit(task)
            yield None
        else:
            logger.info('%-15s' % 'Stopping', task.uuid, 'at generation', self._gen(task))
            del self._count[task.uuid]
            yield task


class Sink(Unique, Processor, Downstream):
    """
    Sink :: Stream a -> b
    """
    def __init__(self, source):
        super(Sink, self).__init__()
        self._source = source

    @property
    def upstream(self):
        return iter(self._source)

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
        for t in self.upstream:
            for r in self.process(t):
                yield r

    def __call__(self):
        """
        Pull all the results
        """
        for obj in self:
            self.consume(obj)

        return self.result
