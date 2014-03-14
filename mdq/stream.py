import work_queue as ccl

import uuid


class Unique(object):
    def __init__(self):
        self._uuid = uuid.uuid1()

    def update_tag(self, task):
        if task.tag:
            task.specify_tag('%s:%s' % (task.tag, self.id))
        else:
            task.specify_tag(self.id)

    def previous_tag(self, tag):
        uuids = tag.split(':')
        if len(uuids) > 1:
            return ':'.join(uuids[:-2])
        else:
            return uuid[0]

    @property
    def id(self): return str(self._uuid)

class Generator(object):
    def generate(self):
        raise NotImplemented

class Processor(object):
    def process(self, task):
        yield task


class Fount(Unique, Generator):
    def __iter__(self):
        for task in self.generate():
            task.specify_tag('%s' % uuid.uuid1())
            yield task

class Stream(Unique, Processor):
    def __init__(self, source):
        super(Stream, self).__init__()
        self._fountain = source

    def __iter__(self):
        for task in self._fountain:
            for result in self.process(task):
                if result is None: continue
                yield result


class WorkQueueStream(Stream):
    def __init__(self, q, source, timeout=5):
        super(WorkQueueStream, self).__init__(source)
        self._q = q
        self._timeout = timeout
        self._count = 0

    @property
    def wq(self): return self._q

    def submit(self, task):
        self.wq.submit(task)
        self._count += 1

    def _empty(self):
        return not self._count > 0

    def _wait(self):
        result = self.wq.wait(self._timeout)
        if result:
            self._count -= 1
        return result

    def __iter__(self):

        for t in self._fountain:
            print 'submitting', t.tag
            self.submit(t)

        while not self._empty():
            self._q.replicate()
            print 'count', self._count
            r = self._wait()
            if r:
                for result in self.process(r):
                    if result is None: continue
                    yield result

class Sink(Unique, Processor):
    def __init__(self, source):
        super(Sink, self).__init__()
        self._source = source

    def __iter__(self):
        for t in self._source:
            for r in self.process(t):
                yield r

    def __call__(self):
        """
        Pull all the results
        """
        for _ in self:
            pass
