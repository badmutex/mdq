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
    def __init__(self, q, source, wait=5):
        super(Stream   , self).__init__()
        self._q = q
        self._fountain = source
        self._wait = wait

    @property
    def wq(self): return self._q

    def __iter__(self):

        # if the usual `while not q.empty()` is used then when
        # multiple `Stream`s are used with the same `WorkQueue` object
        # then the second loop will never exit since the queue will
        # never empty.
        
        count = 0
        for t in self._fountain:
            print 'submitting', t.tag
            self._q.submit(t)
            count += 1

        while count > 0:
            self._q.replicate()
            r = self._q.wait(self._wait)
            if r:
                count -= 1
                for result in self.process(r):
                    yield result

class Sink(Unique, Processor):
    def __init__(self, source):
        super(Sink, self).__init__()
        self._source = source

    def __iter__(self):
        for t in self._source:
            t.specify_tag('%s:%s' % (t.tag, self._uuid))
            yield t, t.tag


if __name__ == '__main__':
    ccl.set_debug_flag('all')
    q       = ccl.WorkQueue(9123)
    fount   = Fount()        # some source of Tasks
    stream0 = Stream(q)      # some execution engin
    stream0.connect(fount)   #+that pulls from upstream
    stream1 = Stream(q)      # another execution engin
    stream1.connect(stream0) #+that may resubmit tasks
    sink    = Sink(stream1)  # where to dump final results

    # the Sink pulls the results from upstream
    for result in sink:
        print result
