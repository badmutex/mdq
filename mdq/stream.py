import work_queue as ccl

import uuid


class Fount(object):

    def __init__(self):
        self._uuid = uuid.uuid1()

    def __iter__(self):
        # MOCKUP
        for i in xrange(10):
            task = ccl.Task('echo %s; sleep 2' % i)
            task.specify_tag(str(self._uuid))
            yield task


class Stream(object):
    def __init__(self, q):
        self._q = q
        self._fountain = None

        self._uuid = uuid.uuid1()

    def connect(self, source):
        self._fountain = source

    def process(self, result):
        """
        To be implemented in a subclass
        """
        return result

    def __iter__(self):

        # if the usual `while not q.empty()` is used then when
        # multiple `Stream`s are used with the same `WorkQueue` object
        # then the second loop will never exit since the queue will
        # never empty.
        
        count = 0
        for t in self._fountain:
            t.specify_tag('%s:%s' % (t.tag, self._uuid))
            self._q.submit(t)
            count += 1

        while count > 0:
            r = self._q.wait(5)
            if r:
                count -= 1
                yield self.process(r)

class Sink(object):
    def __init__(self, source):
        self._source = source

        self._uuid = uuid.uuid1()

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
