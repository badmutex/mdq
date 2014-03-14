from mdq.stream    import Fount, Stream, WorkQueueStream, Sink
from mdq.workqueue import Task, MkWorkQueue
import collections


class MockFount(Fount):
    def generate(self):
        import random
        for i in xrange(10):
            sleep = 1
            sleep = random.randint(1,30)
            yield Task('echo %s;sleep %s' % (i, sleep))

class MockGenerations(WorkQueueStream):
    def __init__(self, *args, **kws):
        gens = kws.pop('generations', 1)
        super(MockGenerations, self).__init__(*args, **kws)
        self._generations = gens
        self._gens = collections.defaultdict(lambda:0) # uuid -> <int>

    def process(self, task):
        self._gens[task.tag] += 1
        if self._gens[task.tag] < self._generations:
            print 'Continuing', task.tag, self._gens[task.tag]
            self.submit(task)
            yield None
        else:
            print 'Stopping generation', task.tag, self._gens[task.tag]
            yield task

if __name__ == '__main__':

    q = (
        MkWorkQueue()
        .port(9123)
        .debug_all()
        .replicate(8)
        ()
    )

    with open('wq.log', 'w') as fd:
        fd.write('#')
        q.specify_log(fd.name)

    fount     = MockFount()
    submit    = MockGenerations(q, fount, generations=5)
    sink      = Sink(submit)

    for r in sink:
        print r.tag
