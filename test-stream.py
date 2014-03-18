from mdq.stream    import Fount, Stream, WorkQueueStream, Sink
from mdq.workqueue import MkWorkQueue, WorkQueue
import mdq.md.gromacs as gmx
import mdq.util

import collections

class MockFount(Fount): # :: Stream gmx.Task

    def generate(self):
        sim = gmx.Task(x='tests/data/mdq/0/x.gps',
                       v='tests/data/mdq/0/v.gps',
                       t='tests/data/mdq/0/t.gps',
                       tpr='tests/data/topol.tpr',
                       cpus=1,
                       )
        sim.keep_trajfiles()
        for name in gmx.EXECUTABLES: sim.add_binary(mdq.util.find_in_path(name))
        yield sim

class MockGenerations(WorkQueueStream):
    def __init__(self, *args, **kws):
        gens = kws.pop('generations', 1)
        super(MockGenerations, self).__init__(*args, **kws)
        self._generations = gens
        self._gens = collections.defaultdict(lambda:0) # uuid -> <int>

    def process(self, task):
        self._gens[task.uuid] += 1
        if self._gens[task.uuid] < self._generations:
            print 'Continuing', task.uuid, self._gens[task.uuid]
            task.extend()
            self.submit(task)
            yield None
        else:
            del self._gens[task.uuid]
            print 'Stopping generation', task.uuid, self._gens[task.uuid]
            yield task

class MockSink(Sink):
    def consume(self, task):
        print 'Complete', task.uuid

if __name__ == '__main__':

    mkq = (
        MkWorkQueue()
        .port(9123)
        .replicate(8)
        # .debug_all()
    )

    q = mkq()

    with open('wq.log', 'w') as fd:
        fd.write('#')
        q.specify_log(fd.name)

    fount     = MockFount()
    submit    = MockGenerations(q, fount, timeout=1, generations=5)
    sink      = MockSink(submit)
    sink()
