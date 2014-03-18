from mdq.stream    import Fount, Stream, WorkQueueStream, Sink
from mdq.workqueue import Task, MkWorkQueue
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

class MockGenerations(WorkQueueStream): # Stream gmx.Task -> Stream pwq.Task
    def __init__(self, *args, **kws):
        gens = kws.pop('generations', 1)
        super(MockGenerations, self).__init__(*args, **kws)
        self._generations = gens
        self._gens = collections.defaultdict(lambda:0) # uuid -> <int>

    def process(self, task):
        self._gens[task.tag] += 1
        if self._gens[task.tag] < self._generations:
            print 'Continuing', task.tag, self._gens[task.tag]
            task.extend()
            self.submit(task)
            yield None
        else:
            print 'Stopping generation', task.tag, self._gens[task.tag]
            yield task

if __name__ == '__main__':

    q = (
        MkWorkQueue()
        .port(9123)
        # .debug_all()
        .replicate(8)
        ()
    )

    with open('wq.log', 'w') as fd:
        fd.write('#')
        q.specify_log(fd.name)

    fount     = MockFount()
    submit    = MockGenerations(q, fount, generations=2)
    sink      = Sink(submit)

    for r in sink:
        print 'Complete', r.tag
