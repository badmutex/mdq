#!/usr/bin/python

from mdq.stream    import Fount, GenerationalWorkQueueStream, Sink
from mdq.workqueue import MkWorkQueue, WorkQueue
import mdq.md.gmx as gmx
import mdq.util


class MockFount(Fount): # :: Stream gmx.Task

    def generate(self):
        sim = gmx.Prepare(cpus       = 1,
                          mdrun      = mdq.util.find_in_path('mdrun'),
                          guamps_get = mdq.util.find_in_path('guamps_get'),
                          guamps_set = mdq.util.find_in_path('guamps_set'),
                          )

        for i in xrange(5):
            yield sim.prepare('tests/data/topol.tpr', outputdir='tests/sim/test%s' % i, seed=i)

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
    submit    = GenerationalWorkQueueStream(q, fount, timeout=1, generations=2)
    sink      = MockSink(submit)
    sink()
