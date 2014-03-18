from mdq.stream    import Fount, GenerationalWorkQueueStream, Sink
from mdq.workqueue import MkWorkQueue, WorkQueue
import mdq.md.gmx as gmx
import mdq.util


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
