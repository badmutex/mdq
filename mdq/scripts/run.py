from .. import state
from ..stream    import Fount, ResumeTaskStream, GenerationalWorkQueueStream, Sink
from ..workqueue import MkWorkQueue

import argparse


def getopts():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-p', '--port', default=9123, type=int, help='Work Queue port')
    p.add_argument('-r', '--replicate', default=1, type=int, help='Task replication')
    p.add_argument('-d', '--debug', action='store_true', help='Turn on debugging information')
    p.add_argument('-l', '--logfile', default=None, help='Write the workqueue log to this file')

    return p.parse_args()


class TaskFount(Fount):
    def set_state(self, state):
        self._state = state

    def generate(self):
        return iter(self._state.values())

class TaskSink(Sink):
    def consume(self, task):
        print 'Complete', task.digest
    
def main():
    opts = getopts()

    mkq = (
        MkWorkQueue()
        .port(opts.port)
        .replicate(opts.replicate)
        )
    if opts.debug: mkq.debug_all()

    q = mkq()

    with state.State.load() as st:
        fount = TaskFount()
        fount.set_state(st)
        persist = ResumeTaskStream(fount, st.store)
        submit = GenerationalWorkQueueStream(q, persist, timeout=5, persist_to=st.store, generations=2)
        sink = TaskSink(submit)
        sink()
