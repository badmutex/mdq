from .. import state
from ..stream    import Fount, ResumeTaskStream, GenerationalWorkQueueStream, Sink
from ..workqueue import MkWorkQueue

import argparse


def build_parser(p):
    p.add_argument('-p', '--port', default=9123, type=int, help='Work Queue port')
    p.add_argument('-r', '--replicate', default=1, type=int, help='Task replication')
    p.add_argument('-d', '--debug', action='store_true', help='Turn on debugging information')
    p.add_argument('-t', '--timeout', default=1, type=int, help='Timeout in seconds when waiting for a task')
    p.add_argument('-l', '--logfile', default=None, help='Write the workqueue log to this file')



class TaskFount(Fount):
    def set_state(self, state):
        self._state = state

    def generate(self):
        return iter(self._state.values())

def main(opts):

    mkq = (
        MkWorkQueue()
        .port(opts.port)
        .replicate(opts.replicate)
        )
    if opts.debug: mkq.debug_all()
    if opts.logfile:
        mkq.logfile(opts.logfile)
        # add a comment to make numpy loading simpler
        # +FIXME: should be moved to pwq
        with open(opts.logfile, 'a') as fd: fd.write('#')

    q = mkq()

    cfg = state.Config.load()
    with state.State.load() as st:
        fount = TaskFount()
        fount.set_state(st)
        persist = ResumeTaskStream(fount, st.store)
        submit = GenerationalWorkQueueStream(q, persist, timeout=opts.timeout,
                                             persist_to=st.store,
                                             generations=cfg.generations)
        sink = Sink(submit)
        sink()
