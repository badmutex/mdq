#!/usr/bin/python

import pxul
from pwq import MkWorkQueue
from mdq.md import queue, gmx
from mdq.state import Config


if __name__ == '__main__':

    # pxul.logging.set_debug()

    mkq = (
        MkWorkQueue()
        .port(9123)
        .replicate(1)
        .debug_all()
    )

    cfg = Config(backend='gromacs', generations=2, time=1,
                 outputfreq=0.01, cpus=8, binaries='binaries')
    cfg.persist_to('config.mdq')

    q = queue.MD(mkq(), configfile='config.mdq', statefile='state.mdq')
    for i in xrange(10):
        spec = gmx.Spec(name='test.%s' % i, tpr='tests/data/topol.tpr')
        q.add(spec)
    q()
