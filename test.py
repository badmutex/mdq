#!/usr/bin/python

import pxul
from pwq import MkWorkQueue
from mdq import mdqueue
from mdq.md import gmx
from mdq.state import Config


if __name__ == '__main__':

    pxul.logging.set_debug()

    mkq = (
        MkWorkQueue()
        .port(9123)
        .replicate(8)
        .debug_all()
    )

    cfg = Config(generations=2, time=5, outputfreq=0.01, cpus=8, binaries='binaries')
    cfg.write('config.mdq')

    q = mdqueue.MDQueue(mkq)
    spec = gmx.Spec(name='test', tpr='tests/data/topol.tpr')
    q.submit(spec)
    q()
