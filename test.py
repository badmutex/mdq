#!/usr/bin/python

import mdprep
mdprep.log.debug()

from mdq.md import gmx

g = gmx.GMX('tests/data', binaries='binaries', time=2, cpus=1)

from mdq.workqueue import MkWorkQueue

m = MkWorkQueue()
(
    m
    .port(9123)
    .debug_all()
    .logfile()
    .replicate(8)
    .generations(9)
)
q = m()

q.submit(g)

while not q.empty():
    q.replicate()
    r = q.wait(5)
    if r and r.result == 0:
        print r



print 'Done'
