#!/usr/bin/python

import mdprep
mdprep.log.debug()

from mdq.md import gmx

g = gmx.GMX('tests/data', binaries='binaries', time=1)
t = g()

from mdq.workqueue import MkWorkQueue
m = MkWorkQueue()
(
    m
    .port(9123)
    .debug_all()
    .logfile()
)
q = m()
q.submit(t)

while not q.empty():
    t = q.wait(5)
    if not t: continue
        
