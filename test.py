#!/usr/bin/python

import work_queue as ccl

import mdprep
mdprep.log.debug()

from mdq.md import gmx
import mdq._wq as _wq

g = gmx.GMX('tests/data', binaries='binaries', time=2, cpus=1)
t = ccl.Task('ls')
t = g()

from mdq.workqueue import MkWorkQueue

m = MkWorkQueue()
(
    m
    .port(9123)
    # .debug_all()
    .logfile()
    .replicate(8)
)
q = m()

q.submit(t)

while not q.empty():
    q.replicate()
    r = q.wait(5)
    if r and r.result == 0:
        print r



print 'Done'
