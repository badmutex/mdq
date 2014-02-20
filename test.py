#!/usr/bin/python

import wqmd

t = wqmd.Task('echo hello world', generations=10)
for n in 'one two three'.split():
    t.add_input(n)

for i in xrange(10):
    print t.label
    t._to_task()
    t._incr()
