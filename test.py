#!/usr/bin/python

import mdprep
mdprep.log.debug()

from mdq.md import gmx

g = gmx.GMX('tests/data', binaries='.')
g()
