"""
Prepare files that have been added to the mdq project
"""

from .. import state
from ..md import gmx

import os

def build_parser(p):
    pass

def main(opts):
    cfg = state.Config.load()
    prep = gmx.Prepare(
        picoseconds    = cfg.time,
        outputfreq     = cfg.outputfreq,
        cpus           = cfg.cpus,
        mdrun          = cfg.binary('mdrun'),
        guamps_get     = cfg.binary('guamps_get'),
        guamps_set     = cfg.binary('guamps_set'),
        keep_trajfiles = True,
        )

    with state.State(state.STATE) as st:

        for h in cfg.sims:
            spec  = cfg.sims[h]
            st[h] = prep.task(
                spec['tpr'],
                x         = spec.get('x'),
                v         = spec.get('v'),
                t         = spec.get('t'),
                outputdir = os.path.join(state.SIMS, h),
                seed      = spec['seed'],
                digest    = h
                )

