"""
Initialize an mdq project

An MD backend must be specified (eg, `gromacs`).
"""

from .. import state

import os.path

def build_parser(p):
    p.add_argument('backend', choices=['gromacs'], help='The backend type')
    p.add_argument('-g', '--generations', default=float('inf'), type=int,
                    help='Number of generations to run')
    p.add_argument('-t', '--time', default=1000, type=int, help='Number of picoseconds to run each generation')
    p.add_argument('-o', '--outputfreq', default=500, type=float,
                   help='Output frequency in picoseconds')
    p.add_argument('-c', '--cpus', default=1, type=int, help='Number of CPUs to run each simulation')
    p.add_argument('-b', '--binaries', default='binaries',
                   help='Where to find the OS and ARCH -dependent files')
    p.add_argument('-s', '--seed', default=None, help='Seed the random number generator with this value')

def main(opts):

    if os.path.exists(state.CONFIG):
        cfg = state.Config.load(path=state.CONFIG)
    else:
        cfg = state.Config()

    cfg.update(
        backend     = opts.backend,
        generations = opts.generations,
        time        = opts.time,
        outputfreq  = opts.outputfreq,
        cpus        = opts.cpus,
        binaries    = opts.binaries,
        seed        = opts.seed
        )

    cfg.write()
