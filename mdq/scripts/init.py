from .. import state

import argparse

def getopts():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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

    return p.parse_args()

def main():
    opts = getopts()

    cfg = state.Config(
        backend=opts.backend,
        generations=opts.generations,
        time=opts.time,
        outputfreq=opts.outputfreq,
        cpus=opts.cpus,
        binaries=opts.binaries,
        seed=opts.seed
        )

    cfg.write()
