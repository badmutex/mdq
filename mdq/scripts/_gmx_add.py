from .. import state
from ..md import gmx

import argparse
import random

def getopts():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-T', '--tpr', required=True)
    p.add_argument('-X', '--traj',
                   help='If given, continue the trajectory from the final position, velocities, and time')
    p.add_argument('-x', '--positions' , help='If given, use the positions from this GUAMPS vector file')
    p.add_argument('-v', '--velocities', help='If given, use the velocities from this GUAMPS vector file')
    p.add_argument('-t', '--time', type=float, default=None, help='If given, start the simulation at this time')

    return p.parse_args()


def main():
    opts = getopts()
    cfg  = state.Config.load()
    if cfg.seed is not None:
        random.seed(cfg.seed)

    spec = state.Spec()

    spec['tpr'] = opts.tpr
    spec['seed'] = random.randint(1, 10**5)
    if opts.traj       is not None: spec['traj'] = opts.traj
    if opts.positions  is not None: spec['x'] = opts.positions
    if opts.velocities is not None: spec['v'] = opts.velocities
    if opts.time       is not None: spec['t'] = opts.time


    cfg.add(spec)
    cfg.write()

