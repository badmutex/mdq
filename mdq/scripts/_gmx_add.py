"""
Add input files to a project.

Each time `add` is called, the parameters and the contents
of any provided files are hashed so that only new files are
actually added.
"""

from .. import state

import random

def build_parser(p):
    p.add_argument('-T', '--tpr', required=True)
    p.add_argument('-X', '--traj',
                   help='If given, continue the trajectory from the final position, velocities, and time')
    p.add_argument('-x', '--positions' , help='If given, use the positions from this GUAMPS vector file')
    p.add_argument('-v', '--velocities', help='If given, use the velocities from this GUAMPS vector file')
    p.add_argument('-t', '--time', type=float, default=None, help='If given, start the simulation at this time')


def main(opts):
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

