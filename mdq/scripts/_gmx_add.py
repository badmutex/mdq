"""
Add input files to a project.

Each time `add` is called, the parameters and the contents
of any provided files are hashed so that only new files are
actually added.
"""

from .. import state
from ..md import gmx

import random

def build_parser(p):
    p.add_argument('-n', '--name', required=True, help='Provide a descriptive name for this simulation')
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

    spec = gmx.Spec(opts.name,
                    tpr = opts.tpr
                    )

    if opts.traj       is not None: spec['traj'] = opts.traj
    if opts.positions  is not None: spec['x'] = opts.positions
    if opts.velocities is not None: spec['v'] = opts.velocities
    if opts.time       is not None: spec['t'] = opts.time

    spec.update_digest()
    cfg.add(spec)
    cfg.seed = spec['seed']
    cfg.alias(spec.digest, opts.name)
    cfg.write()
