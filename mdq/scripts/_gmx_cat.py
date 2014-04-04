"""
Concatenate trajectory files from multiple generations of a simulation
"""

from .. import state
from ..md import gmx

from pxul.logging import logger

import glob
import mdprep
import os

def build_parser(p):
    p.add_argument('-o', '--outputdir', default='sims', help='Output to here')
    p.add_argument('--xtc', action='store_true', help='Concat the .xtc files')
    p.add_argument('--trr', action='store_true', help='Concat the .trr files')


def list_traj_parts(prefix, suffix):
    return glob.glob(os.path.join(prefix, '*', '*'+suffix))

def cat_traj_parts(parts, out):
    logger.info('Writing', out, '\n' + '\n'.join(parts))
    gmx.trjcat(f = ' '.join(parts), o = out)

def cat(simdir, suffix, out):
    cat_traj_parts(list_traj_parts(simdir, suffix),
                   os.path.join(out, 'traj' + suffix))

def main(opts):
    hashes = state.State.load().keys()
    names  = state.Config.load().aliases
    suffixes = list()
    for h in  hashes:
        out = os.path.join(opts.outputdir, names[h])
        mdprep.util.ensure_dir(out)
        simdir = os.path.join(state.SIMS, h)
        if opts.xtc: suffixes.append('.xtc')
        if opts.trr: suffixes.append('.trr')
        for suffix in suffixes:
            with gmx.disable_gromacs_backups():
                cat(simdir, suffix, out)
