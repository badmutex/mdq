from .  import init, _gmx_add, _gmx_prepare, run, _gmx_cat, status
from .. import state
from .. import version

from pxul import logging as log

import argparse
import collections
import itertools

SUBCMDS = collections.OrderedDict()

def getopts():
    SUBCMDS['init'] = init
    SUBCMDS['add']     = _gmx_add
    SUBCMDS['prepare'] = _gmx_prepare
    SUBCMDS['run'] = run
    SUBCMDS['cat'] = _gmx_cat
    SUBCMDS['status']= status

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbosity', action='count', default=0, help='Increase verbosity')
    parser.add_argument('-V', '--version', action='version', version=version.full_version, help='Print version information and quit')

    subparsers = parser.add_subparsers(dest='command')

    for key, module in SUBCMDS.iteritems():
        p = subparsers.add_parser(key, description=module.__doc__,
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        module.build_parser(p)

    return parser.parse_args()


def main():
    opts = getopts()
    verbosity_levels = [log.set_info, log.set_info1, log.set_info2, log.set_debug]
    level = opts.verbosity if opts.verbosity < len(verbosity_levels) else -1
    verbosity_levels[level]()
    SUBCMDS[opts.command].main(opts)
