from .  import init, _gmx_add, _gmx_prepare, run
from .. import state

import argparse
import collections

SUBCMDS = collections.OrderedDict()

def getopts():
    SUBCMDS['init'] = init
    SUBCMDS['add']     = _gmx_add
    SUBCMDS['prepare'] = _gmx_prepare
    SUBCMDS['run'] = run

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest='command')

    for key, module in SUBCMDS.iteritems():
        p = subparsers.add_parser(key, description=module.__doc__)
        module.build_parser(p)

    return parser.parse_args()


def main():
    opts = getopts()
    SUBCMDS[opts.command].main(opts)
