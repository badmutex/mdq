"""
Summarize the progress of the simulations
"""

from .. import state
from ..log import logger

def build_parser(p):
    pass


def main(opts):
    cfg = state.Config.load()
    st  = state.State.load()

    for h in st.keys():
        logger.info('%+10s' % cfg.aliases[h],
                    '%s'    % (st[h].generation + 1),
                    '/ %s'  % cfg.generations
                    )
