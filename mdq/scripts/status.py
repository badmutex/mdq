"""
Summarize the progress of the simulations
"""

from .. import state
from pxul.logging import logger

def build_parser(p):
    pass


def main(opts):
    cfg = state.Config.load()
    st  = state.State.load()

    for h in sorted(st.keys()):
        logger.info('{:.<30s} {} / {}'.format(
            cfg.aliases[h],
            st[h].generation + 1,
            cfg.generations,
            )
        )
