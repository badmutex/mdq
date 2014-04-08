
from __future__ import absolute_import
from .stream import Fount, ResumeTaskStream, GenerationalWorkQueueStream, Sink
from . import state

from pxul.logging import logger

class Type:
    GROMACS = 0


class TaskFount(Fount):
    def __init__(self, st):
        super(TaskFount, self).__init__()
        self._st = st

    def generate(self):
        return iter(self._st.values())

class MDQueue(object):
    def __init__(self, mkq, configfile='config.mdq', statefile='state.mdq'):
        """
        Create a queue to submit MD Tasks to.

        Params:
          - wq :: pwq.WorkQueue
          - backend :: str : one of {'gromacs'}
        """
        self._mkq = mkq
        self._configfile = configfile
        self._statefile = statefile

        self._cfg = state.Config.load(configfile)
        self._st  = state.State.load(statefile)

    def submit(self, spec):
        spec.update_digest()
        self._cfg.add(spec)
        self._cfg.seed = spec.seed
        self._cfg.alias(spec.digest, spec['name'])
        self._cfg.write(self._configfile)

    def _prepare(self, st):
        cfg = self._cfg
        if cfg.backend == 'gromacs':
            from mdq.md.gmx import Prepare
        else: raise ValueError('Unknown MD backend: %s' % self._cfg.backend)

        prep = Prepare.from_config(self._cfg)
        for h in cfg.sims:
            if h in st: continue
            spec = cfg.sims[h]
            st[h] = prep.task_from_spec(spec)

    def __call__(self, timeout=1, outputdir='mdq-sims'):
        q = self._mkq()


        with state.State.load(self._statefile) as st:
            self._prepare(st)
            fount = TaskFount(st)
            persist = ResumeTaskStream(fount, st.store)
            gens = GenerationalWorkQueueStream(q, persist,
                                               timeout     = timeout,
                                               persist_to  = st.store,
                                               generations = self._cfg.generations
                                               )
            sink = Sink(gens)
            sink()
