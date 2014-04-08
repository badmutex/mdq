
from __future__ import absolute_import
from ..stream import Fount, Stream, ResumeTaskStream, GenerationalWorkQueueStream, Sink
from .. import state

from pxul.logging import logger

class Type:
    GROMACS = 0


class TaskFount(Fount):
    """
    :: Fount Spec
    """
    def __init__(self):
        super(TaskFount, self).__init__()
        self._specs = list()

    def add(self, spec):
        self._specs.append(spec)

    def generate(self):
        logger.debug('%s: Generating' % self.__class__.__name__)
        for spec in self._specs:
            logger.debug('%s: Processing %s' % (self.__class__.__name__, spec.digest))
            yield spec

class AddStream(Stream):
    """
    :: Stream Spec -> Stream Spec

    Update the configuration with the task specifications
    """
    def __init__(self, fount, cfg, cfgfile):
        super(AddStream, self).__init__(fount)
        self._cfg = cfg
        self._cfgfile = cfgfile

    def process(self, spec):
        logger.debug('%s: Processing %s' % (self.__class__.__name__, spec.digest))
        cfg = self._cfg
        spec.update_digest()
        cfg.add(spec)
        cfg.seed = spec.seed
        cfg.alias(spec.digest, spec['name'])
        yield spec

    def __iter__(self):
        for v in super(AddStream, self).__iter__():
            yield v
        self._cfg.write(self._cfgfile)

class PrepareStream(Stream):
    """
    :: Stream Spec -> Stream Task
    """
    def __init__(self, fount, cfg, st):
        super(PrepareStream, self).__init__(fount)

        if cfg.backend == 'gromacs':
            from mdq.md.gmx import Prepare
        else: raise ValueError('Unknown MD backend: %s' % cfg.backend)

        self._prepare = Prepare.from_config(cfg)
        self._cfg = cfg
        self._st = st

    def process(self, spec):
        logger.debug('%s: Processing %s' % (self.__class__.__name__, spec.digest))
        st = self._st
        h = spec.digest
        if h not in st:
            st[h] = self._prepare.task_from_spec(spec)
        yield st[h]



class MD(object):
    def __init__(self, wq, configfile='config.mdq', statefile='state.mdq'):
        """
        Create a queue to submit MD Tasks to.

        Params:
          - wq :: pwq.WorkQueue
          - backend :: str : one of {'gromacs'}
        """
        self._wq = wq
        self._configfile = configfile
        self._statefile = statefile

        self._cfg = state.Config.load(configfile)
        self._st  = state.State.load(statefile)

        self._fount = TaskFount()

    def add(self, spec):
        self._fount.add(spec)

    def __call__(self, timeout=1, outputdir='mdq-sims'):
        with state.State.load(self._statefile) as st:
            fount = self._fount
            add = AddStream(fount, self._cfg, self._configfile)
            prepare = PrepareStream(add, self._cfg, st)
            persist = ResumeTaskStream(prepare, st.store)
            gens = GenerationalWorkQueueStream(self._wq, persist,
                                               timeout     = timeout,
                                               persist_to  = st.store,
                                               generations = self._cfg.generations
                                               )
            sink = Sink(gens)
            sink()
