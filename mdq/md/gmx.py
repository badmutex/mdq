from .  import api
from .. import state
from .. import stream
from .. import workqueue as wq

import pxul
from pxul.logging import logger
from pxul.os import SetEnv

import mdprep

import hashlib
import os
import random
import tempfile
import shutil
import textwrap


EXECUTABLES = [
      'guamps_get'
    , 'guamps_set'
    , 'mdrun'
]


TRAJ_FILES          = dict(trr='traj.trr',
                           xtc='traj.xtc',
                           edr='ener.edr',
                           log='md.log'  )

SELECTIONS          = dict(positions='x', velocities='v',time='t')
FILE_NAMES          = dict(x = 'x.gps'  , v = 'v.gps'  , t = 't.gps')
SCRIPT_INPUT_NAMES  = dict(x = 'x_i.gps', v = 'v_i.gps', t = 't_i.gps', tpr='topol.tpr', cpus='cpus.gps')
SCRIPT_OUTPUT_NAMES = dict(x = 'x_o.gps', v = 'v_o.gps', t = 't_o.gps')

SCRIPT_NAME = 'md.sh' # name of the script on the worker
LOGFILE = 'task.log'  # log of the task run

SCRIPT_CONTENTS = textwrap.dedent("""\
#!/usr/bin/env bash
# exit if any command fails
set -o errexit 

export PATH=$PWD:$PATH

# input files
x_i=%(x_i)s
v_i=%(v_i)s
t_i=%(t_i)s
tpr=%(tpr)s
cpus=%(cpus)s

# output files
x_o=%(x_o)s
v_o=%(v_o)s
t_o=%(t_o)s

# disable gromacs automatic backups
export GMX_MAXBACKUP=-1

# continue from previous positions, velocities, and time
guamps_set -f $tpr -s positions  -i $x_i
guamps_set -f $tpr -s velocities -i $v_i
guamps_set -f $tpr -s time       -i $t_i

# run with given number of processors
mdrun -nt $(cat $cpus) -s $tpr

# retrieve the positions, velocities, and time
guamps_get -f traj.trr -s positions  -o $x_o
guamps_get -f traj.trr -s velocities -o $v_o
guamps_get -f traj.trr -s time       -o $t_o

""" % dict(
    x_i = SCRIPT_INPUT_NAMES ['x'],
    v_i = SCRIPT_INPUT_NAMES ['v'],
    t_i = SCRIPT_INPUT_NAMES ['t'],
    tpr = SCRIPT_INPUT_NAMES ['tpr'],
    cpus= SCRIPT_INPUT_NAMES ['cpus'],
    x_o = SCRIPT_OUTPUT_NAMES['x'],
    v_o = SCRIPT_OUTPUT_NAMES['v'],
    t_o = SCRIPT_OUTPUT_NAMES['t'],
    )
)




pdb2gmx    = mdprep.gmx.pdb2gmx
editconf   = mdprep.gmx.editconf
grompp     = mdprep.gmx.grompp
genion     = mdprep.gmx.genion
genbox     = mdprep.gmx.genbox
mdrun      = mdprep.gmx.mdrun
trjcat     = pxul.command.OptCommand('trjcat')
guamps_get = pxul.command.OptCommand('guamps_get')
guamps_set = pxul.command.OptCommand('guamps_set')


def disable_gromacs_backups():
    """
    Intended to be used in a `with` statement:
    eg.
    >>> with disable_gromacs_backups():
    ...    ...
    """
    return SetEnv(GMX_MAXBACKUP=-1)

def tpr_set_scalar(tpr, name, value):
    logger.info1('Setting', name, '=', value, 'in', tpr)
    tmpfile = os.path.join(os.path.dirname(tpr), name +'.gps')
    with open(tmpfile, 'w') as fd: fd.write('%s\n' % value)
    guamps_set(f=tpr, s=name, i=tmpfile, O=True)
    os.unlink(tmpfile)

def tpr_get_scalar(tpr, name, mktype):
    logger.info1('Getting', name, 'from', tpr)
    tmpfile = os.path.join(os.path.dirname(tpr), name+'.gps')
    guamps_get(f=tpr, s=name, o=tmpfile)
    with open(tmpfile) as fd:
        value = fd.readline()
        value = mktype(value)
    os.unlink(tmpfile)
    return value


class Spec(api.Spec):
    def __init__(self, name, tpr='topol.tpr'):
        """
        Create the specification for a GROMACS simulation
        """
        super(Spec, self).__init__(name = name,
                                   tpr  = tpr,
                                   )


    def continue_traj(self, path):
        self['traj'] = path

    def continue_from(self, x='x.gps', v='v.gps', t='t.gps'):
        self['x'] = x
        self['v'] = v
        self['t'] = t

class Prepare(api.Preparable):
    def __init__(self,
                 picoseconds=None, outputfreq=None,
                 cpus=0, mdrun=None, guamps_get=None, guamps_set=None,
                 keep_trajfiles=True):
        self._picoseconds = picoseconds
        self._outputfreq = outputfreq
        self._cpus = cpus
        self._mdrun = mdrun
        self._guamps_get = guamps_get
        self._guamps_set = guamps_set
        self._keep_trajfiles = keep_trajfiles

    @classmethod
    def from_config(cls, cfg):
        return cls(
            picoseconds    = cfg.time,
            outputfreq     = cfg.outputfreq,
            cpus           = cfg.cpus,
            mdrun          = cfg.binary('mdrun'),
            guamps_get     = cfg.binary('guamps_get'),
            guamps_set     = cfg.binary('guamps_set'),
            keep_trajfiles = cfg.keep_trajfiles,
            )

    def task_from_spec(self, spec):
        return self.task(
            spec['tpr'],
            x         = spec.get('x'),
            v         = spec.get('v'),
            t         = spec.get('t'),
            outputdir = spec.get('outputdir', os.path.join('mdq-sims', spec['name'])),
            seed      = spec.seed,
            digest    = spec.digest,
            )

    def task(self, tpr, x=None, v=None, t=None, outputdir=None, seed=None, digest=None):
        outdir = outputdir or tpr + '.mdq'
        pxul.os.ensure_dir(outdir)
        logger.debug('Ensured', outdir, 'exists')

        tpr2 = os.path.join(outdir, 'topol.tpr')
        shutil.copy(tpr, tpr2)
        logger.debug(tpr, '->', tpr2)

        gendir = os.path.join(outdir, '0')
        pxul.os.ensure_dir(gendir)
        logger.debug('Ensured', gendir, 'exists')

        gps = dict(x = os.path.join(gendir, SCRIPT_INPUT_NAMES['x']),
                   v = os.path.join(gendir, SCRIPT_INPUT_NAMES['v']),
                   t = os.path.join(gendir, SCRIPT_INPUT_NAMES['t']))

        if x is not None:
            shutil.copy(x, gps['x'])
            logger.debug(x, '->', gps['x'])
        if v is not None:
            shutil.copy(v, gps['v'])
            logger.debug(v, '->', gps['v'])
        if t is not None:
            if type(t) is float:
                with open(gps['t'], 'w') as fd: fd.write(str(t))
                logger.debug('Wrote', t, 'to', gps['t'])
            elif type(t) is str:
                shutil.copy(t, gps['t'])
                logger.debug(t, '->', gps['t'])
            else: raise ValueError, 'Illegal state: invalid time spec %s' % t


        for sel, key in SELECTIONS.iteritems():
            logger.info1('Getting', sel, 'from', tpr2)
            guamps_get(f=tpr2, s=sel, o=gps[key])

        if seed:
            logger.info1('Setting seed', seed)
            tpr_set_scalar(tpr2, 'ld_seed', seed)

        dt = tpr_get_scalar(tpr2, 'deltat', float)
        if self._picoseconds:
            nsteps = int(self._picoseconds / dt)
            logger.info1('Running for', self._picoseconds, 'ps as', nsteps, 'nsteps')
            tpr_set_scalar(tpr2, 'nsteps', nsteps)

        if self._outputfreq:
            freq = int(self._outputfreq / dt)
            # FIXME nstenergy, see badi/guamps#27
            for attr in 'nstxout nstxtcout nstfout nstvout nstlog'.split():
                logger.info1('Setting output frequency', self._outputfreq,
                             'for', attr, 'as', freq, 'steps', 'in', tpr2)
                tpr_set_scalar(tpr2, attr, freq)

        if not digest:
            logger.info1('Computing digest for', tpr2)
            sha256 = hashlib.sha256()
            sha256.update(open(tpr2, 'rb').read())
            digest = sha256.hexdigest()

        task = Task(x=gps['x'], v=gps['v'], t=gps['t'], tpr=tpr2,
                    outputdir=outdir, cpus=self._cpus, digest=digest)

        task.add_binary(self._mdrun)
        task.add_binary(self._guamps_get)
        task.add_binary(self._guamps_set)

        if self._keep_trajfiles:
            task.keep_trajfiles()

        logger.info('Prepared', digest, 'from', tpr)
        for k in self.__dict__:
            logger.info(10*' ', k.lstrip('_'), '=', getattr(self, k))
        return task

class Task(stream.Unique, api.Taskable, api.Persistable, api.Extendable):
    """
    This represents everything needed to run a simulation.

    The following example runs three generations of a simulations.
    In order to run, the binaries (mdrun, guamps_{g,s}et) need to be in the PATH

    >>> import pxul
    >>> import mdq.workqueue as wq
    >>> import mdq.md.gromacs as gmx
    >>> sim = gmx.Task(x='tests/data/mdq/0/x.gps',
    ...                v='tests/data/mdq/0/v.gps',
    ...                t='tests/data/mdq/0/t.gps',
    ...                tpr='tests/data/topol.tpr',
    ...                )
    >>> sim.keep_trajfiles()
    >>> for name in gmx.EXECUTABLES:
    ...     sim.add_binary(mdq.os.find_in_path(name))
    >>> worker = wq.WorkerEmulator() # doctest:+ELLIPSIS
    WorkerEmulator working ...
    >>> for gen in xrange(3): # 3 generations
    ...   task = sim.to_task()
    ...   worker(task)
    ...   assert task.result == 0
    ...   sim.extend()

    """

    def __init__(self,
                 x='x.gps', v='v.gps', t='t.gps', tpr='topol.tpr',
                 outputdir=None, cpus=0, digest=None
                 ):

        super(Task, self).__init__()

        self._x         = x # positions
        self._v         = v # velocity
        self._t         = t # start time
        self._tpr       = tpr
        self._outputdir = outputdir if outputdir is not None else os.path.splitext(tpr)[0] + '.mdq'
        self._cpus      = cpus
        self._digest    = digest

        self._generation = 0
        self._binaries   = list()
        self._trajfiles  = list()

    def _keep_XXX(self, suffix):
        """Mark a simulation output file to be transferred back from the worker"""
        logger.debug('Task keeping', suffix, 'as', TRAJ_FILES[suffix])
        self._trajfiles.append(TRAJ_FILES[suffix])

    def keep_trr(self):
        """Keep the trajectory .trr file"""
        self._keep_XXX('trr')

    def keep_xtc(self):
        """Keep the trajecotry .xtc file"""
        self._keep_XXX('xtc')

    def keep_edr(self):
        """Keep the energy .edr file"""
        self._keep_XXX('edr')

    def keep_log(self):
        """Keep the simulation .log file"""
        self._keep_XXX('log')

    def keep_trajfiles(self):
        """Transfer back all the trajectory output files"""
        for suffix in TRAJ_FILES.keys():
            self._keep_XXX(suffix)

    def add_binary(self, path):
        """Add a binary file to cache"""
        logger.debug('Adding binary', path)
        self._binaries.append(path)

    def check_binaries(self):
        """Checks that the required executables (EXECUTABLES) have been added"""
        logger.debug('Checking that all executables were added')
        found    = 0
        notfound = list()
        for path in self._binaries:
            base = os.path.basename(path)
            for name in EXECUTABLES:
                if name == base:
                    logger.debug('Found', name, 'as', path)
                    found += 1
                    continue
                notfound.append(name)
        if not found == len(EXECUTABLES):
            raise ValueError, 'Binaries for %s were not added' % ', '.join(notfound)

    @property
    def input_files(self):
        """Input files for the simulation script"""
        return dict(x=self._x, v=self._v, t=self._t, tpr=self._tpr)

    @property
    def output_files(self):
        """Files needed to start the next generation"""
        return dict(x = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['x']),
                    v = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['v']),
                    t = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['t']),
                    log=os.path.join(self.outputdir, LOGFILE),
                    )

    @property
    def generation(self):
        """The current generation"""
        return self._generation

    @property
    def outputdir(self):
        """The output directory for the current generation"""
        return os.path.join(self._outputdir, str(self._generation))

    def output_path(self, name):
        """Return the local filename"""
        return os.path.join(self.outputdir, os.path.basename(name))

    ###################################################################### Implement the Persistable interface
    @property
    def digest(self):
        return self._digest

    ###################################################################### Implement the Extendteable interface
    def extend(self):
        """Set the file names to run the next generation"""
        logger.debug('Extending generation:', self._generation, '->', self._generation + 1)
        self._x = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['x'])
        self._v = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['v'])
        self._t = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['t'])
        self._generation += 1

    ###################################################################### Implement Taskable interface
    def to_task(self):
        logger.info1('Creating task for', self.digest)

        pxul.os.ensure_dir(self.outputdir)
        logger.debug('Ensured', self.outputdir, 'exists')

        cmd = 'bash %(script)s > %(log)s' % dict(script = SCRIPT_NAME, log = LOGFILE)
        task = wq.Task(cmd)

        # input files
        task.specify_buffer(SCRIPT_CONTENTS, SCRIPT_NAME              , cache=True)
        task.specify_buffer(str(self._cpus), SCRIPT_INPUT_NAMES['cpus'],cache=True)
        task.specify_input_file(self._x    , SCRIPT_INPUT_NAMES['x']  , cache=False, name='x_i')
        task.specify_input_file(self._v    , SCRIPT_INPUT_NAMES['v']  , cache=False, name='v_i')
        task.specify_input_file(self._t    , SCRIPT_INPUT_NAMES['t']  , cache=False, name='t_i')
        task.specify_input_file(self._tpr  , SCRIPT_INPUT_NAMES['tpr'], cache=True , name='tpr')

        # output files
        task.specify_output_file(self.output_files['log'], LOGFILE                 , cache=False, name='log')
        task.specify_output_file(self.output_files['x'], SCRIPT_OUTPUT_NAMES['x']  , cache=False, name='x_o')
        task.specify_output_file(self.output_files['v'], SCRIPT_OUTPUT_NAMES['v']  , cache=False, name='v_o')
        task.specify_output_file(self.output_files['t'], SCRIPT_OUTPUT_NAMES['t']  , cache=False, name='t_o')

        self.check_binaries()
        for path in self._binaries:
            task.specify_input_file(path, cache=True)

        for name in self._trajfiles:
            task.specify_output_file(self.output_path(name), name, cache=False)

        logger.debug('Created task:\n', str(task))

        return task

    def update_task(self, task): pass
