from .  import api
from .. import command
from .. import stream
from .. import workqueue as wq

import mdprep

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
guamps_get = mdprep.process.OptCommand('guamps_get')
guamps_set = mdprep.process.OptCommand('guamps_set')


class Prepare(api.Preparable):
    def __init__(self,
                 cpus=0, mdrun=None, guamps_get=None, guamps_set=None,
                 keep_trajfiles=True):
        self._cpus = cpus
        self._mdrun = mdrun
        self._guamps_get = guamps_get
        self._guamps_set = guamps_set
        self._keep_trajfiles = keep_trajfiles

    def task(self, tpr, outputdir=None, seed=None):
        outdir = outputdir or tpr + '.mdq'
        mdprep.util.ensure_dir(outdir)

        tpr2 = os.path.join(outdir, 'topol.tpr')
        shutil.copy(tpr, tpr2)

        gendir = os.path.join(outdir, '0')
        mdprep.util.ensure_dir(gendir)

        gps = dict(x = os.path.join(gendir, SCRIPT_INPUT_NAMES['x']),
                   v = os.path.join(gendir, SCRIPT_INPUT_NAMES['v']),
                   t = os.path.join(gendir, SCRIPT_INPUT_NAMES['t']))

        for sel, key in SELECTIONS.iteritems():
            guamps_get(f=tpr2, s=sel, o=gps[key])

        if seed:
            seedfile = os.path.join(outdir, 'seed.gps')
            with open(seedfile, 'w') as fd: fd.write('%s\n' % seed)
            guamps_set(f=tpr2, s='ld_seed', i=seedfile, O=True)

        task = Task(x=gps['x'], v=gps['v'], t=gps['t'], tpr=tpr2,
                    outputdir=outdir, cpus=self._cpus)

        task.add_binary(self._mdrun)
        task.add_binary(self._guamps_get)
        task.add_binary(self._guamps_set)

        if self._keep_trajfiles:
            task.keep_trajfiles()

        return task

class Task(stream.Unique, api.Taskable, api.Extendable):
    """
    This represents everything needed to run a simulation.

    The following example runs three generations of a simulations.
    In order to run, the binaries (mdrun, guamps_{g,s}et) need to be in the PATH

    >>> import mdq.util
    >>> import mdq.workqueue as wq
    >>> import mdq.md.gromacs as gmx
    >>> sim = gmx.Task(x='tests/data/mdq/0/x.gps',
    ...                v='tests/data/mdq/0/v.gps',
    ...                t='tests/data/mdq/0/t.gps',
    ...                tpr='tests/data/topol.tpr',
    ...                )
    >>> sim.keep_trajfiles()
    >>> for name in gmx.EXECUTABLES:
    ...     sim.add_binary(mdq.util.find_in_path(name))
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
                 outputdir=None, cpus=0
                 ):

        super(Task, self).__init__()

        self._x         = x # positions
        self._v         = v # velocity
        self._t         = t # start time
        self._tpr       = tpr
        self._outputdir = outputdir if outputdir is not None else os.path.splitext(tpr)[0] + '.mdq'
        self._cpus      = cpus

        self._generation = 0
        self._binaries   = list()
        self._trajfiles  = list()

    def _keep_XXX(self, suffix):
        """Mark a simulation output file to be transferred back from the worker"""
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
        self._binaries.append(path)

    def check_binaries(self):
        """Checks that the required executables (EXECUTABLES) have been added"""
        found    = 0
        notfound = list()
        for path in self._binaries:
            base = os.path.basename(path)
            for name in EXECUTABLES:
                if name == base:
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
                    )

    def extend(self):
        """Set the file names to run the next generation"""
        self._x = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['x'])
        self._v = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['v'])
        self._t = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['t'])
        self._generation += 1

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

    ###################################################################### Implement Taskable interface
    def to_task(self):

        mdprep.util.ensure_dir(self.outputdir)

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
        task.specify_output_file(self.output_files['x'], SCRIPT_OUTPUT_NAMES['x']  , cache=False, name='x_o')
        task.specify_output_file(self.output_files['v'], SCRIPT_OUTPUT_NAMES['v']  , cache=False, name='v_o')
        task.specify_output_file(self.output_files['t'], SCRIPT_OUTPUT_NAMES['t']  , cache=False, name='t_o')

        self.check_binaries()
        for path in self._binaries:
            task.specify_input_file(path, cache=True)

        for name in self._trajfiles:
            task.specify_output_file(self.output_path(name), name, cache=False)

        return task

    def update_task(self, task): pass
