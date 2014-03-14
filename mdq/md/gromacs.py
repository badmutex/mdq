from .. import command
from .. import stream

import mdprep

import os
import random
import textwrap
import uuid

BINARY_NAMES        = ['guamps_get',
                       'guamps_set',
                       'mdrun']

TRAJ_FILES          = ['traj.trr',
                       'traj.xtc',
                       'ener.edr',
                       'md.log']

SELECTIONS          = dict(positions='x', velocities='v',time='t')
FILE_NAMES          = dict(x = 'x.gps'  , v = 'v.gps'  , t = 't.gps')
SCRIPT_INPUT_NAMES  = dict(x = 'x_i.gps', v = 'v_i.gps', t = 't_i.gps')
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

# output files
x_o=%(x_o)s
v_o=%(v_o)s
t_o=%(t_o)s

# disable gromacs automatic backups
export GMX_MAXBACKUP=-1

# continue from previous positions, velocities, and time
guamps_set -f topol.tpr -s positions  -i $x_i
guamps_set -f topol.tpr -s velocities -i $v_i
guamps_set -f topol.tpr -s time       -i $t_i

# run with given number of processors
mdrun -nt $(cat processors.gps)

# retrieve the positions, velocities, and time
guamps_get -f traj.trr -s positions  -o $x_o
guamps_get -f traj.trr -s velocities -o $v_o
guamps_get -f traj.trr -s time       -o $t_o

""" % dict(
    x_i = SCRIPT_INPUT_NAMES ['x'],
    v_i = SCRIPT_INPUT_NAMES ['v'],
    t_i = SCRIPT_INPUT_NAMES ['t'],
    x_o = SCRIPT_OUTPUT_NAMES['x'],
    v_o = SCRIPT_OUTPUT_NAMES['v'],
    t_o = SCRIPT_OUTPUT_NAMES['t'],
    )
)


EXECUTABLES = [
      'guamps_get'
    , 'guamps_set'
    , 'mdrun'
]




pdb2gmx    = mdprep.gmx.pdb2gmx
editconf   = mdprep.gmx.editconf
grompp     = mdprep.gmx.grompp
genion     = mdprep.gmx.genion
genbox     = mdprep.gmx.genbox
mdrun      = mdprep.gmx.mdrun
guamps_get = mdprep.process.OptCommand('guamps_get')
guamps_set = mdprep.process.OptCommand('guamps_set')


class SimulationUnit(stream.Unique):
    """
    This represents everything needed to run a simulation.
    """

    def __init__(self,
                 x=None, v=None, t=None, tpr=None,
                 binaries=None, outputdir='mdq', seed=None, cpus=None, verbose=False
                 ):

        super(SimulationUnit, self).__init__()

        # simulation data
        self._x = x # positions
        self._v = v # velocity
        self._t = t # start time
        self._tpr = tpr

        # for `running` the simulation
        self._binaries = binaries
        self._outputdir = outputdir
        self._seed = seed if seed is not None else random.randint(1, 10**10)
        self._cpus = cpus if cpus is not None else 0
        self._verbose = verbose

        # results to transfer back
        self._save_trr = 'traj.trr'
        self._save_xtc = 'traj.xtc'
        self._save_edr = 'ener.edr'
        self._save_log = 'md.log'

    def _save_XXX_default(self, suffix):
        return dict(trr = 'traj.trr',
                    xtc = 'traj.xtc',
                    edr = 'ener.edr',
                    log = 'md.log')[suffix]

    def _save_XXX(self, suffix, save):
        attr = '_save_%s' % suffix
        curr = getattr(self, attr)

        # if save is None: toggle
        # if save is bool: default or no change
        # if save is str : set value

        if save is None and curr:
            name = None

        elif save is None and not curr:
            name = self._save_XXX_default(suffix)

        elif isinstance(save, bool) and save:
            name = self._save_XXX_default(suffix)

        elif isinstance(save, bool) and not save:
            name = curr

        elif isinstance(save, str):
            ext = os.path.splitext(save)[-1][1:] # [-1]: get extension, [1:]: drop period from extension
            assert ext == suffix, 'Unexpected suffix for %s: got %s, expected %s' % (save, ext, suffix)
            name = save

        else: raise ValueError, 'Unknown value %s of type %s' % (save, type(save))

        setattr(self, attr, name)


    def save_trr(self, save=None): self._save_XXX('trr', save)
    def save_xtc(self, save=None): self._save_XXX('xtc', save)
    def save_edr(self, save=None): self._save_XXX('edr', save)
    def save_log(self, save=None): self._save_XXX('log', save)


    def _add_save_files_to_task(self, task):
        types = 'trr xtc edr log'.split()
        attrs = map(lambda suf: '_save_%s' % suf, types)
        for a in attrs:
            remote = getattr(self, a)
            if remote:
                local = os.path.join(self._outputdir, remote)
                task.specify_output_file(local, remote, cache=False)

    def task(self):
        """Create a WorkQueue Task"""

        from .. import workqueue
        task = workqeue.Task('bash %s > %s' % (SCRIPT_NAME, LOGFILE))
        self._add_save_files_to_task(task)

    def run(self):
        tpr = os.path.abspath(self._tpr)
        with mdprep.util.StackDir(self._outputdir), mdprep.gmx.NoAutobackup():
            self._binaries.mdrun(s=tpr, nt=self._cpus, v=self._verbose)

class SimulationGeneration(object):
    def __init__(self, generations=1):
        self._generations = generations
        self._units = dict() # unit uuid -> unit
        self._gens  = dict() # unit uuid -> <int>



class SimulationEngine(object):
    def __init__(self, q, generations=1):
        self._q = q
        self._generations = generations
        self._units = dict() # unit uuid -> unit





if __name__ == '__main__':

    class Binaries(object):
        def __init__(self):
            self.mdrun = mdprep.mdrun

    mdprep.log.debug()
    sim = SimulationUnit(tpr='tests/data/topol.tpr', outputdir='/tmp/mdq', verbose=True, binaries=Binaries())
    sim.run()
