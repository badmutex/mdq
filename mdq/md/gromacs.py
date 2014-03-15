from .. import command
from .. import stream
from .. import workqueue as wq

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


class Task(stream.Unique):
    """
    This represents everything needed to run a simulation.
    """

    def __init__(self,
                 x=None, v=None, t=None, tpr=None,
                 outputdir='mdq', cpus=1
                 ):

        super(Task, self).__init__()

        self._x         = x # positions
        self._v         = v # velocity
        self._t         = t # start time
        self._tpr       = tpr
        self._outputdir = outputdir
        self._cpus      = cpus

        self._generation = 0

    @property
    def input_files(self):
        return dict(x=self._x, v=self._v, t=self._t, tpr=self._tpr)

    @property
    def output_files(self):
        return dict(x = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['x']),
                    v = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['v']),
                    t = os.path.join(self.outputdir, SCRIPT_OUTPUT_NAMES['t']),
                    )

    def extend(self):
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

    def to_task(self):
        mdprep.util.ensure_dir(self.outputdir)

        cmd = 'bash %(script)s > %(log)s' % dict(script = SCRIPT_NAME, log = LOGFILE)
        task = wq.Task(cmd)

        # input files
        task.specify_input_file(self._x  , SCRIPT_INPUT_NAMES['x']  , cache=False, named='x_i')
        task.specify_input_file(self._v  , SCRIPT_INPUT_NAMES['v']  , cache=False, named='v_i')
        task.specify_input_file(self._t  , SCRIPT_INPUT_NAMES['t']  , cache=False, named='t_i')
        task.specify_input_file(self._tpr, SCRIPT_INPUT_NAMES['tpr'], cache=True , named='tpr')

        # output files
        task.specify_output_file(self.output_files['x'], SCRIPT_OUTPUT_NAMES['x']  , cache=False, named='x_o')
        task.specify_output_file(self.output_files['v'], SCRIPT_OUTPUT_NAMES['v']  , cache=False, named='v_o')
        task.specify_output_file(self.output_files['t'], SCRIPT_OUTPUT_NAMES['t']  , cache=False, named='t_o')

        return task
