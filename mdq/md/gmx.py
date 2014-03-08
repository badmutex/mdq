from .. import workqueue
import mdprep
import collections
import os
import random
import subprocess
import textwrap
import tempfile
import uuid

BINARY_NAMES        = ['guamps_get',
                       'guamps_set',
                       'mdrun']

TRAJ_FILES          = ['traj.trr',
                       'traj.xtc',
                       'ener.edr'
                       'md.log']

SELECTIONS          = dict(positions='x', velocities='v',time='t')
SCRIPT_INPUT_NAMES  = dict(x = 'x_i.gps', v = 'v_i.gps', t = 't_i.gps')
SCRIPT_OUTPUT_NAMES = dict(x = 'x_o.gps', v = 'v_o.gps', t = 't_o.gps')

SCRIPT = textwrap.dedent("""\
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


executables = [
      'guamps_get'
    , 'guamps_set'
    , 'mdrun'
]

LOGFILE = 'task.log'

def get_nsteps(path):
    suffix = os.path.splitext(path)[-1]
    suffix = suffix.lower()
    if suffix == '.tpr':
        cmd = 'guamps_get -f %(tpr)s -s nsteps' % dict(tpr=path)
    elif suffix == '.mdp':
        cmd = "egrep '^ *nsteps *=' %(mdp)s | awk '{print $3}'" % dict(mdp=path)
    else:
        raise Exception, '%s has unknown filetype %s' % (path, suffix)

    out = subprocess.check_output(cmd, shell=True).strip()
    nsteps = int(out)
    return nsteps

def which(exes, search=None):
    """
    Attemps to locate the given executable names in the provides search paths
    """

    search = search if search is not None else os.environ['PATH'].split(os.pathsep)
    found  = collections.defaultdict(lambda: False)
    for prefix in search:
        for name in exes:
            path = os.path.join(prefix, name)
            if os.path.exists(path) and os.access(path, os.X_OK):
                found[name] = path
                continue

    assert set(found.keys()).difference(set(exes)) == set()
    if not all(found.values()): raise Exception
    return dict(found)

guamps_get = mdprep.process.optcmd('guamps_get')
guamps_set = mdprep.process.optcmd('guamps_set')

class GMX(object):

    def __init__(self, workarea='.', out='mdq', generations=1,
                 binaries=None,
                 transfer_traj=True,
                 time=None, outfreq=1000,
                 nprocs=None,
                 gro='conf.gro',
                 top='topol.top',
                 itp='posre.itp',
                 mdp='grompp.mdp',
                 tpr='topol.tpr'):

        assert binaries is not None
        # input parameters
        self._workarea     = workarea
        self._out          = out
        self._gens         = generations
        self._transfer_traj=transfer_traj
        self._binaries     = binaries
        self._time         = time if time is not None else -1
        self._outfreq      = outfreq
        self._nprocs       = nprocs if nprocs is not None else 0
        self._gro          = gro
        self._top          = top
        self._itp          = itp
        self._mdp          = mdp
        self._tpr          = tpr

        # properties / attributes
        self._uuid         = uuid.uuid1()
        self._gen          = 0 # current generation
        self._ok           = False # current generation is successfull

    def _set_seed(self, seed):
        mdp = mdprep.load(self._mdp)
        mdp.seed(seed)
        mdp.save(self._mdp)

    def _mk_tpr(self):
        with mdprep.util.StackDir(self._workarea), mdprep.gmx.NoAutobackup():
            mdprep.grompp(f=self._mdp,
                          c=self._gro,
                          p=self._top,
                          o=self._tpr
                          )

    def _set_tpr_scalar_params(self, selection, value):
        with mdprep.util.StackDir(self._workarea), tempfile.NamedTemporaryFile() as tmp:
            tmp.write('%s' % value)
            tmp.flush()
            guamps_set(f=self._tpr,
                       s=selection,
                       i=tmp.name,
                       O=True
                       )

    def _get_tpr_scalar(self, selection, typed):
        with mdprep.util.StackDir(self._workarea), tempfile.NamedTemporaryFile() as tmp:
            guamps_get(f=self._tpr,
                       s=selection,
                       o=tmp.name,
                       )
            tmp.seek(0)
            return typed(tmp.readline())

    def _set_tpr(self):
        dt = self._get_tpr_scalar('deltat', float)
        if self._time > 0:
            nsteps = int(self._time / dt)
        else:
            nsteps = -1
        self._set_tpr_scalar_params('nsteps', nsteps)
        for sel in 'nstxout nstvout nstfout nstlog nstxtcout'.split():
            freq = int(self._outfreq / dt)
            self._set_tpr_scalar_params(sel, freq)

    def _reseed(self):
        self._set_tpr_scalar_params('ld_seed', random.randint(1, 10**10))

    def _write_task_files(self):
        tpr = os.path.abspath(os.path.join(self._workarea, self._tpr))
        with mdprep.util.StackDir(self.current_simdir):
            for selection, name in SELECTIONS.iteritems():
                guamps_get(f=tpr,
                           s=selection,
                           o=SCRIPT_INPUT_NAMES[name]
                           )

    def _init_prepare(self):
        """
        Prepare for the first simulation
        """
        self._mk_tpr()
        self._set_tpr()
        self._reseed()
        self._write_task_files()

    def _prepare(self, task):
        if task is None:
            assert self._gen == 0
            self._init_prepare()
        else:
            assert self._gen > 0
            assert task.result == 0
            assert task.return_status == 0
            assert task.tag == self.id

        t = workqueue.Task('bash md.sh >%s 2>&1' % LOGFILE)
        t.specify_buffer(SCRIPT, 'md.sh', cache=True)
        t.specify_buffer(str(self._nprocs), 'processors.gps', cache=True)
        open(os.path.join(self.current_simdir, 'md.sh'), 'w').write(SCRIPT)

        # cache tpr
        t.specify_input_file(os.path.join(self._workarea, self._tpr), cache=True)

        # don't cache input files
        for n in SCRIPT_INPUT_NAMES.itervalues():
            local  = os.path.join(self.previous_simdir, n)
            remote = n
            t.specify_input_file(local, remote, cache=False)
            print local, '->', remote

        # don't cache output files
        for n in SCRIPT_OUTPUT_NAMES.values() + [LOGFILE]:
            local  = os.path.join(self.current_simdir, n)
            remote = n
            t.specify_output_file(local, remote, cache=False)
            print local, '<-', remote

        # cache the binaries
        for n in BINARY_NAMES:
            local  = os.path.join(self._binaries, '$OS', '$ARCH', n)
            remote = n
            t.specify_input_file(local, remote, cache=True)

        # tag with uuid
        t.specify_tag(str(self._uuid))

        return t

    def _gendir(self, gen):
        """Get the simulation directory of the specified `gen`"""
        assert gen >= 0
        return os.path.join(self._workarea, self._out, str(gen))

    def incr(self):
        """Increment the generation count"""
        self._gen += 1

    @property
    def id(self): return self._uuid

    @property
    def gen(self):
        """The current generation"""
        return self._gen

    @property
    def current_simdir(self):
        """
        Get the simulation directory of the current `gen`
        """
        return self._gendir(self._gen)

    @property
    def previous_simdir(self):
        """
        Get the simulation directory of the previous `gen`
        """
        assert self._gen >= 0
        g = self._gen if self._gen == 0 else self._gen - 1
        return self._gendir(g)

    def __call__(self, task=None):
        """
        task: the WQ Task of the previous generation
        """
        return self._prepare(task)




if __name__ == '__main__':
    g = GMX('tests/data', generations=2)
    print g.tag
