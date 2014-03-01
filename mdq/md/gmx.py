from .. import util
import mdprep
import collections
import os
import subprocess
import textwrap
import uuid

TRAJ_FILES          = ['traj.trr',
                       'traj.xtc',
                       'ener.edr'
                       'md.log']

SELECTIONS          = dict(positions='x', velocities='v',time='t')
script_input_names  = dict(x = 'x_i.gps', v = 'v_i.gps', t = 't_i.gps')
script_output_names = dict(x = 'x_o.gps', v = 'v_o.gps', t = 't_o.gps')

script = textwrap.dedent("""\
#!/usr/bin/env bash
# exit if any command fails
set -o errexit 

# disable gromacs automatic backups
export GMX_MAXBACKUP=-1

continue from previous positions, velocities, and time
guamps_set -f topol.tpr -s positions  -i %(x_i)s
guamps_set -f topol.tpr -s velocities -i %(v_i)s
guamps_set -f topol.tpr -s time       -i %(t_i)s

# run with given number of processors
mdrun -nt $(cat processors.gps)

# retrieve the positions, velocities, and time
guamps_get -f traj.trr -s positions  -o %(x_o)s
guamps_set -f traj.trr -s velocities -o %(v_o)s
guamps_get -f traj.trr -s time       -o %(t_o)s
""" % dict(
    x_i = script_input_names ['x'],
    v_i = script_input_names ['v'],
    t_i = script_input_names ['t'],
    x_o = script_output_names['x'],
    v_o = script_output_names['v'],
    t_o = script_output_names['t'],
    )
)


executables = [
      'guamps_get'
    , 'guamps_set'
    , 'mdrun'
]

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

class NoAutobackup(object):
    def __init__(self):
        self._k = 'GMX_MAXBACKUP'
        self._val = None

    def __enter__(self):
        if self._k in os.environ:
            self._val   = os.environ[self._k]
        os.environ[self._k] = '-1'

    def __exit__(self, *args, **kws):
        if self._val is not None:
            os.environ[self._k] = self._val

guamps_get = mdprep.process.optcmd('guamps_get')
guamps_set = mdprep.process.optcmd('guamps_set')

class GMX(object):

    def __init__(self, workarea='.', name='mdq', generations=1,
                 transfer_traj=True,
                 time=None, outfreq=1000,
                 gro='conf.gro',
                 top='topol.top',
                 itp='posre.itp',
                 mdp='grompp.mdp',
                 tpr='topol.tpr'):

        # input parameters
        self._workarea     = workarea
        self._name         = name
        self._gens         = generations
        self._transfer_traj=transfer_traj
        self._time         = time if time is not None else -1
        self._outfreq      = outfreq
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
        with mdprep.util.StackDir(self._workarea), NoAutobackup():
            mdprep.grompp(f=self._mdp,
                          c=self._gro,
                          p=self._top,
                          o=self._tpr
                          ).run()

    def _set_tpr_scalar_params(self, selection, value):
        with mdprep.util.StackDir(self._workarea), tempfile.NamedTemporaryFile() as tmp:
            tmp.write('%s' % value)
            tmp.flush()
            guamps_set(f=self._tpr,
                       s=selection,
                       i=tmp.name,
                       O=True
                       ).run()

    def _get_tpr_scalar(self, selection, typed):
        with mdprep.util.StackDir(self._workarea), tempfile.NamedTemporaryFile() as tmp:
            guamps_get(f=self._tpr,
                       s=selection,
                       o,tmp.name
                       ).run()
            tmp.seek(0)
            return typed(tmp.readline())

    def _set_tpr(self):
        dt = self._get_tpr_scalar('dt', float)
        if self._time > 0:
            nsteps = int(self._time / dt)
        else:
            nsteps = self._nsteps
        self._set_tpr_scalar_params('nsteps', nsteps)
        for sel in 'nstxout nstvout nstfout nstlog nstxtcout'.split():
            freq = int(self._outfreq / dt)
            self._set_tpr_scalar_params(sel, freq)

    def _reseed(self):
        self._set_tpr_scalar_params('ld_seed', random.randint(1, 10**10))

    def _write_inputs_from_tpr(self):
        tpr = os.path.abspath(self._tpr)
        with mdprep.util.StackDir(self.simdir):
            for selection, name in SELECTIONS.iteritems():
                guamps_get(f=tpr,
                           s=selection,
                           o=script_input_names[name]
                           ).run()

    def _init_prepare(self):
        """
        Prepare for the first simulation
        """
        self._mk_tpr()
        self._set_tpr()
        self._reseed()
        self._write_input_from_tpr()

    @property
    def id(self): return self._uuid

    @property
    def tag(self): return '%s.%d' % (self.id, self._gen)

    @property
    def simdir(self):
        """
        Get the simulation directory of the current `gen`
        """
        return os.path.join(self._workarea, self._name, str(self._gen))

    def __call__(self, task=None):
        """
        task: the WQ Task of the previous generation
        """
        self._prepare(task)
        t = self._mk_task()
        return t




if __name__ == '__main__':
    g = GMX('tests/data', generations=2)
    print g.tag
