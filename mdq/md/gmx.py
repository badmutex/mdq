import mdprep

import collections
import os
import subprocess
import textwrap

script = textwrap.dedent("""\
#!/usr/bin/env bash
# exit if any command fails
set -o errexit 

# disable gromacs automatic backups
export GMX_MAXBACKUP=-1

continue from previous positions, velocities, and time
guamps_set -f topol.tpr -s positions  -i positions.gps
guamps_set -f topol.tpr -s velocities -i velocities.gps
guamps_set -f topol.tpr -s time       -i time.gps

# run with given number of processors
mdrun -nt $(cat processors.gps)

# retrieve the positions, velocities, and time
guamps_get -f traj.trr -s positions  -o x.gps
guamps_set -f traj.trr -s velocities -o v.gps
guamps_get -f traj.trr -s time       -o t.gps
""")


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


class GMX(object):

    def __init__(self, workarea='.', generation=1,
                 gro='conf.gro',
                 top='topol.top',
                 itp='posre.itp',
                 mdp='grompp.mdp'):

        self._workarea = workarea
        self._gens     = generations
        self._gro      = os.path.join(workarea, gro)
        self._top      = os.path.join(workarea, top)
        self._itp      = os.path.join(workarea, itp)
        self._mdp      = os.path.join(workarea, mdp)
        self._gensteps = None

    def _set_seed(self, seed):
        mdp = mdprep.load(self._mdp)
        mdp.seed(seed)
        mdp.save(self._mdp)

