"""\
mdq: A library for running molecular dynamics simulations using WorkQueue

This library provides an API for running molecular dynamics simulations
using different MD backends, such as GROMACS, NAMD, and others.
"""

DOCLINES = __doc__.split('\n')


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import glob
import os
import subprocess
import sys
import textwrap

###################################################################### prepare kw arguments to `setup`
setup_kws = dict()

###################################################################### python dependencies
dependencies = ['pwq', 'mdprep']
if 'setuptools' in sys.modules:
    setup_kws['install_requires'] = dependencies
else:
    setup_kws['requires'] = dependencies

###################################################################### Version information
# Writing version control information to the module
# adapted from MDTraj setup.py

def git_version():
    # Return the git revision as a string
    # copied from numpy setup.py
    def _minimal_ext_cmd(cmd):
        # construct minimal environment
        env = {}
        for k in ['SYSTEMROOT', 'PATH']:
            v = os.environ.get(k)
            if v is not None:
                env[k] = v
        # LANGUAGE is used on win32
        env['LANGUAGE'] = 'C'
        env['LANG'] = 'C'
        env['LC_ALL'] = 'C'
        out = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, env=env).communicate()[0]
        return out

    try:
        out = _minimal_ext_cmd(['git', 'rev-parse', 'HEAD'])
        GIT_REVISION = out.strip().decode('ascii')
    except OSError:
        GIT_REVISION = 'Unknown'

    return GIT_REVISION


def write_version_py(filename):
    cnt = textwrap.dedent("""\
    # THIS FILE IS GENERATED FROM SETUP.PY
    version = '%(version)s'
    short_version = version
    full_version = '%(full_version)s'
    release = %(isrelease)s

    if not release:
        version = full_version
    """)
    if os.path.exists('.git'):
        git_revision = git_version()
    else:
        git_revision = 'Unknown'

    a = open(filename, 'w')
    try:
        keys = dict(
            version      = git_revision[:7],
            full_version = git_revision,
            git_revision = git_revision,
            isrelease    = ISRELEASED,
            )
        a.write(cnt % keys)
    finally:
        a.close()

###################################################################### Find my python modules

def find_packages(root):
    """Find all python packages.
    Adapted from IPython's setupbase.py. Copyright IPython
    contributors, licensed under the BSD license.
    """
    packages = []
    for dir,subdirs,files in os.walk(root):
        package = dir.replace(os.path.sep, '.')
        if '__init__.py' not in files:
            # not a package
            continue
        packages.append(package)
    return packages

###################################################################### scripts

if 'setuptools' in sys.modules:
    setup_kws['entry_points'] = {
        'console_scripts' : ['mdq = mdq.scripts.mdq:main']
        }
else:
    setup_kws['scripts'] = glob.glob('scripts/*')

###################################################################### run Setup
ISRELEASED = False
VERSION = git_version()[:7]
__version__ = VERSION

write_version_py('mdq/version.py')
setup(name = 'mdq',
      author = "Badi' Abdul-Wahid",
      author_email = 'abdulwahidc@gmail.com',
      description = DOCLINES[0],
      long_description = '\n'.join(DOCLINES),
      version = __version__,
      license = 'LGPLv2',
      url = 'http://github.com/badi/mdq',
      platforms = ['Linux', 'Mac OS-X', 'Unix', 'Windows'],
      packages = find_packages('mdq'),
      **setup_kws)
