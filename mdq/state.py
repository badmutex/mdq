
from .persistence import Persistent
from .stringio import StringIO

import hashlib
import os

DOT_DIR = '.mdq'
CONFIG = os.path.join(DOT_DIR, 'config')
SIMS = os.path.join(DOT_DIR, 'sims')
STATE = os.path.join(DOT_DIR, 'state')

def to_yaml_sio(sio, obj):
    if hasattr(obj, 'to_yaml_sio'):
        obj.to_yaml_sio(sio)
    else:
        sio.writeln(str(obj))

class CADict(object):
    """
    Content-Addressable Dictionary
    """

    def __init__(self):
        self._d = dict()

    def __iter__(self):
        return iter(self._d)

    def __getitem__(self, k): return self._d[k]

    def add(self, obj):
        """
        Only add the object if it has not been previously added
        obj: must provide a `digest` method returning the hexdigest
        """
        h = obj.digest()
        if h not in self._d:
            self._d[h] = obj


def hash_file(hasher, path, size=32*1024*1024):
    """
    Hash the contents of the file given by `path`, reading at most `size` bytes at a time
    """
    with open(path, 'rb') as fd:
        while True:
            data = fd.read(size)
            if data == '': break
            hasher.update(data)

class Spec(dict):
    """:: name -> str """

    def digest(self):
        h = hashlib.sha256()
        for obj in self.keys() + self.values():
            if os.path.exists(str(obj)):
                hash_file(h, str(obj))
            else:
                data = repr(obj)
            h.update(data)
        return h.hexdigest()

class Config(object):
    def __init__(self, backend='gromacs', total_time=-1, time=1000, cpus=1, binaries=None, seed=19):

        self.backend    = backend
        self.sims       = CADict()
        self.total_time = total_time
        self.time       = time
        self.cpus       = cpus
        self.binaries   = binaries
        self.seed       = seed

    def add(self, spec):
        self.sims.add(spec)

    def binary(self, name):
        """
        Return the path to the binary for the Tasks $OS and $ARCH
        """
        return os.path.join(self.binaries, '$OS', '$ARCH', name)

    def write(self, path=None):
        path = path or CONFIG
        if not os.path.exists(os.path.dirname(CONFIG)):
            os.makedirs(os.path.dirname(CONFIG))

        p = Persistent(path)
        p['config'] = self
        p.close()

    @classmethod
    def load(cls, path=None):
        path = path or CONFIG
        p = Persistent(path)
        c = p['config']
        p.close()
        return c

class State(object):
    def __init__(self, path):
        self._p = Persistent(path)

    def __setitem__(self, key, obj): self._p[key] = obj

    def __getitem__(self, key): return self._p[key]

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._p.sync()













