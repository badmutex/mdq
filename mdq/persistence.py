from pxul.logging import logger

import pickle
import shelve

class Persistent(object):
    def __init__(self, name, flag='c', protocol=0, writeback=False):
        logger.debug('Opening %s' % name)
        shelf = shelve.open(name, flag=flag, protocol=protocol, writeback=writeback)
        object.__setattr__(self, '_shelf', shelf)
        object.__setattr__(self, '_name', name)

    @property
    def name(self): return self._name

    def __getattribute__(self, attr):
        try:
            attribute = object.__getattribute__(self, attr)
        except AttributeError:
            obj = object.__getattribute__(self, '_shelf')
            attribute = getattr(obj, attr)
        return attribute

    def __contains__(self, key):
        return key in self._shelf

    def __del__(self):
        self.close()

    def __delitem__(self, key):
        del self._shelf[key]

    def __getitem__(self, key):
        return self._shelf[key]

    def __iter__(self):
        return iter(self._shelf)

    def __len__(self):
        return len(self._shelf)

    def __setitem__(self, key, value):
        self._shelf[key] = value

    def __str__(self):
        return str(self._shelf)

    def sync(self):
        logger.debug('Syncing %s' % self._name)
        self._shelf.sync()

    def close(self):
        logger.debug('Closing %s' % self._name)
        self._shelf.close()
