from pxul.logging import logger

import pickle
# import shelve

class Persistent(object):
    def __init__(self, obj, to=None, flag='c', protocol=0, writeback=False):
        self._obj = obj
        self._name = to

    @property
    def name(self): return self._name

    def __getattribute__(self, attr):
        try:
            attribute = object.__getattribute__(self, attr)
        except AttributeError:
            obj = object.__getattribute__(self, '_obj')
            attribute = getattr(obj, attr)
        return attribute

    def sync(self):
        logger.debug('Syncing %s' % self._name)
        with open(self.name, 'w') as fd:
            pickle.dump(self._obj, fd)

    @classmethod
    def load(cls, path):
        with open(path) as fd:
            return pickle.load(fd)
