
class WorkQueue(object):

    def __init__(self, q):
        object.__setattr__(self, '_q', q)

    def __getattribute__(self, attr):

        try:
            attribute = object.__getattribute__(self, attr)
        except AttributeError:
            q = object.__getattribute__(self, '_q')
            attribute = object.__getattribute__(q, attr)

        return attribute
