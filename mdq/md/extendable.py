"""
MD Tasks typicall cannot finish within the timeframe supported by most workers.
The `Extendable` interface allows tasks implementing it to update their internal
state so the then subsequent task continue the simulation from the previous time point.
"""

class Extendable(object):
    def extend(self):
        """
        Update the object internal state so that it represent the next set of work to do.
        """
        raise NotImplemented
