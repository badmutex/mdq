

from .. import workqueue

Taskable = workqueue.Taskable

class Extendable(object):
    """
    MD Tasks typicall cannot finish within the timeframe supported by most workers.
    The `Extendable` interface allows tasks implementing it to update their internal
    state so the then subsequent task continue the simulation from the previous time point.
    """

    def extend(self):
        """
        Update the object internal state so that it represent the next set of work to do.
        """
        raise NotImplemented

class Preparable(object):
    """
    Different MD backends may require different steps to create a `Taskable`.
    """

    def task(self, *args, **kws):
        """
        Return an object implementing the `Exentable` and `Taskable` interfaces
        """
        return NotImplemented
