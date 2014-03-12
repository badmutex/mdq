__all__ = ['MkWorkQueue']

from . import _wq
import work_queue as ccl
import os
import pwd
import socket

Task        = ccl.Task

class MkWorkQueue(object):
    """
    Factory for creating WorkQueue object
    Call the factory instance to return the WorkQueue instance
    E.g.
      mk = MkWorkQueue()
      mk.port(1234).name('hello').catalog().debug_all().logfile()
      q = mk()
      for t in <tasks>: q.submit(t)
      ...
    """

    def __init__(self):
        self._port = 9123
        self._name = None
        self._catalog = False
        self._exclusive = True
        self._shutdown = False
        self._fast_abort = -1
        self._debug = None
        self._logfile = None

        # mdq additions
        self._replicate = None

    def __call__(self):

        ################################################## Debugging
        if self._debug is not None:
            ccl.set_debug_flag(self._debug)

        ################################################## Vanilla WorkQueue
        kws = dict()
        kws['port'] = self._port
        kws['catalog'] = self._catalog
        if self._name is not None:
            kws['name'] = self._name
        kws['exclusive'] = self._exclusive
        kws['shutdown'] = self._shutdown

        q = ccl.WorkQueue(**kws)

        q.activate_fast_abort(self._fast_abort)
        if self._logfile is not None:
            q.specify_log(self._logfile)

        ################################################## Task Replication
        if self._replicate is not None:
            q = _wq.replication.WorkQueue(q, maxreplicas=self._replicate)

        return q

    def port(self, port=None):
        """
        no arg: choose random port
        port: int
        """
        p = -1 if port is None else port
        self._port = p
        return self
        
    def name(self, name=None):
        """
        Give a name to the WorkQueue.
        If none: default to "<hostname>-<username>-<pid>"
        """
        if name is None:
            host = socket.gethostname()
            user = pwd.getpwuid(os.getuid()).pw_name
            pid  = os.getpid()
            name = '%s-%s-%s' % (host, user, pid)
        self._name = name
        return self

    def catalog(self, catalog=True):
        """
        Set catalog mode
        """
        self._catalog = catalog
        return self

    def exclusive(self, exclusive=None):
        """
        no args: toggle exclusivity
        exclusive: bool
        """
        if exclusive is None:
            self._exclusive = self._exclusive or not self._exclusive
        else:
            self._exclusive = exclusive
        return self

    def shutdown(self, shutdown=None):
        """
        no args: toggle automatic shutdown of workers when work queue finished
        shutdown: bool
        """
        if shutdown is None:
            self._shutdown = self._shutdown or not self._shutdown
        else:
            self._shutdown = shutdown
        return self

    def fast_abort(self, multiplier=None):
        """
        no arg: toggle between inactive and 3
        multiplier: float
        """
        if multiplier is None:
            if self._fast_abort < 0:
                self._fast_abort = 3
            else:
                self._fast_abort = -1
        else:
            self._fast_abort = multiplier
        return self

    def debug(self, debug='all'):
        self._debug = debug
        return self

    def debug_all(self):
        return self.debug('all')

    def debug_wq(self):
        return self.debug('wq')

    def logfile(self, logfile='wq.log'):
        self._logfile = logfile
        return self

    def replicate(self, maxreplicates=9):
        """
        Use task replication.
        """
        self._replicate = maxreplicates
        return self


### test
if __name__ == '__main__':
    m = MkWorkQueue()
    (
    m
    .name()
    .catalog()
    .debug_all()
    .logfile()
    )
    q = m()
    import time
    time.sleep(60)
