__all__ = ['MkWorkQueue']

from . import _wq
import work_queue as ccl

import copy
import os
import pwd
import socket

class FileType:
    INPUT  = ccl.WORK_QUEUE_TASK_INPUT
    OUTPUT = ccl.WORK_QUEUE_TASK_OUTPUT

class File(object):
    def __init__(self, local, remote=None, cache=True, filetype=None):
        assert filetype is not None
        self._local    = local
        self._remote   = remote if remote is not None else os.path.basename(local)
        self._cache    = cache
        self._filetype = filetype

    @property
    def local(self):
        """Name of the file on the local machine"""
        return self._local

    @property
    def remote(self):
        """Name of the file on the worker"""
        return self._remote

    @property
    def cached(self):
        """Is this file to be cached on the remote machine?"""
        return self._cache

    @property
    def type(self):
        """The file type"""
        return self._filetype

    def add_to_task(self, task):
        task.specify_file(self.local, remote=self.remote, type=self.type, cache=self.cached)

class Buffer(object):
    def __init__(self, string, remote, cache=True):
        self._buffer = string
        self._remote = remote
        self._cache = cache

    def add_to_task(self, task):
        task.specify_buffer(self._buffer, self._remote, cache=self._cache)

class Schedule:
    """An `enum` of scheduling algorithms"""
    FCFS  = ccl.WORK_QUEUE_SCHEDULE_FCFS
    FILES = ccl.WORK_QUEUE_SCHEDULE_FILES
    TIME  = ccl.WORK_QUEUE_SCHEDULE_TIME
    RAND  = ccl.WORK_QUEUE_SCHEDULE_RAND

class Task(object):
    """
    A pure python description of a task mirroring the `work_queue.Task` API
    """
    def __init__(self, command):
        self._command = command
        self._files = list()
        self._named_files = dict()
        self._algorithm = Schedule.FCFS
        self._buffer = None
        self._tag = None

    ################################################################################ WQ API wrapper

    def clone(self):
        """Create a copy of this task that may be submitted"""
        return copy.deepcopy(self)

    def specify_algorithm(self, alg):
        self._algorithm = alg

    def specify_buffer(self, string, remote, cache=True):
        self._buffer = Buffer(string, remote, cache=cache)

    def specify_file(self, local, remote=None, filetype=None, cache=True, name=None):
        f = File(local, remote=remote, cache=cache, filetype=filetype)
        self._files.append(f)
        if name is not None:
            self._named_files[name] = f

    def specify_input_file(self, local, remote=None, cache=True, name=None):
        self.specify_file(local, remote=remote, cache=cache, name=name, filetype=FileType.INPUT)

    def specify_output_file(self, local, remote=None, cache=True, name=None):
        self.specify_file(local, remote=remote, cache=cache, name=name, filetype=FileType.OUTPUT)

    def specify_tag(self, tag):
        self._tag = tag

    @property
    def command(self):
        """The command to execute"""
        return self._command

    def _filter_files_by(self, filetype):
        return filter(lambda f: f.type == filetype, self._files)

    ################################################################################

    @property
    def input_files(self):
        return self._filter_files_by(FileType.INPUT)

    @property
    def output_files(self):
        return self._filter_files_by(FileType.OUTPUT)

    @property
    def named_files(self):
        return self._named_files

    ################################################################################ To WQ Tasks

    def to_task(self):
        """
        Return a `work_queue.Task` object that can be submitted to a `work_queue.WorkQueue`
        """
        task = ccl.Task(self.command)
        for f in self._files:
            f.add_to_task(task)
        task.specify_algorithm(self.algorithm)
        if self._buffer is not None:
            self._buffer.add_to_task(task)
        if self._tag is not None:
            task.specify_tag(self._tag)
        return task


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
        self._generations = False

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

        ################################################## Generational Tasks
        if self._generations:
            q = _wq.generational.WorkQueue(q, self._generations)

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

    def generations(self, generations=True):
        self._generations = generations
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
