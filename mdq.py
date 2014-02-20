import work_queue
import os

class FileType:
    INPUT  = work_queue.WORK_QUEUE_INPUT
    OUTPUT = work_queue.WORK_QUEUE_OUTPUT

class File(object):
    def __init__(self, ftype, localpath, remotepath=None, cache=True):
        remotepath = remotepath if remotepath is not None else os.path.basename(localpath)
        self._type   = ftype
        self._local  = localpath
        self._remote = remotepath
        self._cache  = cache

    @classmethod
    def input(cls, localpath, remotepath=None, cache=True):
        """
        Construct an input File.
        """
        return cls(FileType.INPUT, localpath, remotepath=remotepath, cache=cache)

    @classmethod
    def output(cls, localpath, remotepath=None, cache=True):
        """
        Construct and output File
        """
        return cls(FileType.OUTPUT, localpath, remotepath=remotepath, cache=cache)

    def add_to_task(self, task):
        task.specify_file(self._local, self._remote, type=self._type, cache=self._cache)
        

class Task(object):

    def __init__(self, cmd, workarea='.', generations=1):
        self._workarea = workarea
        self._gen      = 0
        self._gens     = generations
        self._cmd      = cmd
        self._files    = list()
        self._tag      = ''

    @property
    def tag(self): return self._tag

    @tag.setter
    def tag(self, s): self._tag = s

    @property
    def label(self): return self._tag + '_%d' % self._gen

    @property
    def location(self): return os.path.join(self._workarea, self.label)

    def add_dynamic(self, ftype, localname, remotename=None, cache=True):
        self._dynamic.append((ftype, localname, remotename, cache))

    def add_static_input_file(self, localpath, cache=True):
        self._static.append((localpath, cache)

    def _incr(self):
        if self._gen < self._gens:
            self._gen += 1
            return True
        else:
            return False

    def _to_task(self):
        t = work_queue.Task(self._cmd)
        t.specify_tag(self.label)

        # static files
        for path, cache in self._static:
            t.specify_input_file(path, cache=cache)

        # dynamic (generational) files
        for ftype, localname, remotename, cache in self._dynamic:
            localpath = os.path.join(self.location, localname)
            t.specify_file(localpath, remote_name=remotename, cache=cache)

        return t
        
    def _next(self):
        raise NotImplemented

    

class WorkQueue(object):

    def __init__(self, *args, **kws):
        self._q = work_queue.WorkQueue(*args, **kws)
        self._tasks = dict() # work_queue.Task.id -> Task


    def submit(self, task):
        t = task.to_task()
        assert t.id not in self._tasks
        self._tasks[t.id] = task
        self._q.submit(t)

    def _recv(self, t):
        task = self._tasks[t.id]
        del self._tasks[t.id]
        if task.incr():
            self.submit(task)
        else:
            return t

    def wait(self, timeout=-1):
        t = self._q.wait(timeout=timeout)
        if t is not None and t.result == 0:
            r = self._recv(t)
            if r is not None:
                return r
            else:
                raise Exception


class GMX(Task):
    def next(self):
        self._incr()
        
