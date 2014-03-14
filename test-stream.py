from mdq.stream    import Fount, Stream, Sink
from mdq.workqueue import Task, MkWorkQueue


class MockFount(Fount):
    def generate(self):
        import random
        for i in xrange(10):
            sleep = 1
            sleep = random.randint(1,30)
            yield Task('echo %s;sleep %s' % (i, sleep))

class MockStream(Stream):
    pass

if __name__ == '__main__':

    q = (
        MkWorkQueue()
        .port(9123)
        .debug_all()
        .replicate(8)
        ()
    )

    with open('wq.log', 'w') as fd:
        fd.write('#')
        q.specify_log(fd.name)

    fount     = MockFount()
    submit    = MockStream(q, fount)
    sink      = Sink(submit)

    for r in sink:
        print r.tag
