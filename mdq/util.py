import os

def find(exe, search=None, root=None):
    """Try to find the executable name in either a ':'-delimited search
    path or by walking a directory from the root
    """
    if search is not None:
        return find_in_path(exe, search=search)
    elif root is not None:
        return _find_root(exe, root=root)
    else: raise ValueError, 'Options exhausted'

def find_in_path(exe, search=None):
    """
    Attempts to locate the given executable in the provides search paths
    """

    search = search if search is not None else os.environ['PATH'].split(os.pathsep)
    for prefix in search:
        path = os.path.join(prefix, exe)
        if os.path.exists(path) and os.access(path, os.X_OK):
            return path

def find_in_root(exe, root='/'):
    """
    Attempts to find the executable name by traversing the directory structure starting at `root`
    """
    for dirpath, dirnames, filenames in os.walk(root):
        path = os.path.join(dirpath, exe)
        if exe in filenames and os.access(path, os.X_OK):
            return path
