#
# Implement bill of materials (bom)
import copy
import datetime
import hashlib
import os
import shutil
import socket
import sys
import tempfile


def system_module(mod):
    """A simple way to determine if a module is a system module"""
    try:
        return "lib/python" in mod.__file__
    except AttributeError:
        return True
    except TypeError:
        return False

def file_stats(path):
    """Look at a path and return the lines, bytes, and sha-1"""
    try:
        with open(path, "rb") as f:
            data = f.read()
            hasher = hashlib.sha1()
            hasher.update(data)
            return len(data), data.count(b"\n"), hasher.hexdigest()
    except TypeError:
        return None, None, None
    except FileNotFoundError:
        return None, None, None
    except NotADirectoryError:
        # Reading out of a ZIP file most likely
        return None, None, None

def get_bom(content=False):
    """Return a set of (name,path,ver,bytecount[,lines,sha1]) values. All paths are absolute.
    :param content: - examine content. Adds lines & sha1 to output.
    """
    for include_system_modules in [False, True]:
        for (name, mod) in sys.modules.items():

            if system_module(mod) != include_system_modules:
                continue  # don't use this one
            try:
                ver = sys.modules[name].__version__
            except AttributeError:
                ver = None
            try:
                fname = mod.__file__
            except (AttributeError,TypeError):
                fname = None
            if type(name) != str:
                name = ""
            if type(fname) != str:
                fname = ""

            if not content:
                try:
                    yield (name, os.path.abspath(fname), ver, os.path.getsize(fname))
                except (FileNotFoundError, NotADirectoryError):
                    yield (name, os.path.abspath(fname), ver, None)
            else:
                if fname is not None:
                    stats = file_stats(fname)
                else:
                    stats = (None, None, None)
                yield (name, os.path.abspath(fname), ver, stats[0], stats[1], stats[2])
