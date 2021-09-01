"""Interface between environment variables, configuration variables, and Python.

Can read environment variables in bash scripts such as:

export FOO=bar
export BAR=biff

and return an associative array of {'FOO':'bar','BAR':biff}

Can find variables in /etc/profile.d/*.bash.

Can process JSON configuration files in the form:
  { "*": {'name':'val',...},
    "E1": {'name':'val',...},
    "E2": {'name':'val',...}}

and search for the value of 'name' going to the current environment (e.g. E1), and then to the default environemnt (e.g. *)
"""

import os
import re
import pwd
import sys
import glob
import json
import logging
from os.path import dirname,basename,abspath

VARS_RE   = re.compile(r"^(export)?\s*(?P<name>[a-zA-Z][a-zA-Z0-9_]*)=(?P<value>.*)$")
EXPORT_RE = re.compile(r"^export ([a-zA-Z][a-zA-Z0-9_]*)=(.*)$")

def get_vars(fname):
    """Read the bash EXPORT variables in fname and return them in a dictionary
    :param fname: the name of a bash script
    """
    ret = {}
    with open(fname, 'r') as f:
        for line in f:
            m = VARS_RE.search(line)
            if m:
                name  = m.group('name')
                value = m.group('value')
                if (len(value)>0) and (value[0] in ['"', "'"]) and (value[0]==value[-1]):
                    value = value[1:-1]
                ret[name] = value
    return ret


def get_env(pathname=None, *, profile_dir=None, prefix=None):
    """Read the BASH file and extract the variables. Currently this is
done with pattern matching. Another way would be to run the BASH
script as a subshell and then do a printenv and actually capture the
variables
    :param pathname: if provided, use this path
    :param profile_dir: If provided, search this directory
    :param prefix: if provided and profile_dir is provided, search for all files in the directory
    :return: the variables that were learned.
"""
    if (pathname is not None) and (profile_dir is not None):
        raise ValueError("pathname and profile_dir canot both be provided")
    if (profile_dir is not None) and (prefix is None):
        raise ValueError("If profile_dir is provided, pathname must be provided.")
    if profile_dir:
        names = sorted(glob.glob(os.path.join(profile_dir, prefix+"*")))
        if len(names)==0:
            raise FileNotFoundError(f"No file with prefix {prefix} in {profile_dir}")
        pathname = names[0]

    ret = {}
    for (key,val) in get_vars(pathname).items():
        ret[key] = os.environ[key] = os.path.expandvars(val)
    return ret


def get_census_env():
    """Legacy to be deleted.
    Look for a script in /etc/profile.d/ beginning with 'census' and read the variables in it.
    """
    return get_env(profile_dir = '/etc/profile.d', prefix = 'census')

def get_home():
    """Return the current user's home directory without using the HOME variable. """
    return pwd.getpwuid(os.getuid()).pw_dir


def dump(out):
    print("==== ENV ====",file=out)
    for (key,val) in os.environ.items():
        print(f"{key}={val}",file=out)


class JSONConfigReader:
    @classmethod
    def searchFile(self,path):
        """Search for the file named by path in the current directory, and then in every directory up to the root.
        Then every directory from ctool's directory to root.
        When found, return it. Otherwise return path if the file exists, otherwise raise an exception
        """
        checked = []
        name = os.path.join( os.getcwd(), basename(path))
        while dirname(name) != '/':
            checked.append(name)
            if os.path.exists(name):
                return name
            name = os.path.join( dirname(dirname(name)), basename(name))

        name = os.path.join( dirname(abspath(__file__)), basename(path))
        while dirname(name) != '/':
            checked.append(name)
            if os.path.exists(name):
                return name
            name = os.path.join( dirname(dirname(name)), basename(name))


        if os.path.exists(path):
            return path
        for check in checked:
            logging.error(f"checked {check}")
        raise FileNotFoundError(path)


    def __init__(self, *, path=None, search=True, config=None, environment=None, envar=None ):
        """
        :param path: location of JSON config file.
        :param search: Search from current directory up to root for a file with the same filename as `path` before using `path`.
        :param config: If provided, use the configuration specified in this dictionary, instead of path.
        :param environment: Specifies the environment inside the JSON dictionary that should be used, and then default to '*'.
        :param envar: Specifics an os.environ[] name that should be used for the environment.
        """
        self.environment= '*'
        self.path = None
        if (path is not None) and (config is not None):
            raise ValueError("Only path or config can be specified")
        if (environment is not None) and (envar is not None):
            raise ValueError("Only environment or envar can be specified")
        if path:
            # If search is true, search for the named config file from the current directory to the root
            # directory. If it isn't found, use the pathname
            if search:
                self.path = self.searchFile(path)
            else:
                self.path = path
            self.config = json.load(open(self.path))
        else:
            self.path = 'provided dictionary'
            self.config = config
        if environment:
            self.environment = environment
        if envar:
            self.environment = os.environ[envar]


    def get_config(self, variable_name, environment=None):
        # Handle one layer deep of FOO.BAR to search in FOO's directory for BAR.
        if environment is None:
            environment = self.environment

        if "." in variable_name:
            (name,ext) = variable_name.split(".",1)
            val = self.get_config(name, environment)
            return val[ext]

        for check in [environment,'*']:
            try:
                return self.config[check][variable_name]
            except KeyError:
                pass
        print(f"config:\n{json.dumps(self.config,default=str,indent=4)}",file=sys.stderr)
        raise KeyError(f"{variable_name} not in {check} or '*' in {self.path}")
