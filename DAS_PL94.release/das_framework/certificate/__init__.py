#!/usr/bin/env python3
#
# Code to construct and print the certificate
#


import copy
import datetime
import hashlib
import os
import shutil
import socket
import sys
import os.path
import tempfile
import io
import logging
from modulefinder import ModuleFinder

__version__ = '0.1.0'

from subprocess import Popen, PIPE

from .bom import get_bom, system_module, file_stats

# grab tytable and latex_tools from ../ctools
from os.path import abspath,basename,dirname
sys.path.append( dirname( dirname( abspath( __file__))))

import ctools.tytable as tytable
import ctools.latex_tools as latex_tools

THIS_DIR   = dirname( abspath( __file__ ))
TEMPLATE   = "cert_template.tex"
SEAL       = "Seal_of_the_United_States_Census_Bureau.pdf"
BACKGROUND = "background1.jpg"

LATEX_BOM_COLSPEC = r"p{1.5in}cp{3.0in}rr>{\tt}l<{}"

SUBST = '@@'
CONFIG_FILENAME_SUBST = '@@CONFIG-FILENAME@@'
CONFIG_SUBST = '@@CONFIG@@'
BOM_SUBST = '@@BOM@@'
GIT_COMMIT_SUBST = '@@GIT-COMMIT@@'


def shell(cmd):
    """Run cmd and return stdout"""
    return Popen(cmd, stdout=PIPE, encoding='utf-8').communicate()[0].strip()

def make_runtime():
    """Generate information about the runtime and return the tt object"""
    tt = tytable.ttable()
    tt.set_col_alignment(0, tt.LEFT)
    tt.set_col_alignment(1, tt.LEFT)
    tt.add_data(tt.HR)
    for (k, v) in [["hostname", socket.gethostname()],
                   ["uptime",   shell("uptime")],
                   ["time",     datetime.datetime.now().isoformat()[0:19]]]:
        tt.add_data([k, v])
        tt.add_data(tt.HR)
    return tt


def typeset_bom():
    """Generate a bill of materials and return the tt object."""

    finder = ModuleFinder()
    finder.run_script(sys.argv[0])
    tt = tytable.ttable()
    tt.add_option(tt.OPTION_LONGTABLE)
    tt.add_head(['name', 'ver', 'path', 'bytes', 'lines', 'sha-1'])
    tt.set_latex_colspec( LATEX_BOM_COLSPEC )
    for inc in [False, True]:
        for name, mod in sorted(finder.modules.items()):
            if system_module(mod) != inc:
                continue  # don't use this one
            stats = file_stats(mod.__file__)
            ver = mod.globalnames.get('__version__', '---')
            if ver == 1 and name in sys.modules:
                # It has a version number; get it.
                try:
                    ver = sys.modules[name].__version__
                except AttributeError:
                    ver = '---'
            fname = mod.__file__
            if type(name) != str:
                name = ""
            if type(fname) != str:
                fname = ""
            tt.add_data([name, ver, fname, stats[0], stats[1], stats[2]])
        tt.add_data(tt.HR)
    return tt

class CertificatePrinter:
    """Class to print a certificate.
    TODO: Modify to get the template, seal and background from the configuration file.
    """

    def __init__(self, *, title=None, template=None, params=None):
        self.title = title
        self.parts = []
        if params is None:
            params = {}
        self.params = copy.deepcopy(params)  # parameters to substitute

        self.template   = template if template is not None else os.path.join(THIS_DIR, TEMPLATE)
        self.background = os.path.join(THIS_DIR, BACKGROUND)
        self.seal       = os.path.join(THIS_DIR, SEAL)
        self.params[GIT_COMMIT_SUBST]      = '*** NO GIT COMMIT PROVIDED ***'
        self.params[CONFIG_FILENAME_SUBST] = '*** NO CONFIG FILENAME PROVIDED ***'
        self.params[CONFIG_SUBST] = '*** NO CONFIG FILE PROVIDED ***'


    def add_params(self, params:dict):
        """Merge params into the existing params, adding the quote characters"""
        for (k,v) in params.items():
            self.params[SUBST + k + SUBST] = v
            logging.info("Certificate param %s = %s",k,v)

    def add_config(self, config):
        depth = 0
        f = io.StringIO()
        f.write("\\begin{Verbatim}\n")
        for line in config.asString().split("\n"):
            if line.strip()[0:1]=='[':
                f.write("\n"
                        "\\end{Verbatim}\n"
                        "\n\n"
                        "\\noindent{\\ttfamily\\bfseries " + latex_tools.latex_escape(line) + "}"
                        "\n\n"
                        "\\begin{Verbatim}\n" )
                continue
            if line.strip().startswith("; end include"):
                depth -= 1
            f.write(" "*(depth*4))
            f.write(line)
            f.write("\n")
            if line.strip().startswith("; begin include"):
                depth += 1
        f.write("\n\\end{Verbatim}\n")
        self.params[CONFIG_FILENAME_SUBST] = latex_tools.latex_escape(os.path.realpath(config.input_filename))
        self.params[CONFIG_SUBST] = f.getvalue()

    def typeset(self, pdf_name):
        """Typeset the template"""
        tt_bom = typeset_bom()
        tt_run = make_runtime()
        self.params[BOM_SUBST]    = tt_bom.typeset(mode='latex') + tt_run.typeset(mode='latex')
        with open(self.template) as f:
            data = f.read()
            for (k, v) in self.params.items():
                data = data.replace(k, v)
            outdir = tempfile.mkdtemp()
            out = tempfile.NamedTemporaryFile(encoding="utf-8", suffix=".tex", mode="w+t", dir=outdir, delete=False)
            out.write(data)
            out.flush()
            # Save a copy for testing
            # open("test.tex","w").write(data)
            shutil.copy(self.seal, os.path.dirname(out.name))
            shutil.copy(self.background, os.path.dirname(out.name))
            texfiles = os.path.join(os.path.dirname(os.path.abspath(__file__)), "texfiles")
            latex_tools.run_latex(out.name, delete_tempfiles=False, repeat=2, texinputs=texfiles + ":")
            shutil.move(out.name.replace(".tex", ".pdf"), pdf_name)
            shutil.rmtree(outdir)
