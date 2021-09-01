#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Some tools for manipulating PDF files

import sys
import os
import re
from subprocess import call, Popen, PIPE, DEVNULL
import subprocess
import glob
import tempfile
import logging
import hashlib
import platform
import shutil
import distutils.spawn
import warnings

ERROR_LINES=50
__version__ = "0.2.0"

DEBUG=False

HEIGHT='height'
LANDSCAPE = 'LANDSCAPE'
ORIENTATION='orientation'
PAGE='page'
PAGES='pages'
POINTS='points'
PORTRAIT = 'PORTRAIT'
UNITS='units'
VERSION='VERSION'
WIDTH='width'
SHA256='sha256'
FILENAME='filename'

if sys.platform=='win32':
    LATEX_EXE='pdflatex.exe'
else:
    LATEX_EXE='pdflatex'

def no_latex():
    """Return true if latex is not available"""
    return distutils.spawn.find_executable(LATEX_EXE) is None


LATEX_QUOTE_TRANSFORMS = {
    "’": "'",
    "“": "``",
    "”": "''",
    '\u2013': '---',
    '\u2014': '---',
    '\u2018': "`",
    '\u2019': "'",
    '\u2019': "'",
    '\u201c': '``',
    '\u201d': "''",
    'ò': r'\`{o}',
    'ó': r"'{o}",
    'ô': r'\^{o}',
    'ö': r'\"{o}',
    'ő': r'\H{o}',
    'õ': r'\~{o}',
    'ç': r'\c{c}',
    'ą': r'\k{a}',
    'ł': r'\l{}',
    'ō': r'\={o}',
    'ȯ': r'\.{o}',
    'ụ': r'\d{u}',
    'å': r'\r{a}',
    'š': r'\v{s}',
    'ø': r'\o{}',
    'î': r'\^{\i}',
    'ï': r'\"{\i}',
    '§': r'\S{}',
    '†': r'\dag{}',
    '©': r'\copyright{}',
    '\\': r'\textbackslash{}',
    '<': r'\textless{}',
    '®': r'\textregistered{}',
    '¿': r'\textquestiondown{}',
    '%': r'\%{}',
    '#': r'\#{}',
    '$': r'\${}',
    '_': r'\_{}',
    '&': r'\&{}',
    '{': r'\{',
    '}': r'\}'}

PAGECOUNTER_TEX=r"""
%% Based on:
%%
%% https://stackoverflow.com/questions/5834014/lf-will-be-replaced-by-crlf-in-git-what-is-that-and-is-it-important/5834094
%% https://tex.stackexchange.com/questions/125869/using-pdfpages-with-mixed-landscape-portrait-pdfs
%% https://stackoverflow.com/questions/17628305/windows-git-warning-lf-will-be-replaced-by-crlf-is-that-warning-tail-backwar/17628353
%%

\batchmode
\documentclass{article}
\usepackage{pdfpages}
\def\pageCountfile{%%FILENAME%%}
\pdfximage{\pageCountfile}

\begin{document}
\newcount\pageCountcount \pageCountcount=0 % This is a page counter...
\newbox\pageCountbox                       % This is a virtual box...

\loop % Let's test all the pages one by one...
\advance\pageCountcount by 1%
% Measure a single page virtually...
\setbox\pageCountbox=\hbox{\includegraphics[page={\the\pageCountcount}]{\pageCountfile}}%
% Send the information to the terminal...
% \scrollmode
\message{Page \the\pageCountcount, \ifnum\wd\pageCountbox<\ht\pageCountbox portrait\else landscape\fi, \the\wd\pageCountbox, \the\ht\pageCountbox, depth \the\dp\pageCountbox.}%
\batchmode
%%
%%
% Test all the pages from the input PDF file...
\ifnum\pageCountcount<\pdflastximagepages\repeat
\endinput
"""

LATEX_EXTRA_DIR=os.path.join(os.path.dirname(__file__), "latex-windows")

class LatexException(Exception):
    def __init__(self, message):
        super().__init__(message)

def latex_escape(msg):
    """Quote all special characters in msg"""
    msg = "".join([LATEX_QUOTE_TRANSFORMS.get(ch, ch) for ch in msg])
    for ch in msg:
        if ord(ch)>127:
            logging.warning("**** Unescaped character: '%s'  dec:%s  hex:%s", ch, ord(ch), hex(ord(ch)))
    return msg

def textbf(msg):
    return "\\textbf{" + msg + "}"

def nl():
    return "\\\\\n"

# from https://stackoverflow.com/questions/4284991/parsing-nested-parentheses-in-python-grab-content-by-level
# with modifications
def parse_nested_braces(string):
    """Generate parenthesized contents in string as pairs (level, contents)."""
    stack = []
    for i, c in enumerate(string):
        if c == '{':
            stack.append(i)
        elif c == '}' and stack:
            start = stack.pop()
            yield (len(stack), string[start + 1: i])


label_re  = re.compile(r'\{([^}]*)\}\{\{([^}]*)\}\{([^}]*)\}\{([^}]*)\}\{([^}]*)\}\{([^}]*)\}')
prefix_re = re.compile(r'^([A-Z]+)-(.*)')
def label_parser(line):
    """Return (prefix,filename,section,page)"""
    if not line.startswith("\\newlabel"):
        return None
    parsed = list(parse_nested_braces(line))

    def get(*, level, count):
        for (l, v) in parsed:
            if level==l:
                count -= 1
                if count==0:
                    return v
        return None

    label   = get(level=0, count=1)            # first level 0 is the filename
    section = get(level=1, count=1)
    page    = int(get(level=1, count=2))

    if not all([label, section, page]):
        return None

    n = prefix_re.search(label)
    if n:
        (prefix, fname) = n.group(1, 2)
    else:
        (prefix, fname) = ("", label)
    if fname[0]=='"':           # remove quotes if they are present
        fname = fname[1:-1]
    return (prefix, fname, section, page)

def extract_pages_from_aux(auxfile):
    """Given an auxfile, return an array of (prefix,filename,section,page,pages) values"""
    assert auxfile.endswith(".aux")
    ret = []
    lastdata = None
    for line in open(auxfile):
        pdata = label_parser(line)
        if not pdata:
            continue
        if pdata[1]=='LastPage':
            continue
        assert pdata[0] in ["", "END"]
        if pdata[0]=="":
            data = pdata
        else:
            ret.append((data[0], data[1], data[2], data[3], pdata[3] -data[3]))
    return ret

def delete_temp_files(latex_source, verbose=False):
    # Delete LaTeX temp file
    (name, ext) = os.path.splitext(latex_source)
    for ext in ['.log', '.aux', '.bbl', '.toc', '.blg', '.dvi']:
        for nfn in glob.glob(name +ext):
            if verbose:
                print("Deleting {}".format(nfn))
            os.unlink(nfn)


TEXINPUTS='TEXINPUTS'
def run_latex(pathname, repeat=1, start_run=1, delete_tempfiles=False,
              texinputs=None,
              callback_aux=None, callback_log=None, ignore_ret=False,
              chdir=True, verbose=False):
    """Run LaTeX and return (name of file PDF file,# of pages)"""

    if DEBUG:
        verbose=True

    # Are we setting TEXINPUTS? If so, remember old value.
    oldenv   = os.environ.get(TEXINPUTS, None)
    if texinputs:
        os.environ[TEXINPUTS] = texinputs
        if DEBUG:
            print("Set TEXINPUTS to", os.environ[TEXINPUTS])

    # Are we changing the directory? If so, remember old value
    assert os.path.exists(pathname)
    assert repeat>=1

    (dirname, filename) = os.path.split(pathname)
    dirname_ = dirname

    if chdir:
        cwd = os.getcwd()
        if dirname:
            os.chdir(dirname)       # change to the directory where the file exists
            if DEBUG:
                print("Changed directory to", dirname)

        # change dirname and pathname to reflect current directory
        dirname="."
        pathname=filename
        assert os.path.exists(filename)  # make sure we can still reach it

    # If we are on windows, copy the context of the LATEX_WINDOWS_EXTRA_DIR to the current directory
    delete_files = []
    if platform.system()=='Windows':
        for fn in glob.glob(os.path.join(LATEX_EXTRA_DIR, "*")):
            dest = os.path.basename(fn)
            if not os.path.exists(dest):
                d = shutil.copy(fn, dest)
                print("copy {} -> {}/{}".format(fn, os.getcwd(), d))

            delete_files.append(dest)

    if DEBUG:
        print("==============", pathname, "===========")
        print(open(pathname).read())
        print("======================================")
        print("")

    for i in range(start_run, start_run +repeat):
        assert os.path.exists(pathname)
        cmd = [LATEX_EXE, pathname, '-interaction=nonstopmode']

        if verbose:
            print("LaTeX Run #{}: {}> {}".format(i, os.getcwd(), " ".join(cmd)), flush=True)

        r = subprocess.run(cmd, stdout=PIPE, stderr=PIPE, stdin=DEVNULL, encoding='utf8', shell=False)
        if r.returncode and (("I can't find the format file" in r.stderr) or
                             ("does not exist" in r.stderr)):
            warnings.warn("Latex returns: " +r.stderr)
            raise LatexException("LaTeX not properly installed")

        if r.returncode and not ignore_ret:
            print("r=", r, "r.returncode=", r.returncode)
            outlines = r.stdout.split("\n")
            print("***************************")

            if len(outlines)<ERROR_LINES *2:
                print("\n".join(outlines))
            else:
                print("First {} lines of error:".format(ERROR_LINES))
                print("\n".join(outlines[:ERROR_LINES]))
                print("")
                print("Last {} lines of error:".format(ERROR_LINES))
                print("\n".join(outlines[-ERROR_LINES:]))

            print("***************************")
            exit(1)

        if r.returncode and DEBUG:
            print("r.returncode=", r.returncode)
            print("STDOUT")
            print(r.stdout)
            print("STDERR")
            print(r.stderr)

    for fn in delete_files:
        print("delete: {}/{}".format(os.getcwd(), fn))
        os.unlink(fn)

    #
    # The logfile and auxfile get written to the current directory
    logfilename = os.path.splitext(os.path.basename(filename))[0] + ".log"
    auxfilename = os.path.splitext(os.path.basename(filename))[0] + ".aux"

    if DEBUG:
        print("Current directory:", os.getcwd())
        print("filename:", filename, os.path.exists(filename))
        print("logfilename:", logfilename, os.path.exists(logfilename))
        print("auxfilename:", auxfilename, os.path.exists(auxfilename))

    if not os.path.exists(logfilename):
        warnings.warn(f"chdir: {chdir} dirname_: {dirname_}")
        warnings.warn(f"cmd: {' '.join(cmd)}")
        raise FileNotFoundError("logfile {} not created. LaTeX failed. dir: {} filename: {}"
                                .format(logfilename, os.getcwd(), filename))

    if not os.path.exists(auxfilename):
        warnings.warn(f"chdir: {chdir} dirname_: {dirname_}")
        warnings.warn(f"cmd: {' '.join(cmd)}")
        raise FileNotFoundError("auxfile {} not created. LaTeX failed. dir: {} filename: {}"
                                .format(auxfilename, os.getcwd(), filename))

    if callback_log:
        callback_log(open(logfilename, "r"))

    if callback_aux:
        callback_aux(open(auxfilename, "r"))

    # Read the log file to determine the number of pages
    # This should be done with the callback

    log = open(logfilename).read().replace("\n", "")
    pat = re.compile(r'Output written on .*[^\d](\d+) pages?')
    m = pat.search(log)
    if m:
        pages = int(m.group(1))
    else:
        pages = None

    # Delete the temp files if requested
    if delete_tempfiles:
        delete_temp_files(pathname)
    # Figure out the PDF filename. It's in the current directory...
    pdffile = os.path.join(os.getcwd(), os.path.basename(os.path.splitext(filename)[0])) + ".pdf"

    # If we changed the current directory, change back
    if chdir:
        os.chdir(cwd)

    # Restore environment
    if texinputs is not None:
        if oldenv:
            os.environ[TEXINPUTS] = oldenv
        else:
            del os.environ[TEXINPUTS]

    return (pdffile, pages)

def extract_pdf_pages(target, source, pagelist="-"):
    """Use pdfpages.sty to extract pagelist from SOURCE and put the results in TARGET"""
    assert os.path.exists(source)
    assert not os.path.exists(target)
    assert target.endswith(".pdf")
    assert source.endswith(".pdf")

    if os.path.dirname(target)==os.path.dirname(source):
        # They are in the same directory
        # run_latex will chdir() to target's directory, so remove
        # the dirname from source
        source = os.path.basename(source)

    target_latex = target.replace(".pdf", ".tex")

    if type(pagelist)==str:
        pagelist_str = pagelist
    else:
        pagelist_str = ",".join(str(x) for x in pagelist)

    #
    # Generate an output file
    with open(target_latex, "w") as f:
        f.write("\\documentclass{book}\n")
        f.write("\\usepackage{pdfpages}\n")
        f.write("\\begin{document}\n")
        f.write("\\includepdf[pages={" +pagelist_str +"}]{" +source +"}\n")
        f.write("\\end{document}\n")

    run_latex(target_latex, repeat=1, delete_tempfiles=True)


def inspect_json_all_pages_have_same_orientation(info):
    orientations = set([pageinfo[ORIENTATION] for pageinfo in info[PAGES]])
    if len(orientations)==1:
        return info[PAGES][0][ORIENTATION]
    return None

def inspect_pdf_latex(pdf_fname, texinputs=None):
    """Using PAGECOUNTER_TEX, run LaTeX on each page and determine each page's orientation and size.
    Returns a dictionary containing the following properties:
    [FILENAME] - filename
    [SHA256]   - sha256
    [PAGES][{pageinfo},pageinfo,...}    - #pages
        {pageinfo}  - Information about each page is stored in its own dictionary.
          ORIENTATION: = PORTRAIT or LANDSCAPE
          WIDTH:  = width (in pt)
          HEIGHT: = height (in pt)
          PAGE:   = page number (starts at 1)
    """
    assert os.path.exists(pdf_fname)
    assert pdf_fname.lower().endswith(".pdf")
    requested_pat = re.compile(r"Requested size: ([\d.]+)pt x ([\d.]+)pt")
    page_pat = re.compile(r"^Page (\d+), (\w+), ([0-9.]+)pt, ([0-9.]+)pt, depth ([0-9.]+)pt")
    ret = {VERSION: 1,
           FILENAME: pdf_fname,
           UNITS: POINTS,
           SHA256: hashlib.sha256(open(pdf_fname, "rb").read()).hexdigest(),
           PAGES: []}

    def cb(auxfile):
        """Callback to search for orientation information in the logfile and extract it"""
        width = None
        height = None
        for line in auxfile:
            m = requested_pat.search(line)
            if m:
                width = float(m.group(1))
                height = float(m.group(2))
            m = page_pat.search(line)
            if m:
                if width==None or height==None:
                    logging.error("************  CANNOT COUNT PAGES IN '%s' **************", pdf_fname)
                    exit(1)
                pageno = int(m.group(1))
                orientation = LANDSCAPE if width>height else PORTRAIT
                ret[PAGES].append({ORIENTATION: orientation, WIDTH: width, HEIGHT: height, PAGE: pageno})

    # Unfortunately, NamedTemporaryFile is not portable to windows, because when the file is open,
    # it cannot be used by other processes, as NamedTemporaryFile opens with exclusive access.
    # The code below fixes this problem
    # See https://bugs.python.org/issue14243
    logging.info("inspect_pdf(%s)", format(pdf_fname))

    if DEBUG:
        print("get_pdf_pages_and_orientation({})".format(pdf_fname))
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf8', suffix='.tex', delete=False,
                                     dir=os.path.dirname(os.path.abspath(pdf_fname))) as tmp:
        tmp.write(PAGECOUNTER_TEX.replace("%%FILENAME%%", os.path.basename(pdf_fname)))
        tmp.flush()             # Make sure contents are written out
        tmp.close()             # Windows compatability
        run_latex(tmp.name, callback_log=cb, ignore_ret=True, delete_tempfiles=True, texinputs=texinputs)
        os.unlink(tmp.name)
    return ret

def inspect_pdf_pypdf(pdf_fname):
    """As above, but with PyPDF"""
    if os.path.getsize(pdf_fname)==0:
        raise LatexException(f"{pdf_fname} is a zero-length file")

    import PyPDF2
    with open(pdf_fname, "rb") as f:
        sha256 = hashlib.sha256(f.read()).hexdigest()
    ret = {VERSION: 1,
           FILENAME: pdf_fname,
           UNITS: POINTS,
           SHA256: sha256,
           PAGES: []}
    pdf = PyPDF2.PdfFileReader(open(pdf_fname, "rb"), strict=False)
    for pageNumber in range(pdf.getNumPages()):
        page = pdf.getPage(pageNumber)
        (x, y, width, height) = page['/MediaBox']
        ret[PAGES].append({ORIENTATION: PORTRAIT if width < height else LANDSCAPE,
                           WIDTH: width,
                           HEIGHT: height,
                           PAGE: pageNumber +1})
    return ret

def inspect_pdf(pdf_fname, texinputs=None):
    try:
        return inspect_pdf_pypdf(pdf_fname)
    except (ImportError, ModuleNotFoundError):
        return inspect_pdf_latex(pdf_fname, texinputs=texinputs)

def count_pdf_pages_pypdf(pdf_fname):
    import PyPDF2
    pdf = PyPDF2.PdfFileReader(open(pdf_fname, "rb"), strict=False)
    return pdf.getNumPages()

def count_pdf_pages(pdf_fname):
    """Use pdfpages.sty to count how many pages in a pdf_fname"""
    if not os.path.exists(pdf_fname):
        raise LatexException("count_pdf_pages: {} does not exist".format(pdf_fname))
    if not pdf_fname.endswith(".pdf"):
        raise LatexException("count_pdf_pages: {} must end with a .pdf".format(pdf_fname))

    try:
        return count_pdf_pages_pypdf(pdf_fname)
    except ImportError:
        return len(inspect_pdf_latex(pdf_fname)[PAGES])


if __name__=="__main__":
    import sys
    m = inspect_pdf(sys.argv[1])
    for info in sorted(m[PAGES].keys()):
        print('page ', info, m[PAGES][info])
