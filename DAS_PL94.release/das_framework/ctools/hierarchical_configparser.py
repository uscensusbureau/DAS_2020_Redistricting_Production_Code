#!/usr/bin/env python3
#
r"""
hierarchical_configparser.py:

Like a regular configparser, but supports the INCLUDE= statement and validation..

Include Statement
-----------------
If INCLUDE=filename.ini is present in any section, the contents of that section are
read from filename.ini and provided as base definitions. Those definitions can be shadowed
by the local config file.

If filename.ini includes its own INCLUDE=, those are included as well.

Special handling of the [default] section:
The [default] section provides default options (e.g. name=value pairs) for *every* section.
If INCLUDE= appears in the [default] section of the ROOT hiearchical config file, then
all of the sections are read.

Validation
----------
Typically an included config file will include default values and they will then be overwritten by the file doing the including.
The included file can also contain validation rules in comments. They are structured like this:

   [section]
   colorNumber.re: (\d+)
   colorNumber.required: True
   colorNumber: 10

A new method called `.validate()` will run all of the validators and raise the exception InvalidConfiguration(section,option,value).



Implementation Notes
--------------------
- This is rev 2.0, which preserves comments and implements blame(). Python's config parser
  strips comments on read and thus cannot write them.

- Name/value pairs in the included file are read FIRST, so that they can be shadowed by name/value
  pairs in the including file.

- We implement a Line() class which remembers, for each line, the file from which it was read.

- The config file is imported with HCP which implements all of the include parsing.
  It then provides a flattened config file to HiearchicalConfigParser().

- Include loops are reliably detected.
"""

import os
import os.path
import logging
import sys
import collections
import re
import io
from configparser import ConfigParser, DuplicateOptionError, DuplicateSectionError
from copy import copy
from collections import defaultdict, namedtuple

DEFAULT='default'
INCLUDE='include'

Line = namedtuple('Line', ['filename', 'lineno', 'line'])

SECTION_RE = re.compile(r'^\[([^\]]*)\]:?\s*$')
OPTION_RE  = re.compile(r'^\s*([.\w]+)\s*[=:]\s*(.*)$')
INCLUDE_RE = re.compile(r'^\s*include\s*=\s*([^\s]+)\s*(;.*)?$', re.I)

HEAD_SECTION=""
DEFAULT_SECTION="default"

def getOption(line):
    """
    If :param line: contains an option, return it, otherwise return None.
    Lowercases option."""
    m = OPTION_RE.search(line)
    if m:
        return m.group(1).lower()
    return None


def getAllOptions(lines):
    return set([option for option in [getOption(line) for line in lines] if option is not None])

def getIncludeFile(line):
    """If the line has an INCLUDE= statementon it, return the the file included"""
    m = INCLUDE_RE.search(line)
    if m:
        return m.group(1)
    return None

class HierarchicalConfigParserError(RuntimeError):
    pass

class RequiredOptionMissing(HierarchicalConfigParserError):
    pass

class RegularExpressionValidationFailure(HierarchicalConfigParserError):
    pass

class HCP:
    """This class implements the HiearchicalConfigFile includes. But it does not implement the generic configfile.
    That's done below in the HiearchicalConfigParser class. This just implements read, write and blame.
    It only does string processing on lines and sections.
    """

    def __init__(self, *args, **kwargs):
        self.sections   = collections.OrderedDict()  # section name is lowercased
        self.seen_files = set()

    def read(self, filename, *, onlySection=None):
        """
        Reads a config file, processing includes as specified above.
        :param filename: filename to read. Mandatory.
        :param section:  if provided, then only read [default] and this section
        """
        self.seen_files.add(os.path.abspath(filename))
        with open(filename, "r") as f:
            #print(f"{self} reading {filename} onlySection={onlySection}")
            currentDirectory = os.path.dirname(os.path.abspath(filename))
            seen_sections = set()
            currentSection = HEAD_SECTION
            if onlySection is None:
                self.sections[HEAD_SECTION] = list()
            for line in f:
                # Check for new section
                m = SECTION_RE.search(line)
                if m:
                    currentSection = m.group(1).lower()
                    if currentSection in seen_sections:
                        raise ValueError(f"{seen_sections} appears twice in {filename}")
                    seen_sections.add(currentSection)

                if onlySection is not None:
                    if (onlySection.lower() != currentSection.lower()) and (currentSection!=DEFAULT_SECTION):
                        #print(f"{self} reading {filename} skipping line in section {currentSection}")
                        continue

                # Add the current section if it is not in the orderedDictionary.
                # Note that the section may be here even if it is not in seen_sections, as it
                # may have been imported as part of an INCLUDE in the [default] section.
                if currentSection not in self.sections:
                    self.sections[currentSection] = list()
                    self.sections[currentSection].append(line)
                    continue

                # Looks like we should just add the line
                self.sections[currentSection].append(line)
            #
            # Once every section has been processed, it's time to handle the includes.
            # We have to process the includes *after* the entire section is read, so that variables
            # in the included file can be shadowed. We could process each section as it is finished,
            # but it is simpler to do it here

            # First we see if there is an include in the default section.
            # INCLUDES there are special, because they can create new sections.
            default_include_files = []
            if DEFAULT_SECTION in self.sections:
                lines = list()
                for line in self.sections[DEFAULT_SECTION]:
                    theIncludeFile = getIncludeFile(line)
                    if theIncludeFile:
                        h2 = HCP()
                        theIncludePath = os.path.join(currentDirectory, theIncludeFile)
                        h2.read(theIncludePath)
                        self.seen_files.update(h2.seen_files)

                        # And make sure that for each of the sections in h2, we have a corresponding section in the current file
                        # provided that we were not asked to read a specific section. In that case, we only add [default] and that section
                        for includedSection in h2.sections:
                            if ((includedSection not in self.sections) and
                                (len(h2.sections[includedSection])>0) and
                                    ((onlySection is None) or (includedSection==onlySection) or (includedSection==DEFAULT_SECTION))):

                                lines = []
                                lines.append(h2.sections[includedSection][0])
                                self.sections[includedSection] = lines

                        # Finally add this file to the list of include files in the default section
                        default_include_files.append(theIncludeFile)

            # Now for each of the sections (including the default section, which no longer requires special processing),
            # See if there is an INCLUDE= statement.
            # If there is:
            #  - get a list of current options in this section
            #  - for each line in the included file, add it without comments if it isn't shadowed,
            #  - otherwise add it as a comment, indicating that it is shadowed.
            for section in self.sections:
                #print(f"{self} reading {filename} *** section {section}")

                newlines = list()
                optionsForThisSection = getAllOptions(self.sections[section])
                linesInSection = self.sections[section]
                #
                # If we are not in the [default] section, add an INCLUDE= directive for the default include files
                # to this section
                if (section != DEFAULT_SECTION) and (section != HEAD_SECTION):
                    for fname in default_include_files:
                        linesInSection.append(f"INCLUDE={fname} ; from [default]\n")

                for line in self.sections[section]:
                    theIncludeFile = getIncludeFile(line)
                    if theIncludeFile:
                        newlines.append(f';{line}')  # {line} already ends with \n
                        h2 = HCP()
                        theIncludePath = os.path.join(currentDirectory, theIncludeFile)
                        h2.read(theIncludePath, onlySection=section)
                        self.seen_files.update(h2.seen_files)

                        #print(f"Read {theIncludeFile} section {section} got {h2.sections}")

                        if (section in h2.sections) and (len(h2.sections[section])>0):
                            newlines.append(f'; begin include from {theIncludeFile}\n')
                            for iLine in h2.sections[section][1:]:  # do not include the section label
                                iLineOption = getOption(iLine)
                                if iLineOption and (iLineOption in optionsForThisSection):
                                    newlines.append(f'; [SHADOWED] {iLine}\n')
                                else:
                                    newlines.append(iLine)
                                    optionsForThisSection.add(iLineOption)
                            newlines.append(f'; end include from {theIncludeFile}\n')
                    else:
                        newlines.append(line)
                assert len(newlines) >= len(self.sections[section])
                self.sections[section] = newlines
            #
            # Done with reading file
            #
        return

    def asString(self):
        s = io.StringIO()
        for (section, lines) in self.sections.items():
            s.write("".join(lines))  # get the lines
        return s.getvalue()


class HierarchicalConfigParser(ConfigParser):
    """
    Similar to a normal ConfigParser, but implements the INCLUDE= statement.
    """

    def read(self, filenames, validate=False):
        """
        Reads a file performing the includes legographically, then parses the file with ConfigParser.
        :param filenames: - if a scalar, the name of the file to read.
                          - if a list, a list of files to try in order, until one is found.
        """
        filenames_ = filenames
        if isinstance(filenames, str) or isinstance(filenames, bytes):
            filenames = [filenames]
        for filename in filenames:
            if os.path.exists(filename):
                self.input_filename = filename
                self.hcp = HCP()
                self.hcp.read(filename)
                self.seen_files = self.hcp.seen_files

                # Now that we have the file expanded, read it as a string
                try:
                    self.read_string(self.hcp.asString())
                except (DuplicateOptionError, DuplicateSectionError) as e:
                    print(str(e), file=sys.stderr)
                    print("Internal Error:", file=sys.stderr)
                    print(self.hcp.asString(), file=sys.stderr)
                    raise e
                if validate:
                    self.validate()
                return
        raise FileNotFoundError(filenames_)

    def write(self, fileobject, **kwargs):
        fileobject.write(self.hcp.asString())

    def asString(self):
        return self.hcp.asString()

    def validate(self):
        """Look for validation rules and validate them. Validation rules are the option name followed by a '.v'"""
        for section in self.sections():
            for option in self[section]:
                required = False
                logging.warning("section %s option %s", section, option)
                if option.endswith('.required'):
                    required = self[section][option].lower()[0] in ['1', 't']
                    option_name = option[:-len(".required")]
                if required and option_name not in self[section]:
                    raise RequiredOptionMissing(option_name)
                if option.endswith('.re'):
                    logging.warning("checking re")
                    option_name = option[:-len(".re")]
                    if option_name in self[section]:
                        option_value = self[section][option_name].strip()
                        m = re.match(self[section][option], self[section][option_name])
                        logging.warning("m=%s", m)
                        if not m:
                            raise RegularExpressionValidationFailure(
                                f" section '{section}' option '{option_name}' value "
                                f"'{self[section][option_name]}' does not match regular expression '{self[section][option]}'")
                        if m.span()[1] != len(option_value):
                            raise RegularExpressionValidationFailure(
                                f" section '{section}' option '{option_name}' value "
                                f"'{self[section][option_name]}' contains extra characters. "
                                f"(expected {m.span()[1]} characters, got {len(option_value)}")
                    else:
                        logging.warning("validated option %s is not present", option_name)


if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("configfile", help="read and print the config file")
    args = parser.parse_args()

    hcp = HierarchicalConfigParser()
    hcp.read(args.configfile)
    hcp.write(sys.stdout)
