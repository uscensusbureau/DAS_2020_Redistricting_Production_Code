#!/usr/bin/env python3.5
# driver.py
#
# William N. Sexton and Simson L. Garfinkel
#
# Major Modification log:
#  2018-06-12  bam - refactored DAS to modularize code found in the run function
#  2017-12-10  slg - refactored the creation of objects for the DAS() object.
#  2017-11-19  slg - rewrite for abstract modular design, created experiment runner
#  2017-08-10  wns - initial framework working
#  2017-07-20  slg - created file

""" This is the main driver for the Disclosure Avoidance Subsystem (DAS).
    It executes the disclosure avoidance programs:
    it runs a setup module and data reader, runs the selected DAS engine,
    calls the output writer, and evaluates the output against the input.

    For systems that use Apache Spark, the driver run command is:

        spark-submit driver.py path/to/config.ini

    For systems that do not use Spark, the driver run command is:

        python3 driver.py path/to/config.ini

       or:

        python3 path/to/driver.py  config.ini

    Note that the driver.py can be included and run in another program.

"""

import sys
import os
import datetime
import json
import logging
import logging.handlers
import re
import time
import zipfile
import numpy
import __main__


from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from configparser import ConfigParser, NoOptionError, NoSectionError
from fractions import Fraction

UNKNOWN_VARIABLES_IGNORE = ['APPLICATIONID']

# DAS-specific libraries
try:
    import ctools
    import ctools.clogging as clogging
    import experiment
    import dfxml_writer
    from certificate.bom import get_bom
    from ctools.hierarchical_configparser import HierarchicalConfigParser
except ModuleNotFoundError:
    # Try relative to parent.
    # This is required when running out of the ZIP file on the spark worker
    #
    import das_framework.ctools as ctools
    import das_framework.ctools.clogging as clogging
    import das_framework.experiment as experiment
    import das_framework.dfxml_writer as dfxml_writer
    from das_framework.certificate.bom import get_bom
    from das_framework.ctools.hierarchical_configparser import HierarchicalConfigParser



DEFAULT = 'DEFAULT'
ENVIRONMENT = "ENVIRONMENT"
SETUP = "setup"
READER = "reader"
ENGINE = "engine"
ERROR_METRICS = "error_metrics"
WRITER = "writer"
VALIDATOR = "validator"
TAKEDOWN = "takedown"
RUN_SECTION = "run"

# LOGGING
LOGGING_SECTION = 'logging'
LOGFILENAME_OPTION = 'logfilename'
LOGLEVEL_OPTION = 'loglevel'
LOGFOLDER_OPTION = 'logfolder'

ROOT = 'root'  # where the experiment is running
LOGFILENAME = 'logfilename'  #
DEFAULT_LOGFILENAME = 'das'
OUTPUT_FNAME = 'output_fname'
OUTPUT_DIR = "output_dir"

# EXPERIMENT values
EXPERIMENT = 'experiment'
RUN_EXPERIMENT_FLAG = "run_experiment_flag"
EXPERIMENT_SCAFFOLD = 'scaffold'
EXPERIMENT_DIR = 'dir'  # the directory in which the experiment is taking place
EXPERIMENT_CONFIG = 'config'  # the name of the configuration file
EXPERIMENT_XLABEL = 'xlabel'  # what to label the X axis
EXPERIMENT_YLABEL = 'ylabel'  # what to label the Y axis
EXPERIMENT_GRID = 'grid'  # Draw the grid? True/False
EXPERIMENT_GRAPH_FNAME = 'graph_fname'  # filename for figure we are saving
EXPERIMENT_GRAPH_DATA_FNAME = 'graph_data_fname'  # Filename for the graph data
EXPERIMENT_AVERAGEX = 'averagex'  # should all Y values for a certain X be averaged?
EXPERIMENT_TITLE = 'title'
EXPERIMENT_DRAW_LEGEND = 'draw_legend'
EXPERIMENT_GRAPHX = 'graphx'
EXPERIMENT_GRAPHY = 'graphy'

RUN_TYPE = 'run_type'
DEV_RUN_TYPE = 'dev'
PROD_RUN_TYPE = 'prod'
DEV_RUN_TYPE_PATH = f'$DAS_S3ROOT/runs/{DEV_RUN_TYPE}/$JBID'
PROD_RUN_TYPE_PATH = f'$DAS_S3ROOT/runs/{PROD_RUN_TYPE}/$JBID'
OUTPUT_PATH = 'output_path'
WRITER = "writer"

CUI_LABEL = '(CUI' + r'//' + 'SP-CENS) '


def config_apply_environment(config):
    """Look for the ENVIRONMENT section and apply the variables to the environment
    Note: By default, section names are case sensitive, but variable names are not.
    Because the convention is that environment variables are all upper-case, we uppercase them.

    Then put all of the environment variables into CONFIG. That's so they will be available on the driver
    """
    if ENVIRONMENT in config:
        for var in config[ENVIRONMENT]:
            name = var.upper()
            value = config[ENVIRONMENT][var]
            logging.info("EXPORT {}={}".format(name, value))
            os.environ[name] = value
    else:
        config.add_section(ENVIRONMENT)

    # copy over the environment variables specified in [setup] environment
    for var in config.get(SETUP, ENVIRONMENT, fallback='').split(","):
        if var in os.environ:
            config.set(ENVIRONMENT, var, os.environ[var])


VARIABLE_RE = re.compile(r"([$][A-Za-z0-9_]+)")


def config_validate(config, extra_sections=None):
    """Make sure mandatory sections exist and that all $variables are defined in the environment"""
    if extra_sections is None:
        extra_sections = []

    for section in [SETUP, READER, ENGINE, WRITER, VALIDATOR, TAKEDOWN] + extra_sections:
        if section not in config:
            logging.error("config file missing section [{}]".format(section))
            raise RuntimeError("config file missing section [{}]".format(section))

    errors = []
    for section in config.sections():
        logging.info(f"Validating config section [{section}]")
        for option in config.options(section):
            val = config.get(section, option)
            for var in VARIABLE_RE.findall(val):
                if var[1:] not in os.environ and var[1:] not in UNKNOWN_VARIABLES_IGNORE:
                    logging.error(f"[{section}] option {option} variable {var} not in environment")
                    errors.append((section, option, val, var))
    if errors:
        print("Current Environment:", file=sys.stderr)
        for (key, val) in sorted(os.environ.items()):
            print(f"   {key}={val}", file=sys.stderr)
        print("\nUnknown variables:", file=sys.stderr)
        message = "\nUnknown variables in config file:\n"
        for (section, option, val, var) in errors:
            message += f"   [{section}] {option}: {val}   ({var} is undefined)\n"
        raise ValueError(message)


### numpy integers can't be serialized; we need our own serializer
### https://stackoverflow.com/questions/27050108/convert-numpy-type-to-python/27050186#27050186
class DriverEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(DriverEncoder, self).default(obj)


def strtobool(val, default=None):
    if val in ["", None] and default is not None:
        return default
    v = val.lower()
    if v in ['y', 'yes', 't', 'true', 'on', '1']:
        return True
    if v in ['n', 'no', 'f', 'false', 'off', '0']:
        return False
    raise ValueError(f"The value {v} cannot be converted to boolean")


class AbstractDASModule:
    def __init__(self, *, name, config, das, setup=None, output_path=None, **kwargs):
        assert isinstance(config, ConfigParser)
        self.name   = name
        self.config = config
        self.setup  = setup
        self.das    = das
        self.output_path = output_path

    def log_and_print(self, *args, **kwargs):
        self.das.log_and_print(*args, **kwargs)

    def log_warning_and_print(self, *args, **kwargs):
        self.das.log_warning_and_print(*args, **kwargs)

    def running_time(self):
        return self.das.running_time()

    def add_output_path(self, path):
        self.das.output_paths.append(path)

    def annotate(self, *args, **kwargs):
        self.das.annotate(*args, **kwargs)

    @staticmethod
    def do_expandvars(val, expandvars):
        if expandvars:
            val = val.replace("$$", str(os.getpid()))
            val = os.path.expandvars(val)
        return val

    def getconfig(self, key, default=None, section=None, expandvars=True):
        """if expandvars is None and key ends with _fname, expandvars is set to True.
        Otherwise it is set to false.
        """
        if section is None:
            section = self.name
        try:
            val = self.do_expandvars(self.config.get(section, key), expandvars)
            logging.debug("config[{}][{}]={}".format(section, key, val))
            return val

        except (NoOptionError, NoSectionError) as err:

            if default is not None:
                val = self.do_expandvars(str(default), expandvars)
                logging.info("config[{}][{}] not present; returning default {}".format(section, key, val))
                return val
            msg = "config[{}][{}] does not exist and no default provided".format(section, key)
            logging.error(msg)
            err.message = msg + " :: " + err.message
            raise err

    def getint(self, key, section=None, **kwargs):
        val = self.getconfig(key, section=section, **kwargs)
        if section is None:
            section = self.name
        try:
            intval = int(val)
            return intval
        except ValueError:
            err_msg = f"Config option \"[{section}]/{key}\" value ( \"{val}\" ) cannot be converted to int"
            logging.error(err_msg)
        raise ValueError(err_msg)

    def getfloat(self, key, section=None, **kwargs):
        val = self.getconfig(key, section=section, **kwargs)
        if section is None:
            section = self.name
        try:
            floatval = float(val)
            return floatval
        except ValueError:
            err_msg = f"Config option \"[{section}]/{key}\" value ( \"{val}\" ) cannot be converted to float"
            logging.error(err_msg)
        raise ValueError(err_msg)

    def getfraction(self, key, section=None, **kwargs):
        val = self.getconfig(key, section=section, **kwargs)
        if section is None:
            section = self.name
        try:
            fracval = Fraction(val)
            return fracval
        except ValueError:
            err_msg = f"Config option \"[{section}]/{key}\" value ( \"{val}\" ) cannot be converted to Fraction"
            logging.error(err_msg)
        raise ValueError(err_msg)

    def getboolean(self, key, default=None, section=None):
        # https://stackoverflow.com/questions/715417/converting-from-a-string-to-boolean-in-python
        # Language flaw!
        val = self.getconfig(key, section=section, default=default)
        if section is None:
            section = self.name
        try:
            boolval = strtobool(val, default=default)
            return boolval
        except ValueError:
            err_msg = f"Config option \"[{section}]/{key}\" value ( \"{val}\" ) cannot be converted to boolean"
            logging.error(err_msg)
        raise ValueError(err_msg)

    def getiter(self, key, sep=',', **kwargs):
        return map(lambda s: s.strip(), re.split(sep, self.getconfig(key, **kwargs)))

    def gettuple(self, key, default=None, **kwargs):
        try:
            tuple_val = tuple(self.getiter(key, **kwargs))
            return tuple_val
        except (NoOptionError, NoSectionError) as err:
            if default is not None:
                return default
            raise err

    def getiter_of_ints(self, key, **kwargs):
        return map(int, self.getiter(key, **kwargs))

    def gettuple_of_ints(self, key, **kwargs):
        try:
            return tuple(self.getiter_of_ints(key, **kwargs))
        except ValueError as err:
            err_msg = f"Some of elements of \"{self.getconfig(key, **kwargs)}\" cannot be converted to int; " + str(err.args[0])
            logging.error(err_msg)
        raise ValueError(err_msg)

    def getiter_of_floats(self, key, **kwargs):
        return map(float, self.getiter(key, **kwargs))

    def gettuple_of_floats(self, key, **kwargs):
        try:
            return tuple(self.getiter_of_floats(key, **kwargs))
        except ValueError as err:
            err_msg = f"Some of elements of \"{self.getconfig(key)}\" cannot be converted to float; " + str(err.args[0])
        logging.error(err_msg)
        raise ValueError(err_msg)

    def getiter_of_fractions(self, key, **kwargs):
        return map(Fraction, self.getiter(key, **kwargs))

    def gettuple_of_fractions(self, key, **kwargs):
        try:
            return tuple(self.getiter_of_fractions(key, **kwargs))
        except ValueError as err:
            err_msg = f"Some of elements of \"{self.getconfig(key)}\" cannot be converted to Fraction" + str(err.args[0])
        logging.error(err_msg)
        raise ValueError(err_msg)

    def getiter_of_fraction2floats(self, key, **kwargs):
        return map(float, self.gettuple_of_fractions(key, **kwargs))

    def gettuple_of_fraction2floats(self, key, **kwargs):
        try:
            return tuple(self.getiter_of_fraction2floats(key, **kwargs))
        except ValueError as err:
            err_msg = f"Some of elements of \"{self.gettuple_of_fractions(key, **kwargs)}\" cannot be converted to Fraction then to float; " + str(err.args[0])
        logging.error(err_msg)
        raise ValueError(err_msg)

    def getconfitems(self, section):
        """
        !! ONLY WORKS with regular ConfigParser! Doesn't work with HierarchicalConfigParser, since it explicitly adds the stuff from
        the DEFAULT section to other sections
        Filters out DEFAULTs from config items of the section,
        :param section: section of config files
        :return: iterator of config items in the section
        """
        if isinstance(self.config, HierarchicalConfigParser):
            self.log_and_print("Trying to filter out [DEFAULT] section items from config that is not regular ConfigParser, but HierarchicalConfigParser. If this causes "
                               "problems, try --nohierconfig command option when starting the DAS.")
        if self.config.has_section(section):
            return list(filter(lambda item: item not in self.config.items(self.config.default_section), self.config.items(section)))
        else:
            return {}


class AbstractExperiment(AbstractDASModule):
    def __init__(self, das=None, **kwargs):
        super().__init__(das=das, **kwargs)
        self.das = das

    def runExperiment(self):
        return None


class AbstractDASExperiment(AbstractExperiment):
    """This is the experiment driver. This is where the loops will be done.
    It brings in the experiment module. Do not import this at top level to avoid
    It being imported if we are shipped off to spark.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.loops = experiment.build_loops(self.config)
        self.state = experiment.initial_state(self.loops)

    def increment_state(self):
        """
        Given a set of loops and a state, increment the state to the next position, handling roll-over.
        Return the next state. If we are finished, return None.
        """
        self.state = experiment.increment_state(self.loops, self.state)
        return self

    def substitute_config(self):
        """Generate a new config given a current config and a state of the loops."""
        for rank in range(len(self.loops)):
            section = self.loops[rank][0]
            var = self.loops[rank][1]
            self.das.config[section][var] = str(self.state[rank])

        return self

    def runExperiment(self):
        scaffold = Scaffolding(config=self.config)
        scaffold.experimentSetup()

        while self.state is not None:
            self.substitute_config()
            DAS(config=self.config).run()
            self.increment_state()

        scaffold.experimentTakedown()

        return None

    def experimentSetup(self):
        pass

    def experimentTakedown(self):
        pass


class AbstractDASSetup(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def setup_func(self):
        """Setup Function. Note special name."""
        return None


class AbstractDASReader(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willRead(self):
        return True

    def read(self):
        """Read the data; return a reference. Location to read specified in config file."""
        return None  # no read data in prototype

    def didRead(self):
        return


class AbstractDASEngine(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willRun(self):
        return True

    def run(self, original_data):
        """Nothing to do in the prototype"""
        return

    def didRun(self):
        return


class AbstractDASErrorMetrics(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willRun(self):
        return True

    def run(self, data):
        """Nothing to do in the prototype"""
        return None

    def didRun(self):
        return


class AbstractDASWriter(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willWrite(self):
        return True

    def write(self, privatized_data):
        """Return the written data"""
        return privatized_data  # by default, just return the privatized_data, nothing is written

    def didWrite(self):
        return


class AbstractDASValidator(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willValidate(self):
        return True

    def validate(self, original_data, written_data_reference, **kwargs):
        """No validation in prototype"""
        return True

    def didValidate(self):
        return

    def storeResults(self, data):
        """data is a dictionary with results. The default implementation
        stores them in a file called 'results' specified in the config file"""
        with open(self.getconfig('results_fname', default='results.json'), "a") as f:
            json.dump(data, f, cls=DriverEncoder)
            f.write("\n")


class AbstractDASTakedown(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willTakedown(self):
        return True

    def takedown(self):
        """No takedown in prototype"""
        return True

    def removeWrittenData(self, reference):
        """Delete what's referred to by reference. Do not call superclass"""
        raise RuntimeError("No method defined to removeWrittenData({})".format(reference))

    def didTakedown(self):
        return True


class Scaffolding(object):
    """ Scaffolding for an experiment"""

    def __init__(self, config):
        assert isinstance(config, ConfigParser)
        self.config = config
        scaffoldstr = config[EXPERIMENT].get(EXPERIMENT_SCAFFOLD, None)
        if not scaffoldstr:
            logging.info("No scaffolding")
            self.scaffold = None
            return
        (scaffold_file, scaffold_class_name) = scaffoldstr.split(".")
        try:
            scaffold_module = __import__(scaffold_file) if scaffold_file else None
        except ModuleNotFoundError as e:
            logging.exception("Scaffolding import failed. current directory: {}".format(os.getcwd()))
            raise e
        self.scaffold = getattr(scaffold_module, scaffold_class_name)(config=config)

    def experimentSetup(self):
        if self.scaffold:
            self.scaffold.experimentSetup(self.config)

    def experimentTakedown(self):
        if self.scaffold:
            self.scaffold.experimentTakedown(self.config)


class DAS:
    """
    The Disclosure Avoidance System Class.
    The DAS() class is a class that returns a singleton of the DAS._DAS class, which is where the action takes place.
    """
    instance = None

    def __init__(self, *args, config, **kwargs):
        if not DAS.instance:
            DAS.instance = DAS._DAS(*args, config=config, **kwargs)
        else:
            reader_class_name, reader_module = DAS.instance.load_module(config, READER, READER, 'driver', 'AbstractDASReader')
            engine_class_name, engine_module = DAS.instance.load_module(config, ENGINE, ENGINE, 'driver', 'AbstractDASEngine')
            error_metrics_class_name, error_metrics_module = DAS.instance.load_module(config, ERROR_METRICS, ERROR_METRICS, 'driver',
                                                                              'AbstractDASErrorMetrics')
            writer_class_name, writer_module = DAS.instance.load_module(config, WRITER, WRITER, 'driver', 'AbstractDASWriter')
            validator_class_name, validator_module = DAS.instance.load_module(config, VALIDATOR, VALIDATOR, 'driver', 'AbstractDASValidator')

            logging.debug(
                "classes: {} {} {} {} {}".format(engine_class_name, error_metrics_class_name,
                                                       reader_class_name, writer_class_name, validator_class_name))
            # Create the instances
            logging.debug(
                "modules: {} {} {} {} {}".format(engine_module, error_metrics_module, reader_module,
                                                       writer_module, validator_module))
            DAS.instance.writer = getattr(writer_module, writer_class_name)(config=config,
                                                                            setup=DAS.instance.setup_data, name=WRITER,
                                                                            das=DAS.instance)
            DAS.instance.reader = getattr(reader_module, reader_class_name)(config=config, setup=DAS.instance.setup_data, name=READER, das=DAS.instance)
            DAS.instance.engine = getattr(engine_module, engine_class_name)(config=config, setup=DAS.instance.setup_data, name=ENGINE, das=DAS.instance)
            DAS.instance.error_metrics = getattr(error_metrics_module, error_metrics_class_name)(config=config, setup=DAS.instance.setup_data, name=ERROR_METRICS,
                                                                                         das=DAS.instance)
            DAS.instance.validator = getattr(validator_module, validator_class_name)(config=config, setup=DAS.instance.setup_data, name=VALIDATOR, das=DAS.instance)
            logging.debug("DAS modules recreated")

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name, value):
        setattr(self.instance, name, value)

    class _DAS:
        def __init__(self, *, config, dfxml_writer=None, logfilename=None, printing_disabled=False, args=None, creating_bom=False, delegate=None):
            """ Initialize a DAS given a config file. This creates all of the objects that will be used"""

            assert isinstance(config, ConfigParser)
            self.args = args
            self.config = config
            self.dfxml_writer = dfxml_writer
            self.logfilename = logfilename
            self.output_paths = []  # all output paths
            self.t0 = time.time()
            self.annotation_hooks = []  # called for annotate
            self.printing_disabled = printing_disabled
            self.delegate   = delegate
            args_create_bom = args.print_bom or args.make_release if args is not None else False
            self.creating_bom = creating_bom or args_create_bom

            # Create output_path
            # output_path_raw = config[CC.WRITER][CC.OUTPUT_PATH]
            # run_type = config[CC.WRITER][CC.RUN_TYPE]

            # Get the input file and the class for each
            logging.debug("Reading filenames and class names from config file")


            # This section can possibly combined with the following section importing the modules and creating the objects,
            # so that the default objects can be created by just using AbstractDASxxxxxx() constructor
            setup_class_name, setup_module = self.load_module(config, SETUP, SETUP, 'driver', 'AbstractDASSetup')
            reader_class_name, reader_module = self.load_module(config, READER, READER, 'driver', 'AbstractDASReader')
            engine_class_name, engine_module = self.load_module(config, ENGINE, ENGINE, 'driver', 'AbstractDASEngine')
            error_metrics_class_name, error_metrics_module = self.load_module(config, ERROR_METRICS, ERROR_METRICS, 'driver', 'AbstractDASErrorMetrics')
            writer_class_name, writer_module = self.load_module(config, WRITER, WRITER, 'driver', 'AbstractDASWriter')
            validator_class_name, validator_module = self.load_module(config, VALIDATOR, VALIDATOR, 'driver', 'AbstractDASValidator')
            takedown_class_name, takedown_module = self.load_module(config, TAKEDOWN, TAKEDOWN, 'driver', 'AbstractDASTakedown')

            logging.debug(
                "classes: {} {} {} {} {} {} {}".format(setup_class_name, engine_class_name, error_metrics_class_name,
                                                       reader_class_name, writer_class_name, validator_class_name, takedown_class_name))
            # Create the instances
            logging.debug(
                "modules: {} {} {} {} {} {} {}".format(setup_module, engine_module, error_metrics_module, reader_module,
                                                       writer_module, validator_module, takedown_module))
            logging.info("Creating and running DAS setup object")
            setup_obj = getattr(setup_module, setup_class_name)(config=config, name=SETUP, das=self)

            self.setup_data = setup_obj.setup_func()
            logging.debug("DAS setup returned {}".format(self.setup_data))

            # Now create the other objects
            self.writer = getattr(writer_module, writer_class_name)(config=config, setup=self.setup_data, name=WRITER,
                                                                    das=self)
            self.reader = getattr(reader_module, reader_class_name)(config=config, setup=self.setup_data, name=READER, das=self)
            self.engine = getattr(engine_module, engine_class_name)(config=config, setup=self.setup_data, name=ENGINE, das=self)
            self.error_metrics = getattr(error_metrics_module, error_metrics_class_name)(config=config, setup=self.setup_data, name=ERROR_METRICS,
                                                                                         das=self)
            self.validator = getattr(validator_module, validator_class_name)(config=config, setup=self.setup_data, name=VALIDATOR, das=self)
            self.takedown = getattr(takedown_module, takedown_class_name)(config=config, setup=self.setup_data, name=TAKEDOWN, das=self)

            logging.debug("DAS object complete")

        @staticmethod
        def load_module(config, section, option, default_file, default_class):
            try:
                (module_file, module_class_name) = config.get(section=section, option=option).rsplit(".", 1)
            except (NoSectionError, NoOptionError) as e:
                msg = (f"Option {option} in section [{section}] not found when specifying module to load, substituting default {default_file}.{default_class}\n{e}")
                print(msg)
                logging.info(msg)
                (module_file, module_class_name) = (default_file, default_class)
            try:
                module = __import__(module_file, fromlist=[module_class_name])
            except ImportError as e:
                err_msg = f"Module {module_file} import failed.\nCurrent directory: {os.getcwd()}\nFile:{__file__}\nsys.path:{sys.path}\n{e.args[0]}"
                logging.error(err_msg)
                raise ImportError(err_msg)
            logging.debug("__import__ file: {}".format(module_file))
            return module_class_name, module

        def timestamp(self, message):
            try:
                self.dfxml_writer.timestamp(message)
            except AttributeError:
                pass
            logging.info(message)

        def make_bom_only(self):
            """Is this specific DAS making a bom? If so, do not launch Java or other expensive operations, just load the modules and exit."""
            return self.creating_bom

        def log_and_print_cui(self, log_func, print_func, message, cui=False):
            assert os.getenv("ISMASTER")!='false'
            if not cui:
                log_func(message)
            else:
                message = CUI_LABEL + " " + message
            if not self.printing_disabled:
                print_func(message)

        def log_and_print(self, message, cui=False):
            assert os.getenv("ISMASTER")!='false'
            self.log_and_print_cui(lambda m: logging.info(m), lambda m: print(f"INFO: {m}"), message, cui=cui)

        def log_warning_and_print(self, message, cui=False):
            # logging.warning may generate a console output
            assert os.getenv("ISMASTER")!='false'
            self.log_and_print_cui(lambda m: logging.warning(m), lambda m: print(f"WARNING: {m}"), message, cui=cui)

        def add_annotation_hook(self, hook):
            self.annotation_hooks.append(hook)

        def annotate(self, message, verbose=True):
            """
            Annotate the DFXML file. If verbose=True, also print.
            Must be run on the master node.
            """
            assert os.getenv("ISMASTER")!='false'
            if verbose:
                when  = time.asctime()[11:16]
                whent = round(self.running_time(),2)
                print(f"ANNOTATE: {when} t={whent} {message}")
            self.timestamp("ANNOTATE: "+message)
            for hook in self.annotation_hooks:
                hook(message)

        def runReader(self):
            self.timestamp("runReader: Creating and running DAS reader")
            if not self.reader.willRead():
                logging.info("self.reader.willRead() returned false")
                raise RuntimeError("reader willRead() returned False")

            if hasattr(self.delegate,'willRunReader'):
                self.delegate.willRunReader(self)
            original_data = self.reader.read()
            self.reader.didRead()
            if hasattr(self.delegate,'didRunReader'):
                self.delegate.didRunReader(self)
            logging.debug("original_data={}".format(original_data))
            return original_data

        def runEngine(self, original_data):
            self.timestamp("runEngine: Creating and running DAS engine")
            if not self.engine.willRun():
                logging.info("self.engine.willRun() returned false")
                raise RuntimeError("engine willRun() returned False")
            if hasattr(self.delegate, 'willRunEngine'):
                self.delegate.willRunEngine(self)
            privatized_data = self.engine.run(original_data)
            if hasattr(self.delegate, 'didRunEngine'):
                self.delegate.didRunEngine(self)
            self.engine.didRun()
            logging.debug("privatized_data={}".format(privatized_data))
            return privatized_data

        def runErrorMetrics(self, privatized_data):
            self.timestamp("runErrorMetrics: Creating and running DAS error_metrics")
            if not self.error_metrics.willRun():
                logging.info("self.error_metrics.willRun() returned false")
                raise RuntimeError("error_metrics willRun() returned False")
            if hasattr(self.delegate, 'willRunErrorMetrics'):
                self.delegate.willRunErrorMetrics(self)
            error_metrics_data = self.error_metrics.run(privatized_data)
            if hasattr(self.delegate, 'didRunErrorMetrics'):
                self.delegate.didRunErrorMetrics(self)
            logging.debug("Error Metrics data = {}".format(error_metrics_data))
            self.error_metrics.didRun()
            return error_metrics_data

        def runWriter(self, privatized_data):
            self.timestamp("runWriter: Creating and running DAS writer")
            if not self.writer.willWrite():
                logging.info("self.writer.willWrite() returned false")
                raise RuntimeError("engine willWrite() returned False")
            if hasattr(self.delegate, 'willRunWriter'):
                self.delegate.willRunWriter(self)
            written_data = self.writer.write(privatized_data)
            if hasattr(self.delegate, 'didRunWriter'):
                self.delegate.didRunWriter(self)
            logging.debug("written_data={}".format(written_data))
            self.writer.didWrite()
            return written_data

        def runValidator(self, original_data, written_data):
            self.timestamp("runValidator: Creating and running DAS validator")
            if not self.validator.willValidate():
                logging.info("self.validator.willValidate() returned false")
                raise RuntimeError("validator willValidate() returned False")
            if hasattr(self.delegate, 'willRunValidator'):
                self.delegate.willRunValidator(self)
            valid = self.validator.validate(original_data, written_data)
            if hasattr(self.delegate, 'didRunValidator'):
                self.delegate.didRunValidator(self)
            logging.debug("valid={}".format(valid))
            if not valid:
                logging.info("self.validator.validate() returned false")
                raise RuntimeError("Did not validate.")
            self.validator.didValidate()

            # If we were asked to get graphx and graphy, get it.
            data = {}
            if EXPERIMENT in self.config:
                for var in ['graphx', 'graphy']:
                    if var in self.config[EXPERIMENT]:
                        (a, b) = self.config[EXPERIMENT][var].split('.')
                        assert a == 'validator'
                        func = getattr(self.validator, b)
                        data[var] = func()

            # Finally take down
            return valid

        def runTakedown(self, written_data):
            self.timestamp("runTakedown: Creating and running DAS takedown")
            if not self.takedown.willTakedown():
                logging.info("self.takedown.willTakedown() returned false")
                raise RuntimeError("validator willTakedown() returned False")
            if hasattr(self.delegate, 'willRunTakedown'):
                self.delegate.willRunTakedown(self)
            self.takedown.takedown()
            if hasattr(self.delegate, 'didRunTakedown'):
                self.delegate.didRunTakedown(self)
            if self.takedown.getboolean("delete_output", False):
                logging.info("deleting output {}".format(written_data))
                self.takedown.removeWrittenData(written_data)
            self.takedown.didTakedown()

        def run(self):
            """ Run the DAS. Returns data collected as a dictionary if an EXPERIMENT section is specified in the config file."""

            # First run the engine and write the results
            # Create the instances is now done when running

            self.timestamp("run")

            if hasattr(self.delegate, 'willRun'):
                self.delegate.willRun(self)

            original_data = self.runReader()
            privatized_data = self.runEngine(original_data)
            error_metrics_data = self.runErrorMetrics(privatized_data)
            written_data = self.runWriter(privatized_data)
            valid = self.runValidator(original_data, written_data)
            self.runTakedown(written_data)
            if hasattr(self.delegate, 'didRun'):
                self.delegate.didRun(self)
            data = {}
            return data

        def running_time(self):
            return time.time() - self.t0


# Only include with these suffixes
BOM_INCLUDE_SUFFIXES = ['Makefile', '.md', '.doc', '.pdf', '.docx']
BOM_ALLOW_SUFFIXES = ['.py', '.ini']
BOM_ALLOWED_SUFFIXES = set(BOM_INCLUDE_SUFFIXES + BOM_ALLOW_SUFFIXES)
BOM_OMIT_DIRS = ['hdmm', 'legacy_code', 'etl_2020', '.cache', '__pycache__', '.git', '.github']


def get_das_dir():
    return os.path.dirname(os.path.abspath( __main__.__file__ ))


def bom_files(*, config, das=None, args=None):
    """
    Returns the bill of materials, relative to the current directory.
    Includes any files with BOM_SUFFIXES and no files in BOM_OMIT_DIRS

    BOM is for the given DAS object. If none is provided, make one.
    The only purpose of making the das object is to assure that the config files are loaded.
    """
    if das is None:
        if args is None:
            raise ValueError("args must be provided if das is None")
        logging.disable(sys.maxsize)
        das = DAS(config=config, args=args, printing_disabled=True, creating_bom=True)

    system_prefix     = "/".join(sys.executable.split("/")[0:-2])
    suppress_prefixes = [system_prefix, '/usr/lib']

    full_paths = set()

    # start with the bom_files from the certificate printer
    for (name, path, ver, bytecount) in get_bom(content=False):
        if not isinstance(path, str):
            continue
        if any([path.startswith(prefix) for prefix in suppress_prefixes]):
            continue
        if len(path) > 0 and any([path.endswith(suffix) for suffix in BOM_ALLOWED_SUFFIXES]):
            full_paths.add(path)

    # add in the config files
    for path in config.seen_files:
        full_paths.add(path)

    # walk the file system from the DAS_DIR and find any filenames with the requested suffixes
    DAS_DIR = get_das_dir()

    for root, dirs, files in os.walk(DAS_DIR):
        if any([(dirpart in BOM_OMIT_DIRS) for dirpart in root.split('/')]):
            continue

        for fname in files:
            if any([fname.endswith(suffix) for suffix in BOM_INCLUDE_SUFFIXES]):
                full_paths.add(os.path.join(root, fname))


    pruned_full_paths = [ (path[len(DAS_DIR)+1:] if path.startswith(DAS_DIR) else path)
                          for path in full_paths]
    return list(sorted(pruned_full_paths))


def print_bom(*, config, das=None, args=None, file=sys.stdout):
    """Print a bom
    :param config: the config file
    :param das: the das object
    :param args: any special arguments
    :param file: where the output goes
    """
    for path in bom_files(config=config, das=das, args=args):
        print(path, file=file)


def make_release(*, config, zipfilename, args, verbose=False):
    """Given a config and a set of arguments, create a named zipfile
    @param config - INPUT - the loaded config to use (loaded)
    @param zipfilename - OUTPUT - where to write the zip file
    @param args    - INPUT - arguments provided to bom_files. Typically the args from ArgumentParser.
    """
    if os.path.exists(zipfilename):
        os.unlink(zipfilename)
    with zipfile.ZipFile(zipfilename, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename in bom_files(config=config, args=args):
            try:
                zf.write(filename, filename)
            except FileNotFoundError:
                logging.warning("Could not add file %s to zipfile",filename)
            else:
                if verbose:
                    print("{} -> {}".format(filename, zipfilename))
    return zipfilename


def main_setup(additional_args = []):
    """
    Setup the DAS system logging, parses arguments and loads the configuration file,
    returning the args and config objects.
    """
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("config", help="Main Config File")
    parser.add_argument("--print_bom", help="Output a bill of materials", action='store_true')
    parser.add_argument("--make_release", help="Create a zip file with all of the files necessary to run the DAS. Similar to print_bom")
    parser.add_argument("--experiment",
                        help="Run an experiment according to the [experiment] section, with the results in this directory")
    parser.add_argument("--isolation", help="Specifies isolation mode for experiments",
                        choices=['sameprocess', 'subprocess'], default='sameprocess')
    parser.add_argument("--graphdata", help="Just draw the graph from the data that was already collected.",
                        action='store_true')
    parser.add_argument("--logfilename", help="Specify logfilename, otherwise auto-generate")
    parser.add_argument("--nohierconfig", help='Use regular Python configparser. ConfigParser instead of ctools.HierarchicalConfigParser',
                        action="store_true")
    parser.add_argument("--dump_config", help="dump the config file, then exit", action='store_true')
    parser.add_argument("--get", help="output the section:option:default from the config file, then exit")
    parser.add_argument("--dry-run", help="Dry run; do not run the algorithm", action='store_true')

    for (args,kwargs) in additional_args:
        parser.add_argument(*args, **kwargs)

    clogging.add_argument(parser)
    args = parser.parse_args()

    if not os.path.exists(args.config):
        raise RuntimeError("{} does not exist".format(args.config))

    if args.graphdata and args.experiment is None:
        parser.error("--graphdata requires --experiment")

    ###
    ### Read the configuration file and handle config-related options
    ###

    config = ConfigParser() if args.nohierconfig else HierarchicalConfigParser()
    config.read(args.config)

    if args.dump_config:
        config.write(sys.stdout)
        exit(0)

    if args.get:
        if args.get.count(":")!=2:
            raise ValueError("Specify section:option:default as the --get argument")
        (section, option, default) = args.get.split(":")
        if (section in config) and (option in config[section]):
            print(config[section][option])
        else:
            print(default)
        exit(0)

    ###
    ### Logging must be set up before any logging is done
    ### By default it is in the current directory, but if we run an experiment, put the logfile in that directory
    ### Added option to put logs in a subfolder specified in the config

    if not args.logfilename:
        isodate = datetime.datetime.now().isoformat()[0:19]
        if (config.has_section(LOGGING_SECTION)
            and config.has_option(LOGGING_SECTION, LOGFOLDER_OPTION)
            and config.has_option(LOGGING_SECTION, LOGFILENAME_OPTION)):
            args.logfilename = (f"{config[LOGGING_SECTION][LOGFOLDER_OPTION]}/"
                                f"{config[LOGGING_SECTION][LOGFILENAME_OPTION]}-{isodate}-{os.getpid()}.log")
        else:
            args.logfilename = f"{isodate}-{os.getpid()}.log"

    # CB: Code needs to be removed.
    # Left here for backward compatibility, to be removed in future versions
    if args.experiment:
        if not os.path.exists(args.experiment):
            os.makedirs(args.experiment)
        if not os.path.isdir(args.experiment):
            raise RuntimeError("{} is not a directory".format(args.experiment))
        config[config.default_section][ROOT] = args.experiment
        args.logfilename = os.path.join(args.experiment, args.logfilename)
        if EXPERIMENT not in config:
            config.add_section(EXPERIMENT)
        config[EXPERIMENT][RUN_EXPERIMENT_FLAG] = "1"

    # If we are making the BOM, make a DAS object so the config file gets processed, then make the bom and exit
    if args.print_bom:
        print_bom(config=config, args=args)
        exit(0)

    if args.make_release:
        make_release(config=config, zipfilename=args.make_release, args=args)
        print("Release: {}".format(args.make_release))
        exit(0)

    #
    #
    # Make sure the directory for the logfile exists. If not, make it.

    logdirname = os.path.dirname(args.logfilename)
    if logdirname and not os.path.exists(logdirname):
        os.mkdir(logdirname)

    clogging.setup(args.loglevel,
                   syslog=True,
                   filename=args.logfilename,
                   log_format=clogging.LOG_FORMAT,
                   syslog_format=clogging.YEAR + " " + clogging.SYSLOG_FORMAT)
    logging.info("Config path: {}".format(os.path.abspath(args.config)))
    return args, config


def main_make_das(args, config, **kwargs):
    """
    Creates the das object after determining whether to run in
    experiment mode based on config file.
    """
    #############################
    # Set up the logging
    #############################
    the_dfxml_writer = dfxml_writer.DFXMLWriter(filename=args.logfilename.replace(".log", ".dfxml"), prettyprint=True)
    logging.getLogger().addHandler(the_dfxml_writer.logHandler())

    #########################
    # Set up the experiment #
    #########################

    # if there is no experiment section in the config file, add one
    if EXPERIMENT not in config:
        config.add_section(EXPERIMENT)

    # If there is no run experiment flag in the config section, add it
    run_experiment = config[EXPERIMENT].getint(RUN_EXPERIMENT_FLAG, 0)
    if args.experiment:
        run_experiment = 1

    ### Now validate and apply the config file
    config_apply_environment(config)
    config_validate(config)

    #########################
    # Create the DAS object #
    #########################

    das = DAS(config=config, args=args, logfilename=args.logfilename, dfxml_writer=the_dfxml_writer, **kwargs)
    das.experiment = run_experiment

    return das


def main_run_das(das, shutdown_logging_on_exit:bool=True):
    """
    Run the DAS!
    :param shutdown_logging_on_exit: If True, execute logging.shutdown() after running the DAS.
           This can result in unexpected errors in logging if logging is used after main_run_das completes.
           Set to False if logging will be used after this method is run. Defaults to True.
    """
    logging.info("START {}".format(os.path.abspath(__file__)))

    #############################
    # DAS Running Section.
    # Option 1 - run_experiment
    # Option 2 - just run the das
    #############################
    logging.info("Config file:")
    for section in das.config.sections():
        logging.info(f"[{section}]")
        for option in das.config.options(section):
            logging.info(f"{option}: {das.config.get(section,option)}")
        logging.info("")

    if das.experiment:
        # set up the Experiment module
        logging.debug("== experiment ==")
        try:
            (experiment_file, experiment_class_name) = das.config[EXPERIMENT][EXPERIMENT].rsplit(".", 1)
        except KeyError:
            (experiment_file, experiment_class_name) = ('driver', 'AbstractDASExperiment')
        try:
            experiment_module = __import__(experiment_file, fromlist=[experiment_class_name])
        except ImportError as e:
            print("Module import failed.")
            print("current directory: {}".format(os.getcwd()))
            print("__file__: {}".format(__file__))
            raise e

        # Name "experiment" conflicts with imported module
        experiment_instance = getattr(experiment_module, experiment_class_name)(das=das, config=das.config, name=EXPERIMENT)
        logging.debug("Running DAS Experiment. Logfile: {}".format(das.logfilename))

        experiment_data = experiment_instance.runExperiment()

    else:
        #### Run the DAS without an experiment
        logging.debug("== no experiment ==")
        try:
            data = das.run()
        except Exception as e:
            raise e

    ###
    ### Shutdown
    ###
    t = das.running_time()
    logging.info("Elapsed time: {:6.2f} seconds".format(t))
    logging.info("END {}".format(os.path.abspath(__file__)))
    if shutdown_logging_on_exit:
        logging.shutdown()
    print("*****************************************************")
    print("driver.py: Run completed in {:,.2f} seconds. Logfile: {}".format(t, das.logfilename))


if __name__ == '__main__':
    (main_args, main_config) = main_setup()
    main_das = main_make_das(main_args, main_config)
    main_run_das(main_das)
