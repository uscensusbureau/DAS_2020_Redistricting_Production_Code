# DAS Spark setup module
# William Sexton
# Last modified: 7/12/2018

"""
    This is the setup module for DAS development on Research 2 and on EMR clusters.
    It launches Spark.
"""
import logging
import tempfile
from typing import Tuple, Dict, List
from hdfs_logger import setup as hdfsLogSetup
from programs.schema.schemas.schemamaker import SchemaMaker, _unit_schema_dict
from programs.invariants.invariantsmaker import InvariantsMaker
from das_utils import ship_files2spark, checkDyadic
from das_framework.driver import AbstractDASSetup
from exceptions import DASConfigError
from constants import CC

SHIP_SUBDIRS = ('programs', 'das_framework', 'analysis')

# Bring in the spark libraries if we are running under spark
# This is done in a try/except so that das_setup.py can be imported for test continious test framework
# even when we are not running under Spark
#
try:
    from pyspark import SparkConf, SparkContext, SQLContext
    from pyspark.sql import SparkSession
except ImportError:
    pass

# Bring in the DAS modules. The debugging is designed to help us understand
# why it's failing sometimes under spark. We can probably clean some of this out.
# For example, why is it looking for the git repository???
# try:
#     from das_framework.driver import AbstractDASSetup
# except ImportError:
#     logging.debug("System path: {}".format(sys.path))
#     logging.debug("Working dir: {}".format(os.getcwd()))
#     path = os.path.realpath(__file__)
#     pathdir = os.path.dirname(path)
#     while True:
#         if os.path.exists(os.path.join(pathdir, ".git")):
#             logging.debug("Found repository root as: {}".format(pathdir))
#             break
#         pathdir = os.path.dirname(pathdir)
#         if os.path.dirname(path) == path:
#             raise ImportError("Could not find root of git repository")
#     path_list = [os.path.join(root, name) for root, dirs, files in os.walk(pathdir)
#                  for name in files if name == "driver.py"]
#     logging.debug("driver programs found: {}".format(path_list))
#     assert len(path_list) > 0, "Driver not found in git repository"
#     assert len(path_list) == 1, "Too many driver programs found"
#     sys.path.append(os.path.dirname(path_list[0]))
#     from das_framework.driver import AbstractDASSetup
# TK - FIXME - Get the spark.eventLog.dir from the config file and set it here, rather than having it set in the bash script
#              Then check to make sure the directory exists.


class DASDecennialSetup(AbstractDASSetup):
    """
        DAS setup class for 2018 development on research 2 and emr clusters.
    """
    levels: Tuple[str, ...]
    schema: str
    hist_shape: Tuple[int, ...]
    hist_vars: Tuple[str, ...]
    postprocess_only: bool
    inv_con_by_level: Dict[str, Dict[str, List[str]]]
    dir4sparkzip: str
    plb_allocation: Dict[str, float]   # Dictionary of Privacy Loss Budget proportion spent of the geounit, by its geocode.
    spine_type: str  # AIAN, NON-AIAN, OPTIMIZED
    geocode_dict: Dict[int, str]  # Lengths of substrings of geocode as key, geocode names as value, THE STANDARD ONE
    qalloc_string = ""  # String for query allocation to be filled by engine module, and accessed by other modules like writer

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if self.getboolean(CC.PRE_RELEASE, default=False):
            import programs.pre_release_datadict as datadict
        else:
            import programs.datadict as datadict

        self.datadict = datadict

        # Whether to run in spark (there is a local/serial mode, for testing and debugging)
        self.use_spark = self.getboolean(CC.SPARK, section=CC.ENGINE, default=True)

        # Geographical level names
        self.levels = self.gettuple(CC.GEODICT_GEOLEVELS, section=CC.GEODICT)

        # Bottom level geolevel we are interested in. Defaults to the first level in self.levels, which is the lowest
        self.geo_bottomlevel = self.getconfig(CC.GEO_BOTTOMLEVEL, section=CC.GEODICT, default=self.levels[0])

        self.only_dyadic_rationals = self.getboolean(CC.ONLY_DYADIC_RATIONALS, section=CC.BUDGET, default=False)
        # self.geolevel_prop_budgets = self.gettuple_of_fraction2floats(CC.GEOLEVEL_BUDGET_PROP, section=CC.BUDGET, sep=CC.REGEX_CONFIG_DELIM)
        self.geolevel_prop_budgets = self.gettuple_of_fractions(CC.GEOLEVEL_BUDGET_PROP, section=CC.BUDGET, sep=CC.REGEX_CONFIG_DELIM)
        if self.only_dyadic_rationals:
            checkDyadic(self.geolevel_prop_budgets, msg="across-geolevel")

        # Create geocode dict
        geolevel_leng = self.gettuple(CC.GEODICT_LENGTHS, section=CC.GEODICT)
        assert len(geolevel_leng) == len(self.levels), "Geolevel names and geolevel lengths differ in size"
        self.geocode_dict = {int(gl_length): gl_name for gl_name, gl_length in zip(self.levels, geolevel_leng)}

        self.spine_type = self.getconfig(CC.SPINE, section=CC.GEODICT, default="non_aian_spine")
        if self.spine_type not in CC.SPINE_TYPE_ALLOWED:
            raise DASConfigError(msg=f"spine type must be {'/'.join(CC.SPINE_TYPE_ALLOWED)} rather than {self.spine_type}.", option=CC.SPINE, section=CC.BUDGET)

        self.plb_allocation = None  # To be filled in the reader module if "opt_spine"

        self.privacy_framework = self.getconfig(key=CC.PRIVACY_FRAMEWORK, section=CC.BUDGET, default=CC.PURE_DP)
        self.dp_mechanism_name = self.getconfig(key=CC.DP_MECHANISM, section=CC.BUDGET, default=CC.GEOMETRIC_MECHANISM)
        mechanism_not_implemented_msg = f"{self.dp_mechanism_name} not implemented for {self.privacy_framework}."
        if self.privacy_framework in (CC.ZCDP,):
            assert self.dp_mechanism_name in (CC.DISCRETE_GAUSSIAN_MECHANISM, CC.ROUNDED_CONTINUOUS_GAUSSIAN_MECHANISM,
                     CC.FLOAT_DISCRETE_GAUSSIAN_MECHANISM), mechanism_not_implemented_msg
        elif self.privacy_framework in (CC.PURE_DP,):
            assert self.dp_mechanism_name in (CC.GEOMETRIC_MECHANISM,), mechanism_not_implemented_msg
        else:
            raise NotImplementedError(f"DP primitives/composition rules for {self.privacy_framework} not implemented.")
        self.log_and_print(f"Privacy mechanism: {self.dp_mechanism_name}")

        self.log_and_print(f"geolevels: {self.levels}")
        # schema keyword
        self.schema = self.getconfig(CC.SCHEMA, section=CC.SCHEMA)
        self.log_and_print(f"schema keyword: {self.schema}")
        self.schema_obj = SchemaMaker.fromName(self.schema)
        self.unit_schema_obj = SchemaMaker.fromName(_unit_schema_dict[self.schema])
        self.postprocess_only = self.getboolean(CC.POSTPROCESS_ONLY, section=CC.ENGINE, default=False)
        self.validate_input_data_constraints = self.getboolean(CC.VALIDATE_INPUT_DATA_CONSTRAINTS, section=CC.READER, default=True)

        self.inv_con_by_level = {}
        for level in self.levels:
            self.inv_con_by_level[level] = {
                "invar_names": self.gettuple(f"{CC.THEINVARIANTS}.{level}", section=CC.CONSTRAINTS, default=()),
                "cons_names": self.gettuple(f"{CC.THECONSTRAINTS}.{level}", section=CC.CONSTRAINTS, default=())
            }

        try:
            # Person table histogram shape (set here and then checked/set in the reader module init)
            self.hist_shape = self.schema_obj.shape
            self.unit_hist_shape = self.unit_schema_obj.shape
            # Person table histogram variables (set here and then checked/set in the reader module init)
            self.hist_vars = self.schema_obj.dimnames
        except AssertionError:
            self.log_warning_and_print(f"Schema {self.schema} is not supported")

        # Temporary directory with code and files shipped to spark, to delete later
        self.dir4sparkzip = None

        noisy_partitions_by_level = self.gettuple_of_ints(CC.NOISY_PARTITIONS_BY_LEVEL, section=CC.WRITER_SECTION, default=",".join(("0",) * len(self.levels)))
        self.annotate(f'noisy_partitions_by_level: {noisy_partitions_by_level}')
        assert len(noisy_partitions_by_level) == len(self.levels), f'Config Error: noisy_partitions_by_level should be the same length as the geolevels. Found instead: self.levels: {self.levels}, noisy_partitions_by_level: {noisy_partitions_by_level }'

        self.noisy_partitions_dict = {self.levels[index]: noisy_partitions_by_level[index] for index in range(len(self.levels))}
        self.annotate(f'noisy_partitions_dict: {self.noisy_partitions_dict}')

        self.dvs_enabled = self.getboolean(CC.DVS_ENABLED, section=CC.DVS_SECTION, default=False)

    def makeInvariants(self, *args, **kwargs):
        return InvariantsMaker.make(*args, schema=self.schema, **kwargs)

    def makeConstraints(self, *args, **kwargs):
        return self.datadict.getConstraintsModule(self.schema).ConstraintsCreator(*args, **kwargs).calculateConstraints().constraints_dict

    def setup_func(self):
        """
            Starts spark up in local mode or client mode for development and testing on Research 2.
        """
        # If we are making the BOM, just return
        if self.das.make_bom_only():
            return self
        # Validate the config file. DAS Developers: Add your own!
        pyspark_log = logging.getLogger('py4j')
        pyspark_log.setLevel(logging.ERROR)
        hdfsLogSetup(self.config)
        conf = SparkConf().setAppName(self.getconfig(f"{CC.SPARK}.{CC.NAME}"))

        # !!! SETTING THE SPARK CONF THIS WAY DOESN'T HAVE EFFECT, PUT IT IN THE RUN SCRIPT AS ARGUMENTS TO spark-submit
        # This should not have much effect in Python, since after the the data is pickled, its passed as bytes array to Java
        # Kryo is a Java serializer. But might be worth to just turn it on to see if there's a noticeable difference.
        # conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Currently we don't (or almost don't) use Spark SQL, so this is probably irrelevant
        # conf.set("spark.sql.execution.arrow.enabled", "true")
        # conf.set("spark.task.maxFailures", "30")
        # conf.set("spark.executor.pyspark.memory", "500K")
        # conf.set("spark.memory.fraction", "0.2")  # tuning GC

        sc = SparkContext.getOrCreate(conf=conf)

        # !!! THIS DOESN'T WORK EITHER, PUT IT IN THE RUN SCRIPT AS ARGUMENTS TO spark-submit
        # SparkSession(sc).conf.set("spark.default.parallelism", "300")

        sc.setLogLevel(self.getconfig(f"{CC.SPARK}.{CC.LOGLEVEL}"))
        self.dir4sparkzip = tempfile.mkdtemp()
        ship_files2spark(SparkSession(sc), allfiles=self.dir4sparkzip, subdirs=SHIP_SUBDIRS)

        # if sc.getConf().get("{}.{}".format(CC.SPARK, CC.MASTER)) == "yarn":
        #     # see stackoverflow.com/questions/36461054
        #     # --py-files sends zip file to workers but does not add it to the pythonpath.
        #     try:
        #         sc.addPyFile(os.environ[CC.ZIPFILE])
        #     except KeyError:
        #         logging.info("Run script did not set environment variable 'zipfile'.")
        #         exit(1)
        # return SparkSession(sc)
        return self
