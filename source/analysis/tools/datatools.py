import os
import shutil
import das_utils as du
import numpy as np
import pandas
import time
import multiprocessing as mp
import subprocess
import re
import shutil

from pyspark.sql import functions as sf
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.rdd import RDD

import analysis.constants as AC
from constants import CC as con
from constants import CC

import analysis.tools.crosswalk as crosswalk
import analysis.tools.mappers as mappers
import analysis.tools.sqltools as sqltools

from programs.schema.schemas.schemamaker import SchemaMaker

import das_framework.ctools.s3 as s3


EXPERIMENT = 'experiment'
RUN_EXPERIMENT_FLAG = "run_experiment_flag"
S3_ROOT = "s3://uscb-decennial-ite-das/"

# regex string obtained from: https://stackoverflow.com/questions/4289331/how-to-extract-numbers-from-a-string-in-python
SCIENTIFIC_NOTATION_REGEX = "[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?"


"""
Input tools
===========
The following functions and classes are used to read in DAS Experiment data from a variety of forms
and transform it into the format required by the analysis.tools.sdftools functions.

- Read a pickled RDD of DAS Experiment data with at least the 'geocode' and 'syn' attributes retained,
  and ideally containing the 'raw' attribute, as well, and transform it into a sparse histogram format.

  **Note that joining two separate 'raw' and 'syn' datasets in the sparse format will be difficult since
    the sparse format has the condition that a row only exists if raw > 0 or syn > 0. This means that the
    dataset being joined will lose rows that don't match those found in the one it's being joined with.
    Are there other kinds of joins that allow us to perform this operation without losing rows? (outer join, perhaps?)

- Read a path and construct DASRun objects for each set of DAS Experiment data.
  **Note that this is specific to the pickled RDDs in the DAS Experiment file hierarchy structure within S3

"""


class Analysis:
    def __init__(self, spark, save_location, analysis_script_path):
        """
        example of save_location (since s3_base and linux_base are expected to be static)
        save_location = "moran331/analysis_results/logname_and_other_info/"

        which, for save_location_s3, will become
        "s3://uscb-decennial-ite-das/users/moran331/analysis_results/logname_and_other_info/"

        and for save_location_linux
        "/mnt/users/moran331/analysis_results/logname_and_other_info/"
        """
        self.save_location = save_location

        self.save_location_s3 = f"{AC.S3_BASE}{save_location}"
        print("")
        print(f"___ANALYSIS_RESULTS_LOCATION_S3___: {self.save_location_s3}")
        print("")

        self.save_location_linux = f"{AC.LINUX_BASE}{save_location}"
        print("")
        print(f"___ANALYSIS_RESULTS_LOCATION_LINUX___: {self.save_location_linux}")
        print("")
        du.makePath(self.save_location_linux)

        self.spark = spark
        self.analysis_script_path = analysis_script_path

    def make_experiment(self, experiment_name, experiment_path, search_threads=20, build_threads=20, schema_name=None, dasruntype=AC.EXPERIMENT_FRAMEWORK_NESTED, budget_group=None, run_id=None):
        """
        Creates and returns a PickledDASExperiment object

        Parameters
        ==========
        experiment_name : str
            - The name of the experiment
            - This name will become a subdirectory within the analysis results directory

        experiment_path : str or list of str
            - One or more S3 paths for accessing pickled DAS run/experiment data

        search_threads : int
            - The number of threads that the multiprocessing module uses to search for the experiment_path's config files

        build_threads : int
            - The number of threads that the multiprocessing module uses to construct the DASRun objects from the config files

        schema_name : str (default is None)
            - If None, then the schema is inferred/learned from the config file(s)
            - Otherwise, the provided schema name is used

        dasruntype : str (from the analysis.constants file)
            - If AC.EXPERIMENT_FRAMEWORK_NESTED, then this DAS Experiment data was generated using the experimental framework that generates a nested s3 file structure.
            - If AC.EXPERIMENT_FRAMEWORK_FLAT, then this DAS Experiment data was generated using the experimental framework that generates a flat s3 file structure.
        """
        return PickledDASExperiment(self.spark, experiment_name, self.save_location, experiment_path, search_threads, build_threads, schema_name, dasruntype=dasruntype, budget_group=budget_group, run_id=run_id)

    def save_analysis_script(self, to_linux=True, to_s3=True):
        """
        Saves a copy of the analysis script (.py file) currently being run locally (in linux) or in S3 (or both)

        Parameters
        ==========
        to_linux : bool (default is True)
            - Save the analysis script locally?

        to_s3 : bool (default is True)
            - Save the analysis script to S3?

        Notes
        =====
        - Will save the analysis script in the analysis results folder (i.e. the save_location_linux/s3 attributes) for 'this' instance of Analysis.
        """
        if to_linux:
            du.makePath(self.save_location_linux)
            print(self.save_location_linux)
            print("^^^^^ Created save location for analysis results ^^^^^\n")

            # copy the analysis script to the local save location (i.e. in Linux)
            shutil.copy2(self.analysis_script_path, self.save_location_linux)
            print(f"{self.save_location_linux}{self.analysis_script_path.split('/')[-1]}")
            print("^^^^^ Copied analysis script to save location for analysis results ^^^^^\n")

        if to_s3:
            # copy the analysis script to the s3 save location
            script_path = self.analysis_script_path.split("/")[-1]
            s3.put_s3url(f"{self.save_location_s3}{script_path}", self.analysis_script_path)
            print(f"{self.save_location_s3}{script_path}")
            print("^^^^^ Copied analysis script to save location in s3 ^^^^^\n")

    def save_log(self, to_linux=True, to_s3=True):
        """
        Saves a copy of the output log locally (in linux) or in S3 (or both)

        Parameters
        ==========
        to_linux : bool (default is True)
            - Save the log locally?

        to_s3 : bool (default is True)
            - Save the log to S3?

        Notes
        =====
        - Will save the log in the analysis results folder (i.e. the save_location_linux/s3 attributes) for 'this' instance of Analysis.
        """
        if to_linux:
            du.makePath(self.save_location_linux)
            print("___SAVE_LOG_TO_LINUX___")

        if to_s3:
            print("___SAVE_LOG_TO_S3___")

    def zip_results_to_s3(self, flag=True):
        """
        Zips and copies the local analysis results folder to S3

        Parameters
        ==========
        flag : bool (default is True)
            - Zip and save the zip file to S3?

        Notes
        =====
        - Zips the save_location_linux folder and saves it in the save_location_s3 key in S3
        """
        if flag:
            print("___ZIP_RESULTS_TO_S3___")


class PickledDASExperiment:
    """
    Contains information on groups of DAS Experiment run data (i.e. pickled RDDs) that are (at a minimum) related by a DAS Schema
    """
    def __init__(self, spark, experiment_name, analysis_save_location, experiment_path, search_threads=20, build_threads=20, schema_name=None, dasruntype=AC.EXPERIMENT_FRAMEWORK_NESTED, budget_group=None, run_id=None):
        self.spark = spark

        self.name = experiment_name
        self.save_location = f"{du.addslash(analysis_save_location)}{du.addslash(self.name)}"
        self.save_location_s3 = f"{AC.S3_BASE}{self.save_location}"
        self.save_location_linux = f"{AC.LINUX_BASE}{self.save_location}"

        self.experiment_path = experiment_path
        self.dasruntype = dasruntype
        if self.dasruntype == AC.EXPERIMENT_FRAMEWORK_NESTED:
            assert budget_group == None and run_id == None
            self.das_runs = getDASRunsNested(self.experiment_path, search_threads, build_threads, schema_name)
        elif self.dasruntype == AC.EXPERIMENT_FRAMEWORK_FLAT:
            self.das_runs = getDASRunsFlat(self.experiment_path, schema_name, budget_group, run_id)
        else:
            raise ValueError(f"The dasruntype needs to be one of the following options: [{AC.EXPERIMENT_FRAMEWORK_NESTED}, {AC.EXPERIMENT_FRAMEWORK_FLAT}].")

        self.schema = [x.schema for x in self.das_runs]
        schema_names = np.unique([x.name for x in self.schema]).tolist()
        assert len(schema_names) == 1, "Data have different, incompatible schemas. Analysis can only group together multiple runs if those runs all use the same DAS Schema."
        self.schema = self.schema.pop()

        self.df = None

    def getDF(self, mapper_type="sparse", add_metadata_columns=['run_id', 'plb', 'budget_group'], rebuild=False):
        """
        Builds the Spark DataFrame for this Experiment

        Parameters
        ==========
        rebuild : bool (default = False)
            To avoid potentially costly load and transformation operations, the Spark DF will only be built once (or if rebuild is set to True)

        Notes
        =====
        - See DASRun.getDF and getDF in this module for more information
        """
        if (self.df is None) or (self.df is not None and rebuild):
            for r, run in enumerate(self.das_runs):
                if r == 0:
                    self.df = run.getDF(self.spark, mapper_type, add_metadata_columns)
                else:
                    self.df = self.df.unionByName(run.getDF(self.spark, mapper_type, add_metadata_columns))
                self.df = self.df.persist()

        # reorder the columns for easier reading
        # AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
        selection = [AC.GEOCODE, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP] + self.schema.dimnames + [AC.ORIG, AC.PRIV]
        selection = [x for x in selection if x in self.df.columns]
        self.df = self.df.select(selection).persist()
        return self.df


def findDASRunConfigs(path, threads=20):
    """
    uses ctools.s3 module to search s3 for the config files saved within DAS Experiment path

    Parameters
    ==========
    path: str
        An s3 path to DAS Experiment data

    threads: int (kwarg with default = 20)
        Number of `multiprocessing` threads to use during s3 search

    Returns
    =======
    A list of paths to all config files within the provided path
    """
    t0 = time.time()
    config_paths = []
    bucket, prefix = s3.get_bucket_key(path)
    for data in s3.search_objects(bucket, prefix, name='config.ini', searchFoundPrefixes=False, threads=threads):
        config_path = f"{S3_ROOT}{data['Key']}"
        config_paths.append(config_path)
    t1 = time.time()
    print(f"Elapsed time for finding all {len(config_paths)} config.ini paths: {t1-t0} seconds")
    return config_paths


def makeDASRunNested(dasrun_ingredients):
    """
    Used by the multiprocessing module within getDASRuns to more quickly build DASRun objects
    """
    config_path, schema_name = dasrun_ingredients
    return DASRunNested(config_path)


def getDASRunsNested(paths, search_threads=20, build_threads=20, schema_name=None):
    """
    returns a list of DASRun objects that contain information about DAS Experiment data

    Parameters
    ==========
    paths: str or list of str
        List of s3 paths to DAS Experiment data

    search_threads: int (kwarg with default = 20)
        Number of `multiprocessing` threads to use during s3 search

    build_threads: int (kwarg with default = 20)
        Number of `multiprocessing` threads to use to construct DASRun objects

    schema_name: str (kwarg with default = None)
        The name of the Schema associated with all of the DAS Experiments in the path_list
        Notes:
            - If None, the DASRun class will search for the schema_name within the config
              file and will throw an error if it can't be found.
            - If not None, the DASRun class will just use the schema_name provided.

    Returns
    =======
    A list of DASRun objects
    """
    t0 = time.time()
    config_paths = []
    paths = du.aslist(paths)
    for path in paths:
        config_paths += findDASRunConfigs(path, threads=search_threads)
    dasrun_ingredients = [(config_path, schema_name) for config_path in config_paths]
    with mp.Pool(build_threads) as pool:
        runs = pool.map(makeDASRunNested, dasrun_ingredients)
    t1 = time.time()
    print(f"It took {t1-t0} seconds to build all DASRuns from the found config.ini files")
    return runs


def getDASRunsFlat(data_paths, schema_name, budget_group=None, run_id=None):
    data_paths = du.aslist(data_paths)
    dasruns = [DASRunFlat(x, schema_name, budget_group, run_id) for x in data_paths]
    return dasruns


class DASRunFlat():
    def __init__(self, data_path, schema_name, budget_group=None, run_id=None):
        """
        .../data-run8.0-epsilon4.0-BlockNodeDicts/

        """
        self.data_path = du.addslash(data_path)
        self.schema_name = schema_name
        self.schema = SchemaMaker.fromName(self.schema_name)

        # extract data from the data_path
        data_info = self.data_path.split("/")[-2]
        #assert data_info.startswith('data'), "The wrong data path has been provided... Cannot load DASrun"
        #TODO: Replace above assert with something more appropriate ('data' was overly narrow)

        print(f"data_info.split(-): {data_info.split('-')}")
        #_, self.run_id, self.budget_group, _ = data_info.split('-')
        if budget_group == None:
            assert run_id == None
            self.parseDataInfo(data_info)
        else:
            assert run_id != None
            self.budget_group = budget_group
            self.run_id = run_id
        self.run_num = self.run_id[3:].split('.')[0]
        self.plb = self.budget_group
        print(f"Detected plb, run_id: {self.plb}, {self.run_id}")

    def __repr__(self):
        items = ["\nDASRun"]
        items += [f"  {k}: {v}" for k,v in self.__dict__.items() if k != 'schema']
        items += [f"  schema: {self.schema.name}"]
        items += ['\n']
        return "\n".join(items)

    def findFirstOccurrenceInList(self, target_list, target_str):
        for elem in target_list:
            if target_str in elem:
                return elem
        raise ValueError(f"Target string {target_str} not found in targetList {target_list}")

    def parseDataInfo(self, data_info):
        split_info = data_info.split('-')
        self.run_id = self.findFirstOccurrenceInList(split_info, "run")
        self.budget_group = self.findFirstOccurrenceInList(split_info, "epsilon")

    def getDF(self, spark, mapper_type="sparse", add_metadata_columns=[AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP]):
        """
        Loads and transforms a Spark RDD with DAS Experiment Data into a Spark DF

        Parameters
        ==========
        spark: Spark Session object

        mapper_type: str (kwarg with default = "sparse")
            Determines which mapper function to use when transforming the RDD into a DF
            Options:
                "sparse": Creates a Sparse representation of the histogram found in the schema.
                          Only creates rows for cells that are nonzero.
                "dense" : Creates a Dense representation of the histogram found in the schema.
                          Creates rows for both zero and nonzero cells.

        add_metadata_columns: bool (kwarg with default = True)
            Adds columns such as 'plb', 'run_id', and 'budget_group' to the DF using this run's information

        Returns
        =======
        a Spark DataFrame
        """
        if mapper_type == 'sparse':
            mapper = mappers.getSparseDF_mapper
        elif mapper_type == 'dense':
            mapper = mappers.getDenseDF_mapper
        else:
            raise ValueError("The only available options are 'sparse' and 'dense'")
        return getDF(spark, self, mapper, add_metadata_columns)


class DASRunNested():
    """
    Representation of a single run of the DAS.

    Contains the config file and uses the config file to extract other metadata that is
    used to construct a Spark DataFrame from the pickled RDD data.
    """
    def __init__(self, config_path, datapath='data', schema_name=None):
        """
        Parameters
        ==========
        config_path : str
            - This is the S3 path that leads to the config.ini file of one of the runs of the DAS

        datapath : str (default is 'data')
            - The folder location for the partition files associated with the pickled RDD data for this run of the DAS

        schema_name : str (default is None)
            - The name of the schema associated with the data contained in the pickled RDD for this run
            - If None, the DASRun object will search the config file for the '[schema]' section and 'schema' item

        Notes
        =====
        - The DASRun object also can tell if it was part of a DAS Experiment and will generate/grab the appropriate
          metadata for itself (e.g. which run? which budget group?)
        - It also can grab the Privacy Loss Budget from the config file (i.e. plb)
        """
        self.config_path = config_path
        self.path = "/".join(self.config_path.split("/")[0:-1]) + "/"
        self.config = du.loadConfigFile(self.config_path)

        if EXPERIMENT not in self.config:
            # if experiment section doesn't exist at all, it's safe to assume it's not an experiment
            self.is_experiment = False
        elif RUN_EXPERIMENT_FLAG not in self.config[EXPERIMENT]:
                if "run_" in self.path:
                    self.is_experiment = True
                else:
                    self.is_experiment = False
        else:
            run_experiment = self.config[EXPERIMENT][RUN_EXPERIMENT_FLAG]
            self.is_experiment = True if run_experiment in ["1", "True"] else False

        if con.OUTPUT_DATAFILE_NAME in self.config[con.WRITER]:
            datafilename = self.config[con.WRITER][con.OUTPUT_DATAFILE_NAME]
        else:
            datafilename = datapath
        self.data_path = self.path + datafilename + "/"

        if self.is_experiment:
            self.run_id = self.path.split("/")[-2]
            self.run_num = str(findallSciNotationNumbers(self.run_id).pop())
            self.budget_group = self.path.split("/")[-3]
            self.run_type = "experiment"
        else:
            self.run_id = "run0"
            self.run_num = "0"
            self.budget_group = "bg0"
            self.run_type = "single_run"

        if schema_name is None:
            section = 'schema'
            item = 'schema'
            try:
                self.schema_name = self.config.get(section, item)
            except KeyError:
                print(f"'schema' cannot be found in config file. Specify the name of the schema using the 'schema_name' attribute")
        else:
            self.schema_name = schema_name

        self.schema = SchemaMaker.fromName(self.schema_name)

        self.plb = str(self.config[CC.BUDGET][CC.EPSILON_BUDGET_TOTAL])

    def __repr__(self):
        items = ["\nDASRun"]
        items += [f"  {k}: {v}" for k,v in self.__dict__.items() if k != 'schema']
        items += [f"  schema: {self.schema.name}"]
        items += ['\n']
        return "\n".join(items)

    def getDF(self, spark, mapper_type="sparse", add_metadata_columns=[AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP]):
        """
        Loads and transforms a Spark RDD with DAS Experiment Data into a Spark DF

        Parameters
        ==========
        spark: Spark Session object

        mapper_type: str (kwarg with default = "sparse")
            Determines which mapper function to use when transforming the RDD into a DF
            Options:
                "sparse": Creates a Sparse representation of the histogram found in the schema.
                          Only creates rows for cells that are nonzero.
                "dense" : Creates a Dense representation of the histogram found in the schema.
                          Creates rows for both zero and nonzero cells.

        add_metadata_columns: bool (kwarg with default = True)
            Adds columns such as 'plb', 'run_id', and 'budget_group' to the DF using this run's information

        Returns
        =======
        a Spark DataFrame
        """
        if mapper_type == 'sparse':
            mapper = mappers.getSparseDF_mapper
        elif mapper_type == 'dense':
            mapper = mappers.getDenseDF_mapper
        else:
            raise ValueError("The only available options are 'sparse' and 'dense'")
        return getDF(spark, self, mapper, add_metadata_columns)


def getJoinedDF(priv_experiment, orig_experiment):
    """
    Joins the CEF and MDF data (as long as they have the same schema)

    - Need to join orig with priv one data set (run, plb, budget_group) at a time
    - Good news, this basically already happens since we already transform each one individually
      and then union them!
    1. Access and generate each priv df
    2. Get the orig df
    3. Join each priv df with the orig df
    4. Union all dfs
    """
    print("Building the Experiment DF and joining it with the CEF...")
    priv_dasruns = priv_experiment.das_runs

    # generate the orig_df
    orig_df = getOrigDF(orig_experiment).persist()

    # generate each of the DASRun's DFs and join with CEF DF
    for r, run in enumerate(priv_dasruns):
        # get the run's information for replacing null values coming from the full outer join
        label = { k: getattr(run, k) for k in [AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP] }
        label[AC.ORIG] = 0
        # AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
        label[AC.PRIV] = 0

        # define the join / sort columns
        order_cols = [AC.GEOCODE] + priv_experiment.schema.dimnames

        ################
        # Build the DF
        ################
        # get the run's df
        temp = run.getDF(priv_experiment.spark).persist()

        # join with the cef data
        temp = temp.join(orig_df, on=order_cols, how="full_outer").persist()

        # replace all nulls with appropriate values
        temp = temp.fillna(label).persist()

        if r == 0:
            df = temp

        else:
            df = df.unionByName(temp)

        df = df.persist()

    # AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
    column_order = [AC.GEOCODE, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP] + priv_experiment.schema.dimnames + [AC.ORIG, AC.PRIV]
    df = df.select(column_order).persist()

    return df


def getOrigDF(orig_experiment):
    """
    1. Select only the [AC.GEOCODE, schema dimnames, and AC.ORIG] columns from the data
    2. Drop duplicates from the data
    """
    # only need one of the orig dasruns (they should all be
    # identical, minus the (run_id, plb, budget_group) differences)
    orig_dasrun = orig_experiment.das_runs[0]

    order_cols = [AC.GEOCODE] + orig_dasrun.schema.dimnames + [AC.ORIG]

    orig_df = orig_dasrun.getDF(orig_experiment.spark)

    orig_df = orig_df.select(order_cols).persist()

    # this should not need to remove any duplicates since we're only taking one
    # dasrun from the experiment with orig data
    # but it won't harm anything either since there should not be any duplicates
    # for a single dasrun anyway
    # To check, see if the row count pre and post de-duplication are the same (they should be)
    orig_df = orig_df.dropDuplicates()

    return orig_df


def getDF(spark, dasrun, row_mapper, add_metadata_columns=[AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP]):
    """
    Loads and transforms a Spark RDD into a Spark DF using a row_mapper

    Parameters
    ----------
    spark : SparkSession object

    dasrun : DASRun[Nested/Flat] object
        contains information about the individual DAS experimental run

    row_mapper : function
        a function that converts the "geounit node" dict into a list of dicts
        that can be turned into pyspark.sql.Row objects

    add_metadata_columns : list of strings; default is [AC.PLB, AC.RUN_ID, and AC.BUDGET_GROUP]
        adds the metadata columns to the data frame

    Returns
    -------
    Spark DF
    """
    rdd = spark.sparkContext.pickleFile(dasrun.data_path)
    df = rdd2df(rdd, dasrun.schema, row_mapper)
    df = addMetadataColumns(df, dasrun, add_metadata_columns)
    return df


def addMetadataColumns(df, dasrun, metadata_columns=[AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP]):
    """
    adds additional experiment/run metadata columns to the Spark DataFrame

    Parameters
    ==========
    df : Spark DataFrame

    dasrun : DASRun object
        A wrapper for the run path and other important metadata for an experiment run

    metadata_columns : list of strings; default is [AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP]
        Add the provided columns to the data frame if the data exists in the das run

    Returns
    =======
    Spark DataFrame
    """
    for column in du.aslist(metadata_columns):
        lit = getattr(dasrun, column)
        if lit is not None:
            df = df.withColumn(column, sf.lit(lit)).persist()
    return df


def findallSciNotationNumbers(s):
    """
    returns a list of strings that contain all consecutive digits and/or scientific notation values
    """
    return re.findall(SCIENTIFIC_NOTATION_REGEX, s)


def rdd2df(rdd, schema, row_mapper=mappers.getSparseDF_mapper):
    """
    Takes an RDD of GeounitNode objects OR dictionaries with geounit node info and converts it into
    a Spark DF in the form expected by most analysis.tools.sdftools functions.

    Parameters
    ==========
    rdd : Spark RDD
        Needs to contain either Geounit Nodes or dictionaries of geounit node info

    schema : programs.schema.schema.Schema
        The DAS schema object associated with the data in the rdd

    row_mapper : function (default = the sparse mapper function contained in analysis.tools.mappers)
        A Spark map function used to transform the sparse matrices/numpy arrays used by the DAS into
        Spark Row objects

    Returns
    =======
    a Spark DF (by default in the standard Sparse Histogram Analysis format)

    Notes
    =====
    - The run metadata columns (e.g. run_id, budget group, plb) will need to be added manually to the returned data
      frame using the df.withColumn function.
    - This function adds support for Analysis calculations within the DAS
    """

    df = (
        rdd.flatMap(lambda node: row_mapper(node, schema))
           .map(lambda rowdict: Row(**rowdict))
           .toDF()
    )
    df = df.persist()
    return df


class Recode2020CensusMDFToSparseHistogramDF():
    def __init__(self, analysis, schema, mdf_path, cef_path=None, budget_group="Unspecified", plb="Unspecified", run_id="Unspecified", debugmode=True):
        self.debugmode = debugmode
        self.analysis = analysis
        self.spark = self.analysis.spark
        self.mdf_path = mdf_path
        self.schema = schema
        self.schema_recode_dict = sqltools.getRecodesFromList(self.schema.dimnames)

        self.mdf = self.spark.read.csv(self.mdf_path, header=True, sep='|', comment='#')

        self.budget_group = budget_group
        self.plb = plb
        self.run_id = run_id

        self.priv_df = self._transform(self.mdf).persist()

        if cef_path is not None:
            self.cef_path = cef_path

            # assume cef_path's data is a pickled rdd
            cef_experiment = self.analysis.make_experiment("_CEF_DATA_", cef_path, schema_name=self.schema.name, dasruntype=AC.EXPERIMENT_FRAMEWORK_FLAT)
            self.orig_df = getOrigDF(cef_experiment).persist()

            self.df = self._join_mdf_and_cef().persist()
        else:
            self.df = self.priv_df

    def _join_mdf_and_cef(self):
        label = {
            AC.PLB: self.plb,
            AC.RUN_ID: self.run_id,
            AC.BUDGET_GROUP: self.budget_group,
            AC.ORIG: 0,
            # AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
            AC.PRIV: 0
        }
        order_cols = [AC.GEOCODE] + self.schema.dimnames

        df = self.priv_df.join(self.orig_df, on=order_cols, how="full_outer").persist()
        df = df.fillna(label).persist()

        # AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
        column_order = [AC.GEOCODE, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP] + self.schema.dimnames + [AC.ORIG, AC.PRIV]
        df = df.select(column_order).persist()

        return df

    def _transform(self, mdf):
        df = mdf
        df = self._recode_geocode(df).persist()
        df = self._recode_schema(df).persist()
        df = self._add_metadata_columns(df).persist()
        df = self._drop_unneeded_columns(df).persist()
        df = self._get_counts(df).persist()
        return df

    def _get_counts(self, df):
        df = df.groupBy(df.columns).count().persist()
        # AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
        df = df.withColumnRenamed("count", AC.PRIV).persist()
        return df

    def _recode_geocode(self, df):
        df = df.withColumn(AC.GEOCODE, sf.concat(sf.col("TABBLKST"), sf.col("TABBLKCOU"), sf.col("TABTRACTCE"), sf.col("TABBLKGRPCE"), sf.col("TABBLK"))).persist()
        return df

    def _drop_unneeded_columns(self, df):
        keep_columns = [AC.GEOCODE, AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP] + self.schema.dimnames
        df = df.select(keep_columns).persist()
        return df

    def _add_metadata_columns(self, df):
        df = df.withColumn(AC.PLB, sf.lit(self.plb)).persist()
        df = df.withColumn(AC.RUN_ID, sf.lit(self.run_id)).persist()
        df = df.withColumn(AC.BUDGET_GROUP, sf.lit(self.budget_group)).persist()
        return df

    def _recode_schema(self, df):
        for colname, sql in self.schema_recode_dict.items():
            df = df.withColumn(colname, sf.expr(sql)).persist()
        return df

# privatized means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
def getMicrodataDF(spark, dasrun, schema, privatized=True, metadata_columns=[AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP], mangled_names=True, recoders=None):
    """
    Takes a pickled experiment run RDD and transforms it into a Microdata Spark DataFrame

    Parameters
    ==========
    spark : SparkSession

    dasrun : DASRun

    schema : programs.schema.schema.Schema

    privatized : bool (privatized means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production)
        If True, then create records from the protected data (i.e. 'syn' or AC.PRIV; aka DAS output)
        If False, then create records from the original data (i.e. 'raw' or AC.ORIG; aka CEF)

    metadata_columns : list of strings; default is [AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP]
        Add the provided columns to the data frame if the data exists in the das run

    mangled_names : bool
        If True, include the schema's name in each of the schema's dimnames, for more unique column names
        If False, only use the schema's dimnames

    recoders : list of functions
        These functions all accept a dictionary (representing a Row), perform some operation, and then return a dictionary


    Returns
    =======
    Spark DataFrame
        Containing the microdata records present in the data
    """
    rdd = spark.sparkContext.pickleFile(dasrun.data_path)
    df = (
        # privatized means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
        rdd.flatMap(lambda node: mappers.getMicrodataDF_mapper(node, schema, privatized=privatized, mangled_names=mangled_names, recoders=recoders))
           .map(lambda rowdict: Row(**rowdict))
           .toDF()
           .persist()
    )
    df = addMetadataColumns(df, dasrun, metadata_columns)
    return df
