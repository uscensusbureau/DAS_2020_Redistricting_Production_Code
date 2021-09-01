# Useful AWS CLI commands
#
# aws s3 ls s3://uscb-decennial-ite-das/
#
# aws s3 rm {path} --recursive --exclude * --include {regex/wildcard/name}
#
# aws s3 cp {from_path} {to_path} --recursive --exclude * --include {regex/wildcard/name} --quiet
#

#####################
# Possibly useful notes:
# https://stackoverflow.com/questions/36994839/i-can-pickle-local-objects-if-i-use-a-derived-class
#####################

import functools
import gc
import glob
import json
import logging
import os
import pickle
import re
import subprocess
import sys
import tempfile
import zipfile
import atexit
import shutil

import scipy.sparse


from typing import Union
from operator import add
from configparser import ConfigParser, NoOptionError, NoSectionError
from collections.abc import Iterable
from fractions import Fraction

import numpy as np
import time
import datetime
# import types

from programs import sparse as sparse
import das_framework.ctools.s3 as s3
import das_framework.ctools.env as env
# import constants as C
from constants import CC

from exceptions import DASValueError

DELIM = CC.REGEX_CONFIG_DELIM


def mergedicts(dict_list):
    """
    Combine all dictionaries in a list into a single dictionary

    Note: Does not protect against overwriting when the same key is used multiple times
    """
    merge = {}
    for d in dict_list:
        merge.update(d)
    return merge


def transpose_pairs(tuple_list):
    """
    For each tuple pair in the list, transpose. i.e. (a,b) => (b,a)
    """
    a,b = [list(x) for x in zip(*tuple_list)]
    return list(zip(b,a))


def uncross_levels_pandas(df):
    """
    takes the "level" column and adds columns for each of the crossed
    dimensions in the levels. requires the "query" column to exist
    """

def addslash(directory):
    if directory[-1] != '/':
        directory = f"{directory}/"
    return directory


def getdir(path):
    """
    gets the directory one level up from the file

    Example:
        path = "/mnt/users/jbid/my_file.csv"
        getdir(path) returns "/mnt/users/jbid/"
        getdir(getdir(path)) returns "/mnt/users/"
    """
    end = path[-1]
    if end == "/":
        path = path.split("/")[0:-2]
    else:
        path = path.split("/")[0:-1]
    path = addslash("/".join(path))
    return path


def getSafeQueryname(queryname):
    """
    Since the asterisk typically has special meaning in file systems, it's usually not used
    in filenames. As such, our standard (schema-based) query naming convention for crosses of
    queries "q1 * q2" needs to be renamed to something else. In this case, we will use a period
    to signify crosses in the filename: "q1.q2"
    """
    return ".".join(re.split(CC.SCHEMA_CROSS_SPLIT_DELIM, queryname))


def getStandardQueryname(safe_queryname):
    """
    Convert from a safe queryname standard "q1.q2" to the schema standard "q1 * q2"
    """
    return CC.SCHEMA_CROSS_JOIN_DELIM.join(safe_queryname.split("."))


@functools.lru_cache()
def getMasterIp():
    """IP Address of EMR Master Node. Note that JOB_FLOW_JSON cannot be read on the worker nodes.
    So this relies on ctools.census_env() to read MASTER_IP from /etc/profile.d/census_das.sh
    """
    env.get_census_env()
    return os.environ[CC.MASTER_IP]

def isodate(sep="-"):
    iso = datetime.date.isoformat(datetime.date.fromtimestamp(time.time()))
    return sep.join(iso.split("-"))


def timestamp(seconds):
    """
    returns an approximate time that has been formatted to read more nicely for long runs

    Inputs:
        seconds: the number of seconds

    Outputs:
        a string representation of the time, as expressed in days, hours, minutes, and seconds

    Notes:
        in order to increase readability, seconds is cast to an int, so the times expressed are only approximate (+/- a second)
    """
    seconds = int(seconds)
    if seconds < 1:
        items = ["less than 1 second"]
    else:
        days, seconds = divmod(seconds, 24*60*60)
        hours, seconds = divmod(seconds, 60*60)
        minutes, seconds = divmod(seconds, 60)
        items = [
            f"{days} days" if days > 1 else f"{days} day" if days > 0 else "",
            f"{hours} hours" if hours > 1 else f"{hours} hour" if hours > 0 else "",
            f"{minutes} minutes" if minutes > 1 else f"{minutes} minute" if minutes > 0 else "",
            f"{seconds} seconds" if seconds > 1 else f"{seconds} second" if seconds > 0 else ""
        ]

    items = [x for x in items if x != ""]

    return ", ".join(items)


def pretty(item, indent=4, join_with='\n'):
    if type(item) == dict:
        return json.dumps(item, indent=indent)
    elif isinstance(item, np.ndarray):
        item = item.tolist()
        return join_with.join(item)
    else:
        item = aslist(item)
        return join_with.join(item)


def aslist(item):
    """
    Wraps a single value in a list, or just returns the list
    """
    if isinstance(item, list):
        value = item
    elif isinstance(item, str):
        value = [item]
    elif isinstance(item, Iterable):
        value = list(item)
    else:
        value = [item]

    return value


def tolist(item):
    """
    Converts the item to a list, or just returns the list
    """
    return item if type(item) == list else list(item)


def quickRepr(item, join='\n'):
    items = ["{}: {}".format(attr, value) for attr, value in item.__dict__.items()]
    if join is None:
        return items
    else:
        return join.join(items)


def getObjectName(obj):
    return obj.__qualname__


def runpathSort(runpaths):
    runpaths_sorted = runpaths.copy()
    runpaths_sorted.sort(key=lambda s: int(s.split("/")[-2].split("_")[1]))
    return runpaths_sorted


def printList(thelist, join_with="\n\n"):
    print(join_with.join([str(x) for x in thelist]))


def flattenList(nested_list):
    nested_list = nested_list.copy()
    flattened_list = []
    while nested_list != []:
        element = nested_list.pop()
        if type(element) == list:
            nested_list += element
        else:
            flattened_list.append(element)

    flattened_list.reverse()
    return flattened_list


#################
# Utility functions
# I/O
#################

def loadConfigFile(path):
    config = ConfigParser()
    # since the config parser object automatically converts item names to lowercase,
    # use this to prevent it from doing so
    config.optionxform = str
    if isS3Path(path):
        config_file = s3.s3open(path=path, mode="r")
        # config.readfp(config_file)  This is deprecated
        config.read_file(config_file)
        config_file.close()
    else:
        with open(path, 'r') as config_file:
            # config.readfp(config_file)  This is deprecated
            config.read_file(config_file)

    return config


def getGeoDict(config):
    assert 'geodict' in config, "This config file doesn't contain a 'geodict' section."
    keys = ['geolevel_names', 'geolevel_leng']
    geodict = {}
    for k in keys:
        geodict[k] = config['geodict'][k]
    return geodict


def makePath(path):
    if not os.path.exists(path):
        os.makedirs(path)

def clearPath(path):
    """
    Remove the parts under a path as well as path.txt in either S3 or HDFS.
    :param path: a path to erase. If S3, erases everything under path/ as well as path.txt.
                 contains safety check to make sure that at least 4 slashes are present.
    """
    logging.info(f"clearPath({path})")
    if path.endswith("/"):
        raise ValueError(f"path '{path}' should not end with a /")
    if isS3Path(path):
        if path.count('/') < 4:
            raise ValueError(f"path '{path}' doesn't have enough /'s in it to allow recursive rm")
        try:
            subprocess.run(['aws', 's3', 'rm', '--recursive', '--quiet', path])
            subprocess.run(['aws', 's3', 'rm', '--recursive', '--quiet', path+".txt"])
        except (subprocess.SubprocessError, ValueError) as e:
            logging.error(f"AWS remove failed: subprocess returned error {e}, {sys.exc_info()}")
    else:
        try:
            subprocess.run(['hadoop', 'fs', '-rm', '-r', path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except (subprocess.SubprocessError, ValueError) as e:
            logging.error(f"HDFS remove failed: subprocess returned error {e}, {sys.exc_info()}")


def loadPickleFile(path):
    contents = None
    if isS3Path(path):
        contents = loadPickleS3(path)
    else:
        with open(path, 'rb') as f:
            contents = pickle.load(f)

    return contents


def loadJSONFile(path):
    contents = None
    if isS3Path(path):
        contents = loadJSONS3(path)
    else:
        with open(path, 'r') as f:
            contents = json.load(f)

    return contents


def loadFromS3(path):
    ext = path.split('.')[1]
    if ext == "json":
        contents = loadJSONS3(path)
    else:
        contents = loadPickleS3(path)

    return contents


def loadJSONS3(path):
    jsonfile = s3.s3open(path=path, mode='r')
    contents = json.load(jsonfile.file_obj)
    return contents


def loadPickleS3(path):
    loadfile = s3.s3open(path=path, mode="rb")
    contents = pickle.load(loadfile.file_obj)
    return contents


def isS3Path(path):
    """
    isS3Path does a simple check to see if the path looks like an s3 path or not

    Notes:
        it checks to see if the path string has the standard s3 prefix "s3://"
        at the beginning
    """
    return path.startswith(CC.S3_PREFIX)


def saveNestedListAsTextFile(path, thelist):
    thelist = flattenList(thelist)
    saveListAsTextFile(path, thelist)


def saveListAsTextFile(path, thelist, mode="w"):
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode=mode)
        for item in thelist:
            savefile.write("{}\n".format(item))
        savefile.close()
    else:
        with open(path, mode) as f:
            for item in thelist:
                f.write("{}\n".format(item))


def saveConfigFile(path, config):
    # Only save the config file in S3.
    # It's in the DFXML file anyway.
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode="w")
        config.write(savefile)
        savefile.close()
    else:
        logging.warning(f"saveConfigFile: not saving config file to {path}")


def saveJSONFile(path, data, indent=None):
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode="w")
        json.dump(data, savefile, indent=indent)
        savefile.close()
    else:
        with open(path, 'w') as f:
            json.dump(data, f, indent=indent)


def expandPathRemoveHdfs(path):
    return os.path.expandvars(path).replace(CC.HDFS_PREFIX,"")


def savePickledRDD(path, rdd, *, dvs_singleton=None):
    """This function is called in all cases to save a RDD as a pickle. This is used for saving noisy measurements.
    :param path: the location to save, either as an hdfs: or s3: URL. The hdfs:// would be removed.
    :param rdd: the RDD being saved.
    """
    path = expandPathRemoveHdfs(path)
    clearPath(path)
    logging.info(f"rdd.saveAsPickleFile({path})")
    rdd.saveAsPickleFile(path)

    logging.info(f"Computing DVS annotations for {path}")
    if dvs_singleton is not None:
        import programs.python_dvs.dvs as dvs
        try:
            dc = dvs.DVS()
            dc.set_message("savePickledRDD")
            dc.add_s3_paths_or_prefixes(dvs_singleton.COMMIT_AFTER, [path + "/"], threads=CC.DVS_THREADS)
            dvs_singleton.add_child(dvs_singleton.COMMIT_AFTER, dc)
        except dvs.exceptions.DVSException as e:
            logging.warning("DVS Exception: %s",str(e))
        except OSError as e:
            logging.warning("DVS OSError %s",str(e))
            logging.warning("Cannot add_s3_paths_or_prefixes for %s",path)
    else:
        logging.info(f"Will not save to DVS")


def savePickleFile(path, data):
    """Saves data from the DRIVER node to a local file or to S3 as a pickle.
    :param path: where to save the data. Can be a regular path or an s3:// path.
    :param data: the data to save as a pickle.
    """
    path = expandPathRemoveHdfs(path)
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode='wb')
        pickle.dump(data, savefile)
        savefile.close()
    else:
        with open(path, 'wb') as f:
            pickle.dump(data, f)


def freeMemRDD(rdd):
    """ Remove RDD from memory, with garbage collection
    :param rdd: the rdd to delete.
    """
    rdd.unpersist()
    del rdd
    gc.collect()


def partitionByParentGeocode(block_nodes, num_partitions=None):
    """ Partition an RDD of geonodes by parent geocode """
    return block_nodes \
        .map(lambda node: (node.parentGeocode, node)) \
        .partitionBy(num_partitions, lambda k: int(k) if len(k)>0 else 0) \
        .map(lambda d: (lambda parent_code, node: node)(*d), preservesPartitioning=True)


def partitionByGeocode(block_nodes, num_partitions=None):
    """ Partition an RDD of geonodes by parent geocode """
    return block_nodes \
        .map(lambda node: (node.geocode, node)) \
        .partitionBy(num_partitions, lambda k: int(k) if len(k)>0 else 0) \
        .map(lambda d: (lambda parent_code, node: node)(*d),  preservesPartitioning=True)


def splitBySize(code_count_list):
    # Sort (code, children_count) list by children count
    a_s = sorted(code_count_list, key=lambda d: (lambda geocode, count: int(count))(*d))

    # Find maximal children count
    a_max = int(a_s[-1][1])

    # Counter to go over a list
    i = 0

    # Partition to assign the node to
    partition = 0

    # {geocode: partition} dictionary
    pdict = {}

    while i < len(a_s):  # Go over list, node by node
        s = 0  # Sum of children in current partition
        while s < 0.7 * a_max:  # Loop over one partition, trying to get in between 0.7 and 1.1 of maximal
            s += int(a_s[i][1])  # Increase number of children already in partition
            pdict[a_s[i][0]] = partition  # Assign node to this partition
            i += 1
        if s > 1.1 * a_max:    # If we overshot, remove the last node from the partition
            i -= 1
            s -= int(a_s[i][1])
        partition += 1     # Proceed to the next partition
    return pdict


def partitionBySiblingNumber(block_nodes, num_partitions):
    """ Partition an RDD of geonodes by number of geounits with the same parent """
    print("Repartitioning by number of siblings...")
    parent_counts = block_nodes.map(lambda node: (node.parentGeocode, 1)).reduceByKey(add).collect()
    pdict = splitBySize(parent_counts)
    return block_nodes \
        .map(lambda node: (node.parentGeocode, node)) \
        .partitionBy(num_partitions, lambda k: pdict[k]) \
        .map(lambda d: (lambda parent_code, node: node)(*d), preservesPartitioning=True)


def rddPartitionDistributionMoments(rdd):
    """Return mean and standard deviation of sizes (number of elements) of partitions of the RDD"""
    sizes = rdd.glom().map(lambda d: len(d)).collect()
    return f"in {len(sizes)} partitions: {np.mean(sizes) - np.std(sizes)}, {np.mean(sizes) +np.std(sizes)}"


def maxPartPerKey(rdd, func):
    """
    From .glom() find out in how many partitions each property of :rdd: element (found by applying :func: to the element)
    shows up, and calculate maximum.
    If the :rdd: is properly partitioned by that property, the maximum should be 1.
    """
    return rdd \
            .map(func) \
            .glom() \
            .map(lambda glist: np.unique(glist)) \
            .zipWithIndex() \
            .flatMap(lambda glpi: [(l, [glpi[1]]) for l in glpi[0]]) \
            .reduceByKey(lambda x, y: x + y).map(lambda gcl: len(gcl[1])).reduce(max)


def ship_files2spark(spark, allfiles=False, subdirs=('programs', 'das_framework', 'etc'), subdirs2root=()):
    """
    Zips the files in das_decennial folder and indicated subfolders to have as submodules and ships to Spark
    as zip python file.
    Also can ship as a regular file (for when code looks for non-py files)
    :param subdirs:  Subdirectories to add to the zipfile
    :param allfiles: whether to ship all files (as opposed to only python (.py) files)
    :param spark: SparkSession where to ship the files
    :return:
    """

    print("das_utils::ship_files2spark(spark=%s, allfiles=%s, subfiles=%s, subdirs2root=%s" %
          (spark, allfiles, subdirs, subdirs2root))

    # for path in list(subdirs) + list(subdirs2root):
    #     assert os.path.isdir(path)

    # Create a temporary directory and register it for deletion at program exit
    tempdir = tempfile.mkdtemp()
    atexit.register(shutil.rmtree, tempdir)

    # das_decennial directory
    ddecdir = os.path.dirname(__file__)
    zipf = zipfile.ZipFile(os.path.join(tempdir, 'das_decennial_submodules.zip'), 'w', zipfile.ZIP_DEFLATED)

    # File with extension to zip
    pat = '*.*' if allfiles else '*.py'

    def addfile(fname, arcname):
        # print(f" {fname} -> {arcname}")
        zipf.write(fname, arcname=arcname)

    # Add the files to zip, keeping the directory hierarchy
    # Don't ship /tests/
    for submodule_dir in subdirs:
        files2ship = [fn for fn in glob.iglob(os.path.join(ddecdir, f'{submodule_dir}/**/{pat}'), recursive=True)]
        files2ship = [fn for fn in files2ship if "/tests/" not in fn]

        for fname in files2ship:
            addfile(fname, arcname=fname.split(ddecdir)[1][1:])

    # Add files in the das_decennial directory
    for fullname in glob.glob(os.path.join(ddecdir, pat)):
        zipf.write(fullname, arcname=os.path.basename(fullname))


    # Add files that are imported as if from root
    for subdir2root in subdirs2root:
        for fullname in glob.glob(os.path.join(ddecdir, subdir2root, pat)):
            addfile(fullname, arcname=os.path.basename(fullname))

    zipf.close()
    spark.sparkContext.addPyFile(zipf.filename)

    if allfiles:
        spark.sparkContext.addFile(zipf.filename)


def class_from_config(config: ConfigParser, key, section):
    """
    Import module and return class from it, based on option in the config file
    :param config: ConfigParser with loaded config file
    :param key: config option within section, containing filename with module and the class name within, dot-separated
    :param section: section of the config file
    :return:
    """
    try:
        (file, class_name) = config.get(section, key).rsplit(".", 1)
    except (NoSectionError, NoOptionError) as e:
        err_msg = f"Key \"{key}\" in config section [{section}] not found when specifying module to load\n{e}"
        logging.error(err_msg)
        raise KeyError(err_msg)

    try:
        module = __import__(file, fromlist=[class_name])
    except ImportError as e:
        err_msg = f"Module {file} import failed.\nCurrent directory: {os.getcwd()}\nFile:{__file__}\nsys.path:{sys.path}\n{e.args[0]}"
        logging.error(err_msg)
        raise ImportError(err_msg)

    try:
        c = getattr(module, class_name)
    except AttributeError as e:
        err_msg = f"Module {module} does not have class \"{class_name}\" which is indicated in config [{section}]/{key} option to be loaded as a module for DAS\n{e.args[0]}"
        logging.error(err_msg)
        raise AttributeError(err_msg)

    return c


def table2hists(d, schema, housing_varname=None, units=False):
    """
    Returns person and housing histograms from a person(or household) table :d: with a unique housingID as last column,
    or just housing histogram from a unit table :d:
    The columns except for the last are according to the :schema:

    Intended for use mainly in the unit tests

    :param d: person(or household) table, a row per person, columns according to the schema + 1 column with a unique housing unit UID, alternatively
        housing unit table, a row per unit, colums according to unit schema + 1 (usually just two columns, unit HH/GQtype + UID)
    :param schema: schema to be used for histogram creation (PL94, SF1 etc.), as programs.schema.schema
    :param housing_varname: name of housing variable in the schema (usually 'rel' or 'hhgq'). Might be temporary until we converge on a single name
    :return: (p_h, h_h), a tuple of das_decennial.programs.sparse.multiSparse arrays, for person and housing histograms respectively
    """
    assert isinstance(d, np.ndarray), "The person/unit data is not passed as numpy array (likely, as a list instead)"

    # This is person/household histogram if d is person/household table and units is False, and unit histogram if d is unit table and units is True
    p_h = np.histogramdd(d[:, :-1], bins=schema.shape, range=tuple([0, ub - 1] for ub in schema.shape))[0]
    if units:
        # Return it as housing units histogram
        return sparse.multiSparse(p_h.astype(int))

    if housing_varname is not None:  # Make units table from units UID and make histogram
        hhgq_ind = schema.dimnames.index(housing_varname)
        h_h = np.histogram(d[np.unique(d[:, -1], return_index=True)[1], hhgq_ind], bins=schema.shape[hhgq_ind], range=[0, schema.shape[hhgq_ind] - 1])[0]
        return sparse.multiSparse(p_h.astype(int)), sparse.multiSparse(h_h.astype(int))

    # Otherwise we're only interested in person/household histogram, and get units histogram with a separate call to this function
    return sparse.multiSparse(p_h.astype(int))


def npArray(a: Union[np.ndarray, sparse.multiSparse], shape=None) -> np.ndarray:
    """
    If argument is a multiSpare, return as numpy array; if numpy array return itself
    :param a:
    :return:
    """
    if isinstance(a, sparse.multiSparse):
        return a.toDense()

    if isinstance(a, np.ndarray):
        return a

    if shape is not None:
        if isinstance(a, scipy.sparse.csr_matrix):
            return a.toarray().reshape(shape)

    raise TypeError("Neither numpy array, nor multiSparse, nor csr_matrix with shape provided")

def int_wlog(s, name):
    """
    Convert a string to integer, wrapped so that it issues error to the logging module, with the variable name
    :param s: string to convert
    :param name: variable name, to indicate in the error message
    :return: integer obtained as conversion of s
    """
    try:
        return int(s)
    except ValueError as err:
        error_msg = f"{name} value '{s}' is not numeric, conversion attempt returned '{err.args[0]}'"
        raise DASValueError(error_msg, s)

def checkDyadic(values, denom_max_power=10, msg=""):
    """
    Checks that all values are dyadic rationals, with power of 2 not more than :denom_max_power: in the
    denominator. E.g. for default of 10, 5/1024 is valid, but 9/2048 is not. All float numbers can be represented
    as dyadic rational with 9007199254740992 (2^53) in denominator (and Fraction will use arbitrarily high denominator),
    so there has to be a limit.
    """
    # return  # TODO: REMOVE THIS WHEN CONFIGS ARE CHANGED TO DYADIC BUDGET PROPORTIONS
    error_msg = f"Non-dyadic-rational factor (or power>{denom_max_power}) present in {msg} budget allocation: {values}, "
    for v in values:
        power2denom = np.log2(Fraction(v).denominator)
        if power2denom > denom_max_power or power2denom != np.floor(power2denom):
            denominators = [int(np.log2(Fraction(v).denominator)) for v in values]
            raise DASValueError(msg=f"{error_msg}, Denominators: {denominators}", value=v)
