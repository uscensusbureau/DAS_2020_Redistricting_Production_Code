"""This is the root class for managing the gurobi optimizer."""
# python imports

import logging
import os
import time
import uuid
import sys
import tempfile
import fnmatch
import subprocess
import socket
import inspect
import resource
import boto3
import random
import os.path

from configparser import NoOptionError,NoSectionError
from abc import ABCMeta, abstractmethod
from typing import Tuple, Iterable, Union, List
from shutil import copyfile
from collections import defaultdict
from urllib.parse import urlparse

## NOTE: Do not import gurobipy at top level, because the path needs to be set first.
import numpy as np
import scipy.sparse as ss

# das-created imports
import programs.queries.querybase as querybase
from programs.nodes.nodes import GeounitNode
from programs.queries.constraints_dpqueries import StackedConstraint
import programs.optimization.maps as maps
import programs.dashboard as dashboard
import programs.optimization.gurobi_stats as gurobi_stats

from das_framework.ctools import clogging as clogging
from das_framework.driver import AbstractDASModule
import das_framework.driver as driver
import das_framework.ctools.aws as aws
# import das_framework.dfxml
# import das_framework.dfxml as dfxml

from exceptions import RandomGurobiLicenseError

import das_utils
from constants import CC

# Set this to true for the gurobi stats to include the call stack
RECORD_CALL_STACK = False
INSTANCE_ID = aws.instanceId()  # this is expensive, so just do it once

# If we hit gb.GRB.TIME_LIMIT, write the model out to this file:
HANG_FILE_TEMPLATE = "/mnt/tmp/grb_hang_{geocode}_{timestr}.lp"

def ASSERT_TYPE(var, aType):
    if not isinstance(var, aType):
        raise RuntimeError("var is type {} but should be type {}".format(type(var), aType))

def getIP():
    """
    Get the IP address of the current node.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip

class AbstractOptimizer(AbstractDASModule, metaclass=ABCMeta):
    """Specifies common functions for SequentialOptimizer and Optimizer classes
    Inputs:
        config: a configuration object
    Instance Variables:
        Instance variables that begin 't' are for the statistics system, mostly times in the optimizer cycle. Comments below
        These statistics are defined here because they can be called from the SequentialOptimizer or from an Optimizer.
    """

    grb_env: object         # Gurobi environment (note: type is gb.Env, but gb cannot be loaded)
    gstats: dict            # where we store the gurobi statistics
    """
    These are no longer instance variables, but are in the gstats directory:
    t_uuid: str             # unique identifier for optimizer solve
    t_env_start: float      # the time Gurobi environment creation begins
    t_env_end: float        # the time Gurobi environment creation ends
    t_modbuild_start: float # when Gurobi begins building the model
    t_modbuild_end: float   # when Gurobi finishes building the model
    t_presolve_start: float # the time when presolve starts
    t_presolve_end: float   # the time when presolve ends
    t_optimize_start: float # the time when Optimizer starts
    t_optimize_end: float   # the time when Optimizer ends
    """

    # pylint: disable=bad-whitespace
    def __init__(self, **kwargs):
        super().__init__(name=CC.GUROBI_SECTION, **kwargs)
        driver.config_apply_environment(self.config)  # make sure that environment section is in the environment
        self.grb_env             = None   # no grb_env created yet...
        self.gstats = {'uuid' : uuid.uuid4().hex}
        self.save_lp_path        = self.getconfig(CC.SAVE_LP_PATH, default=CC.SAVE_LP_PATH_DEFAULT, expandvars=False)
        self.save_lp_pattern     = self.getconfig(CC.SAVE_LP_PATTERN, default='', expandvars=False)
        self.save_lp_seconds     = self.getfloat(CC.SAVE_LP_SECONDS, section=CC.GUROBI_SECTION, default=CC.SAVE_LP_SECONDS_DEFAULT)
        self.gurobi_path         = self.getconfig(CC.GUROBI_PATH, section=CC.GUROBI_SECTION, default=None, expandvars=True)

        ## gurobi_lic_fail_rate allows us to simulate contention on the license server to test the retry logic.
        ## In a production enviornment this should be zero.
        self.gurobi_lic_fail_rate= self.getfloat(CC.GUROBI_LIC_FAIL_RATE, section=CC.GUROBI, default=CC.GUROBI_LIC_FAIL_DEFAULT)
        os.environ[CC.PYTHON_VERSION] = f'python{sys.version_info.major}.{sys.version_info.minor}'

        if "LD_LIBRARY_PATH" not in os.environ:
            raise RuntimeError("LD_LIBRARY_PATH not set in config file")

        if self.gurobi_path and self.gurobi_path not in sys.path:
            sys.path.insert(0, self.gurobi_path)
            try:
                import gurobipy as gb
            except ImportError:
                nl = '\n'
                raise RuntimeError(f"Cannot import gurobipy; host:{socket.gethostbyname()} path:{nl.join(sys.path)}")

    @abstractmethod
    def run(self):
        """
        To be filled in by subclass.
        This method will specify the sequence of optimizers (in sequential optimizers) and
        build the model, perform optimization(s) and return the result
        """
        pass

    def addGurobiModel(self, model):
        """add flattened model attributes"""
        if self.getboolean(CC.RECORD_GUROBI_STATS_OPTION, default=False):
            self.gstats =  {**self.gstats, **gurobi_stats.model_info(model)}

    def addGurobiStatistics(self,  **kwargs):
        """add arbitrary name=value statistics"""

        if self.getboolean(CC.RECORD_GUROBI_STATS_OPTION, default=False):
            self.gstats = {**self.gstats, **kwargs}

    def sendGurobiStatistics(self):
        """
        Add stat to the list of gurobi_stats.
        @param optimizer - the optimizer we were called from. Optimizer objects run on the mapper.
        @param point     - the point in the code where we were called
        """

        # got to here
        if self.getboolean(CC.RECORD_GUROBI_STATS_OPTION, default=False):

            self.gstats[CC.OPTIMIZER]    = type(self).__name__
            self.gstats['instanceId']    = INSTANCE_ID
            self.gstats['applicationId'] = clogging.applicationId()

            if RECORD_CALL_STACK:
                # Grab the last 4 frames of the call stack, because it might be interesting
                self.gstats['stack'] = CC.STATISTICS_PATHNAME_DELIMITER.join(
                    [f"{frame.filename}:{frame.lineno}({frame.function})" for frame in inspect.stack()[1:4]])

            # Record the CPU and VM statistics. This used to be optional but it is not high overhead
            # compared to the rest and removing the variables simplifies configuration files

            rusage_self = resource.getrusage(resource.RUSAGE_SELF)
            rusage_children = resource.getrusage(resource.RUSAGE_CHILDREN)
            self.gstats['pid'] = os.getpid()
            self.gstats['ppid'] = os.getppid()
            self.gstats['loadavg'] = os.getloadavg()[0]
            self.gstats['utime'] = rusage_self.ru_utime
            self.gstats['stime'] = rusage_self.ru_stime
            self.gstats['maxrss_bytes'] = rusage_self.ru_maxrss * 1024
            self.gstats['utime_children'] = rusage_children.ru_utime
            self.gstats['stime_children'] = rusage_children.ru_stime
            self.gstats['maxrss_children'] = rusage_children.ru_maxrss

            if hasattr(self, 'childGeoLen'):
                self.gstats['childGeoLen'] = getattr(self, 'childGeoLen')

            # Send the gurobi statistics using AWS SQS
            dashboard.send_obj(sender='gstats', obj=self.gstats)

    def getGurobiEnvironment(self, retries=CC.GUROBI_LICENSE_MAX_RETRIES):
        """ Create a new license environment
            IMPORTANT: HAS TO BE NEW ENVIRONMENT, DO NOT TRY TO RETURN ONE ALREADY IN PYTHON OBJECT
        Input:
            config: config file.

        Output:
            environment object

        Notes:
            1. if config["ENVIRONMENT"] is "GAM" or if ISV_NAME is not sent, create an environment using the public
               gb.Env() API, which typically uses the academic license.
            2. If a license cannot be obtained, implements retries with random backoff.

        """

        # This appears to be the first function called in the python environment on each worker node.
        # Be sure the enviornment is propertly set up.

        if self.gurobi_path and self.gurobi_path not in sys.path:
            sys.path.insert(0, self.gurobi_path)
            import gurobipy as gb

        # logging() is used to send notice of failed gurobi license acquisitions via syslog to the DRIVER<
        # where they are reported by syslog to splunk.
        #


        clogging.setup(level=logging.INFO,
                       syslog=True,
                       syslog_address=(das_utils.getMasterIp(), CC.SYSLOG_UDP),
                       syslog_format=clogging.YEAR + " " + clogging.SYSLOG_FORMAT)

        # Always create a new gurobi environment!
        # (Previously we just returned grb_env if we already had one. This was an error)

        # First, create a gurobi license file on this node for this run of the optimizer.
        # if one does not exist.
        #
        try:
            os.environ[CC.DAS_RUN_UUID] = self.getconfig(CC.DAS_RUN_UUID, section=CC.ENVIRONMENT_SECTION)
        except NoSectionError:
            os.environ[CC.DAS_RUN_UUID] = "NO_DAS_RUN_UUID"

        if self.getboolean(CC.GUROBI_LIC_CREATE, section=CC.GUROBI_SECTION, default=False):
            if not os.path.exists(CC.GUROBI_LIC_CREATE_FNAME):
                with tempfile.NamedTemporaryFile(suffix='.lic', mode='w') as tf:
                    tf.write("TOKENSERVER={}\n".format(self.getconfig(CC.TOKENSERVER, section=CC.GUROBI_SECTION)))
                    tf.write("PORT={}\n".format(self.getconfig(CC.PORT, section=CC.GUROBI_SECTION)))
                    tf.flush()
                    #os.rename(tf.name, CC.GUROBI_LIC_CREATE_FNAME)
                    copyfile(tf.name, CC.GUROBI_LIC_CREATE_FNAME)
            os.environ[CC.GRB_LICENSE_FILE] = CC.GUROBI_LIC_CREATE_FNAME
        else:
            os.environ[CC.GRB_LICENSE_FILE] = self.getconfig(CC.GUROBI_LIC)

        import gurobipy as gb

        # Get environment variables
        cluster  = self.getconfig(CC.CLUSTER_OPTION, section=CC.ENVIRONMENT, default=CC.CENSUS_CLUSTER)
        logfile  = self.getconfig(CC.GUROBI_LOGFILE_NAME)
        isv_name = self.getconfig(CC.GRB_ISV_NAME, section=CC.ENVIRONMENT, default='')
        app_name = self.getconfig(CC.GRB_APP_NAME, section=CC.ENVIRONMENT, default='')

        # env = None
        attempt = 0
        rand_wait = 0
        while True:
            try:
                # Implement RFC 748 for Gurobi licenses: https://tools.ietf.org/html/rfc748
                if self.gurobi_lic_fail_rate != CC.GUROBI_LIC_FAIL_DEFAULT:
                    if np.random.uniform(0,1) < self.gurobi_lic_fail_rate:
                        raise RandomGurobiLicenseError("Randomly failed to get a Gurobi license.")
                if (cluster == CC.EDU_CLUSTER) or (isv_name == ''):
                    # Use academic license
                    env = gb.Env(logfile)
                    if self.getconfig(CC.NOTIFY_DASHBOARD_GUROBI_SUCCESS, section=CC.MONITORING_SECTION, default=False):
                        dashboard.token_retry(retry=attempt, delay=rand_wait, success=1)
                else:
                    # Use commercial license
                    env3 = self.getint(CC.GRB_ENV3, section=CC.ENVIRONMENT)
                    env4 = self.getconfig(CC.GRB_ENV4, section=CC.ENVIRONMENT).strip()
                    env = gb.Env.OtherEnv(logfile, isv_name, app_name, env3, env4)
                    logging.info("Acquired gurobi license on attempt %s", attempt)
                # We got the environment, so break and return it
                return env
            except (gb.GurobiError,RandomGurobiLicenseError) as err:
                # If the environment is not obtained, wait some random time and try again if attempt number is still within range

                # This means that the maximum retry time would be (2^17 + (random number
                # between 0 and 1)) * 0.01 which would be about 1310 seconds (21.8 minutes)
                # and the summation of all the times would be on the order of (2^18-1 +
                # 0.5*17)*0.01 which is 2621.515 seconds or about 43 minutes.

                attempt += 1

                rand_wait = (CC.GUROBI_LICENSE_RETRY_EXPONENTIAL_BASE ** (attempt - 1)
                             + np.random.uniform(0, CC.GUROBI_LICENSE_RETRY_JITTER)) * CC.GUROBI_LICENSE_RETRY_QUANTUM
                logging.info("Failed to acquire gurobi license on attempt %s; waiting %s", attempt, rand_wait)
                logging.info("(Gurobi error %s)", str(err))
                if self.getconfig(CC.NOTIFY_DASHBOARD_GUROBI_RETRY, section=CC.MONITORING_SECTION, default=False):
                    dashboard.token_retry(retry=attempt, delay=rand_wait, success=0)
                if attempt > retries:
                    raise RuntimeError("Could not acquire Gurobi license: " + str(err))
                time.sleep(rand_wait)

        # Attempt number loop is over, ran out of attempts, raise the latest Gurobi error




class Optimizer(AbstractOptimizer, metaclass=ABCMeta):
    """
    This is a generic class for gurobi optimization. It includes methods common to generic optimization.
    Superclass Inputs:
        config: a configuration object
    Inputs:
        grb_env: a gurobi environment object. This is a required parameter.
    """
    def __init__(self, *, grb_env, **kwargs):
        super().__init__(**kwargs)
        self.grb_env = grb_env

    # pylint: disable=bad-whitespace
    def newModel(self, model_name):
        """ Creates a new gurobi model utilizing arguments in the self.config object
        Inputs:
            model_name: a string giving the model name
        Output:
            a model object
        """
        import gurobipy as gb
        model = gb.Model(model_name, env=self.grb_env)
        model.Params.LogFile            = self.getconfig(CC.GUROBI_LOGFILE_NAME, section=CC.GUROBI, default=model.Params.LogFile)
        model.Params.OutputFlag         = self.getint(CC.OUTPUT_FLAG,            section=CC.GUROBI, default=model.Params.OutputFlag)
        model.Params.OptimalityTol      = self.getfloat(CC.OPTIMALITY_TOL,       section=CC.GUROBI, default=model.Params.OptimalityTol)
        model.Params.BarConvTol         = self.getfloat(CC.BAR_CONV_TOL,         section=CC.GUROBI, default=model.Params.BarConvTol)
        model.Params.BarQCPConvTol      = self.getfloat(CC.BAR_QCP_CONV_TOL,     section=CC.GUROBI, default=model.Params.BarQCPConvTol)
        model.Params.BarIterLimit       = self.getint(CC.BAR_ITER_LIMIT,         section=CC.GUROBI, default=model.Params.BarIterLimit)
        model.Params.FeasibilityTol     = self.getfloat(CC.FEASIBILITY_TOL,      section=CC.GUROBI, default=model.Params.FeasibilityTol)
        model.Params.Threads            = self.getint(CC.THREADS,                section=CC.GUROBI, default=model.Params.Threads)
        model.Params.Presolve           = self.getint(CC.PRESOLVE,               section=CC.GUROBI, default=model.Params.Presolve)
        model.Params.NumericFocus       = self.getint(CC.NUMERIC_FOCUS,          section=CC.GUROBI, default=model.Params.NumericFocus)

        try:
            model.Params.timeLimit      = self.getfloat(CC.TIME_LIMIT,           section=CC.GUROBI, default=model.Params.timeLimit)
        except NoOptionError:
            pass

        return model

    @staticmethod
    def disposeModel(model):
        """
        Gathers the result and model status and deletes the model objects
        Inputs:
            model: gurobi model object
        Outputs:
            mstatus: The Gurobi model status code
        """
        import gurobipy as gb
        mstatus = model.Status
        del model
        gb.disposeDefaultEnv()
        return mstatus

class GeoOptimizer(Optimizer, metaclass=ABCMeta):
    """
    An Optimizer subclass that operates on a parent -> child

    Superclass Inputs:
        config: a configuration object
        grb_env: a gurobi environment object
    Inputs:
        identifier: a string giving identifying information about the optimization (e.g. the parent geocode)
        parent: a tuple of numpy multi-arrays of the parent histogram (or None if no parent) (one per each histogram)
        parent_shape: a tuple with the shapes of the parent histograms or would be shape if parent histogram is None
        constraints: a list of StackedConstraint objects (see constraints_dpqueries.py)
        childGeoLen: int giving the number of child geographies
        Outputs:
    """

    model_name: str                 # Name of the model in Gurobi, just a label
    rounder: bool                   # Whether optimizer is a Rounder
    parent_constraints_name: str    # Name of parent_constraints in Gurobi, just a label
    child_obj_term_wt: Iterable[float]     # Weight of the objective f-n terms, each cell of each child histogram (coming from detailed query), only needed for L2Opt
    backup_obj_fxn_constr_name: str        # Name of backup_feasibility obj fun in Gurobi, just a label
    use_parent_constraints: bool    # Whether to use parent constrains. They are used everywhere except top level and backup feasibility
    backup_feas: bool               # Whether the optimizer is running in backup feasibility mode
    min_schema: Tuple[str]          # Feasibility schema for backup_feas regime

    # pylint: disable=bad-whitespace
    def __init__(self, *, identifier, child_geolevel, parent, parent_shape, constraints, childGeoLen, backup_feas=False,
                 min_schema=None, child_groups=None, **kwargs):
        super().__init__(**kwargs)
        self.identifier   = identifier
        self.child_geolevel     = child_geolevel
        self.parent       = parent
        self.parent_shape = parent_shape
        self.childGeoLen  = childGeoLen
        self.constraints  = constraints
        self.child_groups = child_groups

        self.backup_feas: bool = backup_feas

        self.answer = None
        self.mstatus = None
        self.use_parent_constraints = not self.backup_feas and self.parent[0] is not None  # If not backup_feas and not top level

        self.acceptable_statuses = [maps.toGRBfromStrStatus('OPTIMAL')]

        self.min_schema: tuple = min_schema

        # The sizes of histograms
        self.hist_sizes = tuple(np.prod(sh) for sh in self.parent_shape)

        # Where they start and end in the joint array (the histograms are going to be flattened to rows and stacked together)
        self.hends = tuple(np.cumsum(self.hist_sizes))
        self.hstarts = (0,) + self.hends[:-1]
        self.random_report = self.getfloat(CC.RANDOM_REPORT_FREQUENCY, section=CC.GUROBI, default=0) > random.random()

        if CC.DAS_RUN_UUID not in os.environ:
            os.environ[CC.DAS_RUN_UUID] = self.getconfig(CC.DAS_RUN_UUID, section=CC.ENVIRONMENT_SECTION)


    def run(self):
        """
        The main function.  Builds the models, solves and returns the answer.
        """

        print(f"{self.model_name} model for parent {self.identifier}")
        import gurobipy as gb

        self.addGurobiStatistics(geocode  = self.identifier,
                                 child_geolevel = self.child_geolevel)

        # Instantiate model, set GRB params
        t_modbuild_start = time.time()
        model = self.newModel(f"{self.model_name}_data")

        # #### Prepare data, subset to non-zeros ####
        # child_floor is only used by Rounder
        # child_sub is the array for per-each-cell terms in the objective function
        #     (coming from the detailed query in L2Opt and from L2opt result in Rounder)
        # n_list is a list of how many gurobi variables (=possible non-zero cells in the histogram) there are in each histogram
        # parent_mask is the mask of these possible non-zero cells
        # parent_sub is parent subset to parent mask
        child_floor, child_sub, n_list, parent_list, parent_mask, parent_sub = self.getNZSubsets()
        print(f"parent_mask in {self.identifier} has shape {parent_mask.shape} sum {np.sum(parent_mask)} and is: {parent_mask}")

        # Total number of gurobi variables per child
        n = sum(n_list)

        # Build variables
        two_d_vars = self.buildMainVars(model, n)

        # Build objective function(s) and add DP query obj fxn terms
        obj_fxn = self.buildObjFxnAddQueries(model, two_d_vars, n_list, child_sub, parent_mask)

        # Build parent constraints
        if self.use_parent_constraints:
            self.buildAndAddParentConstraints(model, parent_sub, two_d_vars, name=self.parent_constraints_name)

        # Build other constraints
        if self.constraints is not None:
            self.addStackedConstraints(model, parent_mask, two_d_vars, child_floor=child_floor)

        # Add constraints on joined children (e.g. so that AIAN and non-AIAN areas totals sum to the invariant state total)
        if self.child_groups is not None:
            self.addGroupedChildTotalConstraint(model, self.hist_sizes[0], two_d_vars,
                                                child_groups=self.child_groups, main_n=n_list[0], rounder=self.rounder, child_floor=child_floor)

        # If in backup, build backup feasibility 2ndary optimization function, & optimize to build degree-of-infeasibility constraint
        if self.backup_feas:
            backup_obj_fxn = self.buildBackupObjFxn(model, two_d_vars, n_list, parent_list, parent_sub, parent_mask)
            backup_obj_val = self.backupFeasOptimize(model, backup_obj_fxn)
            self.backupFeasAddConstraint(model, backup_obj_val, backup_obj_fxn)

        # Log model build start/end times
        # TODO: more model-building happens in below loop, but incrementally, with solves in between. Does
        #       gurobi stats support several model build times?
        self.addGurobiStatistics(t_modbuild = time.time() - t_modbuild_start)
        self.optimizationPassesLoop(model, obj_fxn, two_d_vars, n_list, child_sub, parent_mask)

        # Extract solution
        self.answer = self.reformSolution(two_d_vars, parent_mask, child_floor) if model.Status in [gb.GRB.OPTIMAL, gb.GRB.SUBOPTIMAL] else None
        print(f"In {self.identifier}, self.answer: {self.answer}")

        # Cleanup
        self.mstatus = self.disposeModel(model)

    def optimizationPassesLoop(self, model, obj_fxn, two_d_vars,  n_list, child_sub, parent_mask):
        """ In generic optimizer just a single pass of optimization"""
        # Primary obj fxn optimization
        self.setObjAndSolve(model, obj_fxn)

    def buildObjFxnAddQueries(self, model, two_d_vars, n_list, child_sub, parent_mask):
        # Build objective function
        obj_fxn = self.buildObjFxn(two_d_vars, n_list, child_sub, self.child_obj_term_wt)
        # Add DP queries to the objective function if any (along with adding constraints for aux vars to the model)
        obj_fxn = self.addDPQueriesToModel(model, two_d_vars, obj_fxn, parent_mask)
        return obj_fxn

    @abstractmethod
    def getNZSubsets(self) -> Tuple[None, np.ndarray, List[int], Tuple[Union[np.ndarray, None], ...], np.ndarray, Union[None, np.ndarray]]:
        """ Find non-zeros in the parent arrays and return corresponding subsets of parent, child array and the non-zero mask"""
        pass

    @abstractmethod
    def buildMainVars(self, model, n_list):
        """ Add variables (corresponding to cells of histograms of the children) to the model"""
        pass

    @staticmethod
    @abstractmethod
    def buildObjFxn(two_d_vars, n_list, child_sub, obj_fxn_weight_list):
        """ Add children histogram cell terms to the objective function"""
        pass

    def addDPQueriesToModel(self, model, two_d_vars, obj_fxn, parent_mask, **kwargs) -> None:
        """ Add dp query terms to the objective function and constraints for the aux variables from those terms to the model"""
        return obj_fxn

    @abstractmethod
    def reformSolution(self, two_d_vars, parent_mask, child_floor=None) -> List[np.ndarray]:
        """ Form children histograms from optimized values of gurobi model vars"""
        pass

    def addStackedConstraints(self, model, parent_mask, two_d_vars, child_floor=None) -> None:
        """
        Wrapper function that adds constraints to the model
        Inputs:
            model:  gurobi model object
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model (i.e. possible non-zero cells of the within the child joint histogram array)
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            child_floor: a 2D numpy array used in the rounder (joint histogram array size X number of children)
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        for c in self.constraints:
            self.addConstraint(c, model, parent_mask, two_d_vars, self.rounder, child_floor)

    @staticmethod
    def addConstraint(st_con: StackedConstraint, model, parent_mask, two_d_vars, rounder=False, child_floor=None) -> None:
        """
        Adds stacked constraints to the model
        Inputs:
            st_con: StackedConstraint object (see constraints_dpqueries.py)
            model:  gurobi model object
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model (i.e. possible non-zero cells of the within the child joint histogram array)
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            rounder: bool indicating if it's the Rounder or L2Opt
            child_floor: a 2D numpy array used in the rounder (joint histogram array size X number of children)
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)

        matrix_rep = st_con.query.matrixRep()[:, parent_mask]
        sense = maps.toGRBFromStr()[st_con.sign]

        # Some constraints only appear in a subset of children. stacked_constraint.indices are indices of those children
        for rhs, child_num in zip(st_con.rhsList, st_con.indices):
            if rounder:  # Make constraint to work on the leftovers
                # Find the right hand side for leftovers
                accounted4byl2opt = st_con.query.answer(child_floor[:, child_num])  # Accounted for by L2 Optimizer
                rhs = rhs - accounted4byl2opt                                       # Left for the Rounder

            # Set a constraint for each value in query answer
            # In the shape Ax=b (appropriate sense instead of '=')
            model.addMConstrs(A=matrix_rep, x=two_d_vars[:, child_num], sense=sense, b=rhs, name=st_con.name)

    @staticmethod
    def addGroupedChildTotalConstraint(model, main_hist_size, two_d_vars, child_groups, main_n, rounder=False, child_floor=None) -> None:
        """
        Ands total constraint for each group of children in child_groups.
        Inputs:
            model:  gurobi model object
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model (i.e. possible non-zero cells of the within the child joint histogram array)
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            child_groups: iterable of tuples of children indices and totals. Children within indices should have totals constrained to the total value given
            main_n: number of vars in two_d_var corresponding to the main histogram. Only adding the total of main histogram constraint
            rounder: bool indicating if it's the Rounder or L2Opt
            child_floor: a 2D numpy array used in the rounder (joint histogram array size X number of children)
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)

        for groupnum, (group, total) in enumerate(child_groups):
            rhs = total
            cons_expr = 0
            #  Note: uses just sum instead of a query. Possible to change to query, like in the .addConstraint(). That would require creating the
            # constraint in a regular way, with query matrix over all histograms and rhs. It would then have to be removed from all children
            # and applied here on the groups
            for child_num in group:
                if rounder:  # Make constraint to work on the leftovers
                    # Find the right hand side for leftovers
                    accounted4byl2opt = child_floor[:main_hist_size, child_num].sum()  # Accounted for by L2 Optimizer
                    rhs = rhs - accounted4byl2opt  # Left for the Rounder
                cons_expr += two_d_vars[:main_n, child_num].sum()
            model.addConstr(cons_expr == rhs, name=f"State_total#{groupnum}")

    def findDesiredIndices(self,
                           backup_feas: bool,
                           min_schemas: Iterable[Tuple[int, ...]],
                           array_shapes: Iterable[Tuple[int, ...]],
                           arrays: List[np.ndarray]) -> Tuple[List[int], np.ndarray, Union[None, np.ndarray]]:
        """
        Finds the cells corresponding to gurobi variables (i.e. possible non-zeros in the histograms) from
        whether they are already zeroed in the parent (if they are zero, then non-negative child cells summing to them are also zero)
        Inputs:
            arrays: iterable of numpy arrays (1 per histogram) to find non-zero indices (typically self.parent or parent_diff)
            backup_feas: bool indicating if using backup feasibility
            min_schemas: iterable of multidimensional array indexes of the minimal schema, 1 per histogram
        Outputs:
            parent_mask: 1-d boolean vector over the joint flattened parent histograms array indicating which corresponding cells in the children
                        should be included in the model
            parent_sub: 1-d array of subset (by parent_mask) of the joint flattened parents
        """

        # Sizes are products of all dimensions
        array_sizes = [int(np.prod(sh)) for sh in array_shapes]

        # Is is the topmost node
        has_parent_flag = self.parent[0] is not None

        # For the topmost node, every cell is a variable to optimize, "mask" if full, and n is all cells in the histogram (i.e. n=array size)
        parent_mask = np.ones(int(np.sum(array_sizes)), dtype=bool)
        n_list = array_sizes

        # The most regular optimization case: the optimization variables are only those that are non-zero in parent
        if not backup_feas and has_parent_flag:
            hist_pmasks = tuple(array.flatten() > 0 for array in self.parent)  # Find parent mask for each histogram, by checking which parent terms are NZ
            parent_mask = np.hstack(hist_pmasks)  # Stack them together for the mask on joint_array
            n_list = [int(np.sum(hist_pmask)) for hist_pmask in hist_pmasks]  # Sum number of NZ in each hist and put in a list of how-many-var-in-hist

        # Backup feasibility regime
        elif has_parent_flag:
            n_list = []
            parent_mask = np.empty(0, dtype=bool)
            for i, (array, min_schema) in enumerate(zip(self.parent, min_schemas)):
                # For backup feasibility with min_schema: Histogram cell is in play if the sum of these cells over min_schema is non-zero
                if min_schema:
                    temp = array.sum(min_schema) > 0
                    for dim in min_schema:
                        temp = np.repeat(np.expand_dims(temp, dim), array.shape[dim], axis=dim)
                    n_list.append(int(np.sum(temp)))
                    parent_mask = np.hstack((parent_mask, temp.flatten()))
                # If no min_schema provided for this histogram, all of its cells are in play
                else:
                    n_list.append(int(np.prod(array.shape)))
                    parent_mask = np.hstack((parent_mask, np.ones(int(np.prod(array.shape)), dtype=bool)))

        # Whichever parent_mask we calculated above, apply it to joint flattened array of the histograms
        parent_sub = np.hstack([array.flatten() for array in arrays])[parent_mask] if has_parent_flag else None
        return n_list, parent_mask, parent_sub

    def saveModelToS3(self, model):
        """
        Determine s3path where LP file will be saved.
        Saves it.
        Returns the save location
        :param model: the model to be saved.
        :return: the path as an s3:// URL.
        """

        import gurobipy as gb

        s3prefix = os.path.join(self.do_expandvars(val=self.save_lp_path, expandvars=True), self.identifier)
        s3prefix = s3prefix.replace(' ','_')
        s3path   = (os.path.join(s3prefix , self.identifier + "_" + str(uuid.uuid4()) + ".zip")).replace(' ','_')

        # Count how many files have been saved with this prefix. If it is more than 10, don't save
        p      = urlparse(s3prefix)
        bucket = boto3.resource('s3').Bucket(p.netloc)
        objs   = list(bucket.objects.filter(Prefix=p.path[1:]))
        if len(objs) > CC.MAX_SAVED_S3_FILES_PER_GEOLEVEL:
            print(f"saveModelToS3: Already saved {len(objs)}. Will not save {s3path}")
            return None

        # Compress the LP file locally into a .zip file and upload it.

        with tempfile.TemporaryDirectory(dir='/mnt/tmp') as td:
            basename = os.path.join(td, self.identifier)
            zfname = basename + ".zip"
            lpname = basename + ".lp"
            model.write(lpname)
            zip_call_cmd = ['zip', os.path.basename(zfname), os.path.basename(lpname)]
            if model.Status in [gb.GRB.INFEASIBLE, gb.GRB.INF_OR_UNBD]:
                ilpname = basename + ".ilp"
                model.computeIIS()
                model.write(ilpname)
                model.write("iis.ilp")
                zip_call_cmd.append(os.path.basename(ilpname))

            # Add it to the zip file. Crash if zip fails (it shouldn't)
            subprocess.check_call(zip_call_cmd, cwd=td)

            # Upload the file boto3 to s3
            p  = urlparse( s3path )
            s3 = boto3.resource( 's3' )

            print("saveModelToS3: {s3path}")
            s3.meta.client.upload_file( zfname, p.netloc, p.path[1:])
            return s3path

    def setObjAndSolve(self, model, obj_fxn):
        """
        Sets the objective function for the model and minimizes
        Inputs:
            model: gurobi model object
            obj_fxn: gurobi expression of the objective function
        Outputs:
        - Optionally saves the LP file to Amazon S3 if geocode matches save_lp_pattern and save_lp_path is set
        - writes model to /mnt/tmp/grb_hang_{timestr}.lp if GRB.TIME_LIMIT is exceeded.
        """

        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        if self.identifier == CC.ROOT_GEOCODE:
            model.Params.Threads = self.getint(CC.THREADS_R2R, section=CC.GUROBI, default=CC.DEFAULT_THREADS_R2R)
        else:
            # See if there is a geolevel-specific threads override
            threads_geolevel    = CC.THREADS_GEOLEVEL_PREFIX + self.child_geolevel
            model.Params.Threads = self.getint(threads_geolevel, section=CC.GUROBI, default=model.Params.Threads)


        report_reason = ''
        save_model    = False
        model.update()
        model.setObjective(obj_fxn, sense=gb.GRB.MINIMIZE)
        child_geo_len = self.childGeoLen

        if self.getboolean(CC.PYTHON_PRESOLVE, default=False):
            # Explicitly call presolve in the python API
            try:
                t_presolve_start = time.time()
                model.presolve()
                self.addGurobiStatistics(t_presolve = time.time() - t_presolve_start)

            except gb.GurobiError as e:
                self.addGurobiStatistics(presolve_error=True,
                                         error=True,
                                         presolve_error_message=str(e))
                report_reason += 'Gurobi error in presolve() '
                save_model = True

        ###
        ### RUN THE OPTIMIZER!!!
        ### This should be the ONLY PLACE in the entire DAS where optimize() is called.
        ###

        try:
            t_optimize_start = time.time()
            # model.Params.DualReductions = 0
            model.optimize()
            self.addGurobiStatistics(t_optimize = time.time() - t_optimize_start)

        except gb.GurobiError as e:
            self.addGurobiStatistics(optimize_error=True,
                                     error=True,
                                     optimize_error_message=str(e))
            report_reason += 'Gurobi error in optimize() '
            save_model = True

        # Alert if model required more than 1 node to solve (e.g. NP-hard)
        if model.NodeCount > 1:
            report_reason += f'model.NodeCount={model.NodeCount} '

        # Save non-optimal models
        if model.Status not in self.acceptable_statuses:
            report_reason += f'model status ({model.Status}) is not in {self.acceptable_statuses} '
            save_model = True

        if model.Status == gb.GRB.TIME_LIMIT:
            timestr = time.strftime("%Y%m%d-%H%M%S")
            model.write(HANG_FILE_TEMPLATE.format(timestr=timestr,geocode=self.identifier))
            report_reason += "GRB.TIME_LIMIT "

        # Save matching models
        if fnmatch.fnmatch(self.identifier, self.save_lp_pattern):
            save_model = True

        try:
            sec = self.t_optimize_end - self.t_optimize_start
            if  sec > self.save_lp_seconds:
                save_model = True
                report_reason += f' optimization took {sec} seconds '
        except (ValueError,AttributeError,TypeError):
            pass

        # Is this one of the nodes that we randomly report?
        if self.random_report:
            report_reason += f'(random_report) '
            save_model = True

        if save_model:
            s3path = self.saveModelToS3(model)
            if s3path is not None:
                report_reason += " Saved lpfile to "+s3path

        if report_reason:
            msg = self.identifier + ": " + report_reason
            dashboard.das_log(msg, code=dashboard.CODE_ALERT)

        if self.getboolean(CC.PRINT_GUROBI_STATS_OPTION, default=False):
            model.printStats()
        self.addGurobiModel(model)
        self.sendGurobiStatistics()

    @staticmethod
    def buildAndAddParentConstraints(model, parent_sub, two_d_vars, name):
        """
        Adds the parent constraints to the model
        Inputs:
            model: gurobi model object
            parent_sub: 1-d array of subset (by parent_mask) of parent
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            name: str giving the name of the constraint
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)

        n, num_children = two_d_vars.shape

        # Vector constraint for all the cells at once, but have to manually sum over children
        cons_expr = 0
        for child_num in range(num_children):
            cons_expr += two_d_vars[:, child_num]
        model.addConstr(cons_expr == parent_sub, name=name)

        # # Adding the constraint in a single go. Doesn't seem simpler, still need to loop over children to create the matrix. Might be faster.
        # if n > 0:
        #     tdv_flat = two_d_vars.sum().x
        #     sum_over_children_mat = ss.lil_matrix((n, n * num_children), dtype=int)
        #     for i in range(num_children):
        #         sum_over_children_mat[:, i::num_children] = np.identity(n)
        #     model.addConstr(sum_over_children_mat.tocsr() @ tdv_flat == parent_sub, name=name)

        # # Original way: a constraint per cell
        # for i in range(n):
        #     model.addConstr(two_d_vars[i, :].sum() == parent_sub[i], name=name + "_" + str(i))

    def buildBackupObjFxn(self, model, two_d_vars, n_list, array_list, array_sub, parent_mask):
        """
        Builds the backup objective function in the l2 and rounder classes and adds constraints
        ensuring that the non-minimal schema marginals are kept equal.
        Inputs:
            model: gurobi model object
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            array: numpy multidimensional array (either parent or parent_diff)
            array_sub: 1-d array of subset of array by the parent_mask
            parent_mask: 1-d boolean vector over the flattened parent indicating which corresponding cells in the children
                        should be included in the model
            min_schema: list of multidimensional array indexes of the minimal schema
            name: str giving the name of the constraint
        Outputs:
            backup_obj_fxn: gurobi expression giving the backup objective function
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        rhs = array_sub
        # backup_obj_fxn = 0  # This is gb.MLinExpr()
        n, num_children = two_d_vars.shape

        slack = model.addMVar(n, vtype=gb.GRB.CONTINUOUS, lb=0.0, name="backup")
        # Minimizing total slack
        backup_obj_fxn = slack.sum()
        # Constraints are | rhs - sum of the cell over children | <= slack
        sum_over_chldrn = 0
        for child_num in range(num_children):
            sum_over_chldrn += two_d_vars[:, child_num]
        parent_children_slack = rhs - sum_over_chldrn
        model.addConstr(slack >= parent_children_slack)
        model.addConstr(slack >= -parent_children_slack)

        # TODO: Only works with a min schema on the first histogram.
        #  Also applies slack above to cells of all histograms. Check whether it's appropriate
        # Add marginal constraint
        if self.min_schema[0] and n_list[0] > 0:
            min_schema = self.min_schema[0]
            arr2sum = array_list[0]
            parent_mask_ms = arr2sum.sum(min_schema).flatten() > 0
            rhs = arr2sum.sum(min_schema).flatten()[parent_mask_ms]
            back_marg_query = querybase.QueryFactory.makeTabularGroupQuery(self.parent_shape[0], add_over_margins=min_schema)
            tmp_kron = back_marg_query.matrixRep()
            row_ind = parent_mask_ms
            parent_mask_0 = parent_mask[:self.hends[0]]
            matrix_rep = tmp_kron[:, parent_mask_0][row_ind, :]

            expr = 0
            for child_num in range(num_children):
                expr += matrix_rep @ two_d_vars[:n_list[0], child_num]
            model.addConstr(expr == rhs, name=self.backup_obj_fxn_constr_name)

        return backup_obj_fxn

    def backupFeasOptimize(self, model, backup_obj_fxn):
        """
        Set and optimize the backup objective function, print information and return the objective value
        Inputs:
            model: gurobi model object
            backup_obj_fxn: gurobi expression giving the backup objective function
        Outputs:
            model.ObjVal: the objective value of the model after optimizing
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        self.setObjAndSolve(model, backup_obj_fxn)
        fail_msg = f"On {getIP()}, degree-of-infeasibility {model.ModelName} backup solve failed with status {model.Status} for parent {self.identifier}. " \
            f"This should not happen! Check that min schema is specified correctly. Exiting program..."
        assert model.Status == gb.GRB.OPTIMAL, fail_msg
        return model.ObjVal

    @staticmethod
    def backupFeasAddConstraint(model, backup_obj_val, backup_obj_fxn, obj_slack=1.0):
        """
        Add the constraint to the model based on the backup objective optimized value plus some slack
        Inputs:
            model: gurobi model object
            backup_obj_val: the objective value of the model after optimizing
            backup_obj_fxn: gurobi expression giving the backup objective function
            obj_slack: float >= 0 giving the amount of slack provided to the constraint
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        model.addConstr(backup_obj_fxn <= backup_obj_val + obj_slack)

    def padQueryMatrix(self, ihist, matrix_rep, n_ans):
        """Add empty columns for preceding and succeeding histograms to the matrix"""
        if ihist > 0:  # Add empty columns in front if the histogram (and hence the query in MultiHistQuery) is not the first
            matrix_rep = ss.hstack((ss.csr_matrix((n_ans, int(np.cumsum(self.hist_sizes[:ihist]))), dtype=int), matrix_rep)).tocsr()
        if ihist < len(self.hist_sizes) - 1:  # Add empty columns after if the histogram (and hence the query in MultiHistQuery) is not the last
            matrix_rep = ss.hstack((matrix_rep, ss.csr_matrix((n_ans, int(np.cumsum(self.hist_sizes[ihist + 1:]))), dtype=int))).tocsr()
        return matrix_rep

    def splitHistograms(self, joint_histogram: np.ndarray) -> List[np.ndarray]:
        """
        Split the joint flattened histogram array into individual histograms and reshape them back into multidimensional
        arrays with hist_shape+number_of_children shape
        :param joint_histogram: 1 x num_children array representing all of the histograms, flattened and stacked
        :return: individual histograms in their appropriate shape (times number of children)
        """
        return [joint_histogram[hstart:hend, :].reshape(shape + (self.childGeoLen, )) for hstart, hend, shape in zip(self.hstarts, self.hends, self.parent_shape)]

    @staticmethod
    def reverseQueryOrdering(input_dict):
        """
            Takes a dict
                a = {int -> [str, ..., str]
            And inverts it to
                b = {str -> set([int, ..., int])}
        """
        reversed_dict = defaultdict(set)
        for pass_num in input_dict.keys():
            for dpqName in input_dict[pass_num]:
                reversed_dict[dpqName].add(pass_num)
        return reversed_dict
