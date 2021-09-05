"""
engine_utils.py:
Contains:
DASEngineHierarchical - parent class for engines, containing functions shared between engines
    including the actual implementation of the top-down algorithm
The actual calls to gurobi appear in nodes.manipulate_nodes, which is called in a .map
(And which runs on the worker nodes).
"""

import logging
import os
import os.path
from abc import ABCMeta, abstractmethod
from typing import Tuple, List, Dict, Union
from configparser import NoOptionError, NoSectionError
from fractions import Fraction

import json
import pickle

import numpy as np

from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from das_framework.driver import AbstractDASEngine, DAS
from exceptions import DASValueError

import das_framework.ctools.clogging as clogging
import programs.engine.primitives as primitives
import programs.queries.querybase as querybase
import programs.queries.constraints_dpqueries as cons_dpq
from programs.engine.budget import Budget
from programs.engine.optimizer_query_ordering import OptimizationQueryOrdering

from programs.das_setup import DASDecennialSetup
from programs.nodes.nodes import GeounitNode
from programs.rdd_like_list import RDDLikeList

import das_utils

from constants import CC

__NodeRDD__ = RDD
__NodeDict__ = Dict[str, __NodeRDD__]
__Nodes__ = Union[__NodeRDD__, __NodeDict__]

DENOM_MAX_POWER = np.ceil(np.log2(CC.PRIMITIVE_FRACTION_DENOM_LIMIT) / 2)
MAX_DENOM = int(2 ** DENOM_MAX_POWER)


class DASEngineHierarchical(AbstractDASEngine, metaclass=ABCMeta):
    """
    Parent class for engines, containing functions shared between engines.
    For engines that infuse noise at each geolevel, setPerGeolevelBudgets() sets the budgets
    For engines that use predefined (in config) queries setPerQueryBudgets() set the budget distribution between queriesy
    For engines using topdown optimization, topdown function is implemented
    noisyAnswers is implemented here only for topdown engines
    """

    # pylint: disable=too-many-instance-attributes
    setup: DASDecennialSetup                                # Setup module/object of the DAS
    minimal_schema: Tuple[str, ...]                         # vars in minimal schema (for minimal schema engine)
    vars_schema: Dict[str, int]                             # vars in the full schema
    hist_shape: Tuple[int, ...]                             # Shape of the data histogram
    budget: Budget                                          # Instance of Budget class holding all the PLB allocations

    write_all_geolevels: bool                               # Should we write all geolevels as MDFs?

    per_attr_epsilons: Dict[str, float]                     # dimname -> computed epsilon
                                                            # Used for single-attr semantics statements like:
                                                            #  If <reference person's record> is changed in just <dimname>, leaving
                                                            # <other histogram dims> unchanged, then an attacker's posterior won't
                                                            # differ by more than exp(2 per_attr_epsilon)...

    per_geolevel_epsilons: Dict[str, float]                 # geolevel_name -> computed epsilon
                                                            # Used for semantic statements like:
                                                            #   If <reference person's record> is changed from <geounit in
                                                            # lowest geolevel> to <other geounit in lowest geolevel> without altering
                                                            # their <geounit in geolevel>, then... (e.g., statements relevant to
                                                            # protecting Block in BG, but not protecting BG or above)

    rounder_query_names: List[str]                          # Queries to target in rounder optimization
    est_and_qadd_queries: bool                              # Whether to fill out the dict with "Est" and "Qadd" queries. Set automatically, defined by l2_optimization_approach
    opt_dict: Dict[int, Dict[str, Tuple[str, ...]]]         # Dict where "Est" and "Qadd" names are kept
    query_ordering: Dict[str, Dict[int, str]]               # L2 targets, L2 constrain-to, and Rounder multipass query ordering
    optimizers: Tuple[str]                                  # Which sequential optimizer, L2 optimizer and rounder to use

    saveloc: str                                            # Where to save noisy measurements

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Person histogram shape to get later from setup object, once reader is initiated and know table parameters
        self.hist_shape = self.setup.hist_shape
        self.unit_hist_shape = self.setup.unit_hist_shape

        # pylint: disable=bad-whitespace
        self.all_levels: Tuple[str, ...] = self.setup.levels
        self.all_levels_reversed: Tuple[str, ...] = tuple(reversed(self.all_levels))

        self.levels_reversed = []
        for level in self.all_levels_reversed:
            self.levels_reversed.append(level)
            if level == self.setup.geo_bottomlevel:
                break

        self.levels = tuple(reversed(self.levels_reversed))

        print(f'self.all_levels: {self.all_levels}')
        print(f'self.all_levels_reversed: {self.all_levels_reversed}')
        print(f'self.levels: {self.levels}')
        print(f'self.levels_reversed: {self.levels_reversed}')

        self.spine_type = self.setup.spine_type
        self.schema: str = self.setup.schema
        logging.info(f"levels: {self.levels}")

        self.vars_schema = {var: i for i, var in enumerate(self.setup.hist_vars)}
        self.log_and_print(f"the variables in the schema are: {self.vars_schema} (reader names)")
        try:
            self.log_and_print(f"Their names in the {self.schema} schema are {self.setup.schema_obj.dimnames}")
        except AssertionError:
            self.log_warning_and_print(f"Schema {self.schema} is not supported")

        self.write_all_geolevels = self.getboolean(CC.WRITE_ALL_GEOLEVELS, section=CC.WRITER, default=False)

        # Default is False. It is used for fail safe in the optimizer
        self.minimal_schema = self.gettuple(CC.MINIMALSCHEMA, section=CC.CONSTRAINTS, sep=" ", default=False)

        # Whether to run in spark (there is a local/serial mode, for testing and debugging)
        self.use_spark = self.setup.use_spark

        # Whether to save noisy answers
        self.save_noisy: bool   = self.getboolean(CC.SAVENOISY, default=True) if self.use_spark else False

        # Whether to discard noisy answers and reload them (makes sure
        # the noise is only drawn once and not again during RDD
        # reconstruction)

        self.reload_noisy: bool = self.getboolean(CC.RELOADNOISY, default=True) if self.save_noisy else False

        # For restarting the DAS from the point after the noise was
        # generated and saved, so that no additional budget is spent
        self.postprocess_only: bool = self.setup.postprocess_only

        if self.postprocess_only:
            # Unique run ID of the run where the noisy answers were saved, to load for postprocess-only
            self.saved_noisy_app_id: str = self.getconfig(CC.SAVED_NOISY_APP_ID)

        try:
            self.optimization_start_from_level = self.getconfig(CC.OPTIMIZATION_START_FROM_LEVEL)
        except (NoSectionError, NoOptionError):
            self.optimization_start_from_level = None

        if self.das.make_bom_only():
            self.app_id = CC.APPID_NO_SPARK
        else:
            self.app_id: str = clogging.applicationId()  # if self.use_spark else CC.APPID_NO_SPARK

        self.validate_levels: list = self.getiter(key=CC.VALIDATE_AT_LEVEL, section=CC.VALIDATOR_SECTION, default=False)

        # Privacy protection mechanism (see primitives.py). Geometric is default, set explicitly in subclasses
        self.mechanism_name = self.setup.dp_mechanism_name
        self.mechanism = primitives.basic_dp_answer
        self.no_noise_mechanism = primitives.NoNoiseMechanism

        # Shares of budget designated to each geolevel. Should be None if not doing by-level bugdet (e.g.bottomup engine)
        self.geolevel_prop_budgets: tuple = None
        self.geolevel_prop_budgets_dict: dict = None

        self.initializeAndCheckParameters()  # TODO: Turn this on to have config parameters checked before reading

    def initializeAndCheckParameters(self):
        """
        The things done here should conceptually be done in the __init__, but since the experiment regime in
        das_framework skips the setup part for the experimental runs expect initial ones, this function has to be
        called to reset the parameters to the values set in the new config corresponding to the new experimental run
        :return:
        """
        # self.total_budget: Fraction = self.getfraction(CC.EPSILON_BUDGET_TOTAL, section=CC.BUDGET)
        # self.log_and_print(f"The total budget is: {self.total_budget}")
        # assert(self.total_budget > 0.), "Total budget is non-positive!"

        # total_budget is now a derived quantity. global_scale is specified in the configuration file.

        self.budget = Budget(self.all_levels, self.setup, config=self.config, das=self.das)

        self.optimization_query_ordering = OptimizationQueryOrdering(self.budget, self.setup.schema_obj, self.unit_hist_shape, config=self.config, das=self.das)

        # Where to save noisy answers (different folder for each experiment, so here) or load from for postprocessing only
        if self.save_noisy or self.postprocess_only:
            output_path = self.das.writer.output_path
            self.saveloc: str = os.path.join(output_path, self.getconfig(CC.NOISY_MEASUREMENTS_POSTFIX, default="noisy_measurements")).strip()

    @abstractmethod
    def run(self, original_data: __NodeRDD__) -> None:
        """
        Run the engine.
        The prototype does not do anything to the data, just sets up engine configuration
        :param original_data: lowest (block) level Geounit nodes RDD
        :return: unchanged original data (protection of data is implemented in subclasses)
        """
        self.annotate(f"{self.__class__.__name__} run")

        # initialize parameters
        self.initializeAndCheckParameters()

        # print the histogram shape
        print(f"Histogram shape is: {self.hist_shape}")

        pass

    def getNodesAllGeolevels(self, block_nodes: __NodeRDD__, additional_ic: bool = True) -> __NodeDict__:
        """
        This function takes the block_nodes RDD and aggregates them into higher level nodes.GeounitNode RDD objects:

        Inputs:
            block_nodes: RDDs of GeounitNode objects aggregated at the block level
            additional_ic: For bottom up engine, it is not needed to deal with constraints, so can be set to False

        Outputs:
            by-level dictionary of nodes
        """
        all_nodes_dict: __NodeDict__ = {self.all_levels[0]: block_nodes}
        # print(f"Blocks in getAllGeolevels max part: {das_utils.maxPartPerKey(block_nodes, lambda n: n.parentGeocode)}")

        try:
            level_partitions = self.gettuple_of_ints("geolevel_num_part")
        except NoOptionError:
            level_partitions = [0] * len(self.all_levels)

        if len(level_partitions) != len(self.all_levels):
            raise DASValueError(f"The partition numbers for levels vector {level_partitions} ([engine]/geolevel_num_part) is not the same length as"
                                 f"levels vector {self.all_levels}. No repartitioning will be performed", value=level_partitions)

        for level, upper_level, num_parts in zip(self.all_levels[:-1], self.all_levels[1:], level_partitions[1:]):
            all_nodes_dict[upper_level] = all_nodes_dict[level] \
                                        .map(lambda node: (node.parentGeocode, node)) \
                                        .reduceByKey(lambda x, y: x.addInReduce(y)) \
                                        .map(lambda d: (lambda geocode, node: node.shiftGeocodesUp())(*d))

            # print(f"{level} max part: {das_utils.maxPartPerKey(nodes_dict[level], lambda n: n.parentGeocode)}")
            # print(f"{upper_level} max part: {das_utils.maxPartPerKey(nodes_dict[upper_level], lambda n: n.parentGeocode)}")

            if num_parts > 0:
                all_nodes_dict[upper_level] = all_nodes_dict[upper_level].coalesce(num_parts)

            # Maybe also repartition by parent, or with shuffle
            # nodes_dict[upper_level] = das_utils.partitionByParentGeocode(nodes_dict[upper_level])

            # print(f"{level} max part: {das_utils.maxPartPerKey(nodes_dict[level], lambda n: n.parentGeocode)}")
            # print(f"{upper_level} max part: {das_utils.maxPartPerKey(nodes_dict[upper_level], lambda n: n.parentGeocode)}")

            if additional_ic:
                all_nodes_dict[upper_level] = \
                    all_nodes_dict[upper_level].map(lambda node: node.makeAdditionalInvariantsConstraints(self.setup))
            all_nodes_dict[upper_level].persist()

            # Check that newly made level RDD of nodes satisfies constraints
            # TODO: Decide how default we want this, and where the function should reside
            if self.setup.validate_input_data_constraints:
                DAS.instance.reader.validateConstraintsNodeRDD(all_nodes_dict[upper_level], upper_level)

        # Populate the nodes_dict with only those levels that are contained in the relevant levels
        nodes_dict = {}
        for (level, rdd) in all_nodes_dict.items():
            if level in self.levels:
                nodes_dict[level] = rdd

        print(f'all_nodes_dict: {len(all_nodes_dict)}, {all_nodes_dict.keys()}')
        print(f'nodes_dict: {len(nodes_dict)}, {nodes_dict.keys()}')

        # garbage collect for memory
        self.freeMemRDD(block_nodes)

        return nodes_dict

    def makeOrLoadNoisy(self, nodes: __Nodes__) -> __Nodes__:
        """
        Obtain noisy nodes for postprocessing. If [engine]/POSTPROCESS_ONLY config option is True, then
        load the noisy nodes saved in the DAS run with application-id indicated in the config file
        [engine]/NOISY_APP_ID.
        Otherwise, generate noisy nodes (i.e. take noisy measurements)
        If SAVENOISY option is True, save the noisy nodes.
        If RELOADNOISY option is True, discard the noisy nodes from memory and load them from the saved files.
        :rtype: Nodes
        :param nodes: RDD with geoNodes without the noisy data of the bottom geographical levels or by-level dict of RDDs of nodes
        :return: by-geolevel dictionary of noisy nodes RDDs or a single RDD with nodes
        """
        if self.postprocess_only:
            self.log_and_print("Postprocessing only, loading noisy answers")
            return self.loadNoisyAnswers(self.saved_noisy_app_id)
        else:
            self.annotate("Generating noisy answers")

            # make higher level nodes by aggregation
            # getNodesAllGeolevels is defined in engine_utils().
            # It returns a dictionary of RDDs, one for each geolevel. (Except in bottom up where it does nothing and returns block nodes)

            nodes: __Nodes__ = self.getNodesAllGeolevels(nodes)

            # This functions return either a single RDD or a dictionary, depending on subclass
            nodes: __Nodes__ = self.noisyAnswers(nodes)
            nodes: __Nodes__ = self.deleteTrueData(nodes)

            # Save the noisy nodes if required. Delete them from memory and reload if required
            nodes: __Nodes__ = self.saveAndOrReload(nodes)

            return nodes

    def getQueriesDict(self):
        return self.budget.query_budget.queries_dict

    @abstractmethod
    def getNodePLB(self, node: GeounitNode):
        pass

    @abstractmethod
    def noisyAnswers(self, nodes: __Nodes__, **kwargs) -> __Nodes__:
        """ Return noisy answers for nodes with true data"""
        pass

    @abstractmethod
    def freeMemNodes(self, nodes: __Nodes__) -> None:
        """ Unpersist and del nodes and collect the garbage"""
        pass

    def saveAndOrReload(self, nodes: __Nodes__) -> __Nodes__:
        """ Save the noisy nodes if required. Delete them from memory and reload if required """
        if self.save_noisy:
            self.annotate("Saving noisy answers")
            self.saveNoisyAnswers(nodes, repart_by_parent=False)

            if self.reload_noisy:
                self.freeMemNodes(nodes)

                self.annotate("Reloading noisy answers")
                return self.loadNoisyAnswers(self.app_id)

        return nodes

    def deleteTrueData(self, nodes: Union[__NodeRDD__, __NodeDict__]) -> __NodeRDD__:
        """
        Deletes true data from a single RDD of geoNodes (nodes.py) and checks that it's done
        :param nodes: RDD of geoNodes
        :return: RDD of geoNodes with 'raw' field equal to None
        """
        if self.getboolean(CC.DELETERAW, default=True):
            logging.info("Removing True Data Geolevel Array")
            ###
            ## explicitly remove true data "raw"
            ##
            nodes = nodes.map(lambda node: node.deleteTrueArray()).persist()
            ##
            ## check that true data "raw" has been deleted from each, if not will raise an exception
            ##
            nodes.map(lambda node: node.checkTrueArrayIsDeleted())
        return nodes

    def saveNoisyAnswers(self, nodes: __NodeDict__, repart_by_parent=True, postfix: str = "") -> None:
        """
        Save RDDs with geonodes as pickle files, by geolevel
        :param repart_by_parent:
        :param nodes: RDD or by-geolevel dictionary of noisy nodes RDDs
        :param postfix: postfix to add to default filename (e.g. "_ms" for minimal_schema run)
        :return:
        """
        if self.setup.dvs_enabled:
            from programs.python_dvs.dvs import DVS_Singleton
            dvs_singleton = DVS_Singleton()
        else:
            dvs_singleton = None

        noisy_partitions_dict = self.setup.noisy_partitions_dict

        for level, nodes_rdd in nodes.items():
            self.annotate(f"Saving {level}{postfix} noisy measurements")
            path = self.noisyPath(self.app_id, level, postfix)
            num_noisy_parts = noisy_partitions_dict[level]
            rdd2save = nodes_rdd
            if repart_by_parent:
                self.annotate(f"Repartitioning by parent geocode")
                rdd2save = das_utils.partitionByParentGeocode(nodes_rdd, nodes_rdd.getNumPartitions())
            elif num_noisy_parts > 0:
                self.annotate(f"Coalescing noisy measurements to {num_noisy_parts} parts")
                self.annotate(f"NOTE: NOT actually Coalescing noisy measurements to {num_noisy_parts} parts")
                # rdd2save = nodes_rdd.coalesce(num_noisy_parts)
            rdd2save = rdd2save.map(lambda node: node.zipNoisy())
            das_utils.savePickledRDD(path, rdd2save, dvs_singleton=dvs_singleton)

        das_utils.saveConfigFile(os.path.join(self.saveloc, f"{self.app_id}-bylevel_pickled_rdds.config"), self.config)

    def loadNoisyAnswers(self, application_id: str, postfix: str = "", levels2load=None) -> __NodeDict__:
        """
        Load pickled noisy geonodes for all levels
        :param levels2load:
        :param application_id: Unique DAS run ID which is part of filenames to load
        :param postfix: postfix to add to default filename (e.g. "_ms" for minimal_schema run)
        :return: by-geolevel dictionary of noisy nodes RDDs
        """
        spark = SparkSession.builder.getOrCreate()

        # # Only load bottom level if no budgets spent on higher levels (like in bottom-up)
        # levels2load = self.levels if self.geolevel_prop_budgets is not None else self.levels[:1]
        if levels2load is None:
            levels2load = self.levels

        nodes_dict = {}
        for level in levels2load:
            path = self.noisyPath(application_id, level, postfix)
            if path.startswith(CC.HDFS_PREFIX):
                level_rdd = spark.sparkContext.pickleFile(das_utils.expandPathRemoveHdfs(path))
            elif das_utils.isS3Path(path):
                level_rdd = spark.sparkContext.pickleFile(path)
            else:
                level_rdd = spark.sparkContext.parallelize(pickle.load(path))
            # level_rdd = level_rdd.map(lambda node: node.unzipNoisy())
            nodes_dict[level] = level_rdd if self.use_spark else RDDLikeList(level_rdd.collect())

        return nodes_dict

    def noisyPath(self, application_id, level, postfix):
        """ Form the filename to save/load noisy measurements
        :param application_id: Unique DAS run ID which is part of filenames to load/save
        :param postfix: postfix to add to default filename (e.g. "_ms" for minimal_schema run)
        :param level: for which geolevel is the saved/loaded file
        """
        return os.path.join(self.saveloc, f"{application_id}-{level}{postfix}.pickle")

    @staticmethod
    def freeMemRDD(some_rdd: __NodeRDD__) -> None:
        """ Remove RDD from memory, with garbage collection"""
        das_utils.freeMemRDD(some_rdd)

    def freeMemLevelDict(self, nodes_dict: __NodeDict__, down2level=0) -> None:
        """Unpersist and remove RDDs for each geolevel from memory"""
        for level in self.levels[down2level:]:
            self.freeMemRDD(nodes_dict[level])

    def makeDPNode(self, geounit_node: GeounitNode) -> GeounitNode:
        """
        This function takes a GeounitNode with "raw" data and generates
        noisy DP query answers depending the specifications in the
        config object.

        NOTE: This function is called inside the mapper (see above),
        so it is executed for every GeounitNode, on the workers.

        This may be confusing, because the .map() function is called from
        self.noisyAnswers() (above), which is run on the Master node.

        Inputs:
            geounit_node: a Node object with "raw" data
            dp_queries: boolean indicating whether dpqueries are present in measurement set, based on config
        Outputs:
            dp_geounit_node: a Node object with selected DP measurements

        """
        logging.info(json.dumps({'geocode': geounit_node.geocode, 'geolevel': geounit_node.geolevel}))

        geolevel_prop = self.getNodePLB(geounit_node)

        main_hist: np.ndarray = geounit_node.getDenseRaw()
        unit_hist: np.ndarray = geounit_node.getDenseRawHousing()

        dp_geounit_node = geounit_node
        dp_geounit_node.dp_queries, dp_geounit_node.unit_dp_queries = self.nodeDPQueries(geolevel_prop, main_hist,
                                                                                        unit_hist, geounit_node.geolevel)

        if self.optimization_query_ordering.est_and_qadd_queries:
            print("multi_pass_try")
            # make estimation queries
            dp_geounit_node = self.optimization_query_ordering.makeOptQueries(dp_geounit_node)

        rounder_queries = {}
        if self.optimization_query_ordering.rounder_query_names:
            rounder_queries = self.setup.schema_obj.getQueries(list(self.optimization_query_ordering.rounder_query_names[geounit_node.geolevel]))
        dp_geounit_node.rounder_queries = rounder_queries
        dp_geounit_node.query_ordering = self.optimization_query_ordering.query_ordering[geounit_node.geolevel]

        return dp_geounit_node

    def getAdjustedInverseScalePerQuery(self, dpq_prop, geolevel_prop):
        """
            To avoid floating-point ops inside primitives, we work in squared scale for eps, delta DP.
        """
        global_scale = self.budget.global_scale
        if self.setup.privacy_framework in (CC.ZCDP,):
            return dpq_prop * geolevel_prop  / (global_scale**2)  # in zCDP, global_scale is not in squared units in config
        elif self.setup.privacy_framework in (CC.PURE_DP,):
            return (dpq_prop * geolevel_prop) / global_scale
        else:
            raise ValueError(f"Privacy framework {self.setup.privacy_framework} not recognized!")

    def nodeDPQueries(self, geolevel_prop: Fraction, main_hist: np.ndarray, unit_hist,
                            geolevel) -> Tuple[Dict[str, cons_dpq.DPquery], ...]:
        """
        For engines which split the budget between some preset queries,
        make measurements on those preset queries on a given node histogram.
        :param unit_hist: unit histogram
        :param geolevel_prop: budget dedicated to the present geolevel
        :param main_hist: the main node histogram (person or household)
        :param geolevel: geolevel
        :return:
        """

        dp_queries = {}
        unit_dp_queries = {}
        assert isinstance(geolevel_prop, Fraction)

        for query, dpq_prop in self.budget.query_budget.queryPropPairs(geolevel):
            self.addDPQ2Dict("main hist", query, dp_queries, dpq_prop, geolevel_prop, main_hist)

        for query, dpq_prop in self.budget.query_budget.unitQueryPropPairs(geolevel):
            self.addDPQ2Dict("unit", query, unit_dp_queries, dpq_prop, geolevel_prop, unit_hist)

        return dp_queries, unit_dp_queries

    def addDPQ2Dict(self, query_set_name, query, dp_queries, dpq_prop, geolevel_prop, main_hist):
        inverse_scale = self.getAdjustedInverseScalePerQuery(dpq_prop, geolevel_prop)
        if inverse_scale < 0.:
            raise ValueError(f"{query_set_name} query {query.name} received dpq_prop, geolevel_prop {dpq_prop, geolevel_prop}")
        dp_query = self.makeDPQuery(hist=main_hist, query=query, inverse_scale=inverse_scale)
        dp_queries.update({query.name: dp_query})

    def makeDPQuery(self, hist, query: querybase.AbstractLinearQuery, inverse_scale: Fraction) -> cons_dpq.DPquery:
        """
        For the data :hist: and :query: on it, creates DP mechanism to protect it, applies it on the true
        answer, and creates DPquery that contains the query, the dp_answer and mechanism parameters
        :rtype: cons_dpq.DPquery
        """

        assert inverse_scale >= 0., "Trying to take a DP measurement with negative PLB"
        assert isinstance(inverse_scale, Fraction)
        dp_mech = self.mechanism(true_data=hist, query=query, inverse_scale=inverse_scale, bounded_dp=True, mechanism_name=self.mechanism_name)

        return cons_dpq.DPquery(query, dp_mech).zipDPanswer()

    def checkBoundedDPPrivacyImpact(self, nodes_dict: __NodeDict__) -> (float, float):
        """
           Checks the privacy impact of the queries. For each node in a level, it gets the matrix representations m_i of each query i,
           and also the sensitivity s_i and epsilon e_i used. It computes sum_i eps_i/s_i * (1^T * abs(q_i)) * 2, which represents how
           much privacy budget is used in each cell of the resulting histogram. It then takes the max and min of the cell. The max and
           min are aggregated across the nodes to find the overall max and min of any cell in the level. The max is the privacy impact
           of that level and the gap beween max and min shows wasted privacy budget. The maxes and mins are then summed over levels. The
           function returns two quantities, the first float is the max privacy budget used (the epsilon of the algorithm) and the second
           float is the min.
        """
        self.log_and_print("Checking privacy impact")

        # setup pyspark rdd.aggregate functions
        def seq_op(x, y):
            return max(x[0], y[0]), min(x[1], y[1])
        comb_op = seq_op

        high = 0.0
        low = 0.0
        initial = (-np.inf, np.inf)
        for level, nodes in nodes_dict.items():
            level_hilo = nodes.map(lambda node: node.getBoundedDPPrivacyImpact())  # rdd of (max, min)
            (tmp_hi, tmp_lo) = level_hilo.aggregate(initial, seq_op, comb_op)
            high += tmp_hi
            low += tmp_lo
            self.log_and_print(f"Privacy impact of level {level}: (hi={tmp_hi}, low={tmp_lo})")
        self.log_and_print(f"Privacy impact: {high}\n inefficiency gap: {high - low}\n Set budget: {self.budget.total_budget}")
        # TODO: Fine, if budget is exceeded by a tolerance, e.g. 1e-7. If this what we want?
        assert high <= self.budget.total_budget + 2 ** (-CC.BUDGET_DEC_DIGIT_TOLERANCE * 10. / 3.), "Privacy budget accidentally exceeded"

    def checkTotalBudget(self, nodes_dict: __NodeDict__) -> dict:
        """
            Sum all epsilons over nodes in each level, divide by number of nodes on that level, then add the level epsilons together
            Or, better, take 1 node from each level and see its budget
        """
        # Dictionary of epsilons for each level
        by_level_eps = {}
        for level, nodes in nodes_dict.items():
            # From each node, get its epsilon
            level_eps = nodes.map(lambda node: node.getEpsilon())
            # Epsilon of some node
            node_eps = level_eps.take(1)[0]

            # If all epsilons are equal, as the should be, this should be sum of near zeros
            print("Checking budgets spent by each node on each level...")
            zero = level_eps\
                .map(lambda neps, eps_dict=node_eps: {budget_id: abs(neps[budget_id] - e) for budget_id, e in eps_dict.items()})\
                .reduce(lambda a, b: {bid: (a[bid]+b[bid]) for bid in a})
            n_nodes = nodes.count()
            print(f"Geolevel {level}: Sum of epsilon differences (should be 0s) over {n_nodes} nodes: {zero}, relative: { {bid: (z/n_nodes) for bid, z in zero.items()} }")

            # Epsilon spent on this level
            by_level_eps[level] = node_eps

        used_budget = {}
        for level, level_eps_dict in by_level_eps.items():
            for bid, eps in level_eps_dict.items():
                if bid not in used_budget:
                    used_budget[bid] = 0
                used_budget[bid] += eps
        self.log_and_print(f"Used budget: {by_level_eps},\n Total used budget: {used_budget},\n Set budget: {self.budget.total_budget}")
        return used_budget

    def logInvCons(self, nodes: __NodeRDD__) -> None:
        """ Log acting invariants and constraints """
        rec: GeounitNode = nodes.take(1)[0]
        # count = nodes.count()
        # self.log_and_print(f"Number nodes: {count}")
        self.log_and_print(f"{rec.geolevel} invariants: " + ", ".join(map(str, rec.invar.keys())))
        self.log_and_print(f"{rec.geolevel} constraints: " + ", ".join(map(str, rec.cons.keys())))

    def validate_level(self, level, data):
        raise NotImplemented
