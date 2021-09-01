"""
Generic Topdown engine with manual workload setup (in form of queries)
topdown_engine.py

specified in a config file as:
[engine]
engine: programs.engine.topdown_engine.TopdownEngine

the run method kicks off the show:
1 - Prints the history gram
2 - makes nodes for all of the geography levels, puts them all in nodes_dict
3 - runs the topdown() method to actually compute the answer.
    (of course, the answers aren't actually computed there; just the RDDs are wired up.
    the answers are actually computed when Spark is told to write the results.)
    topdown() appears in engine_utils.DASEngineHierarchical
"""

# python imports
from typing import Tuple, Dict
from pyspark.sql import SparkSession
import pyspark.accumulators

# das-created imports
import programs.nodes.manipulate_nodes as manipulate_nodes
from programs.engine.engine_utils import DASEngineHierarchical, __NodeRDD__, __NodeDict__
import programs.dashboard as dashboard
from constants import CC
# import das_utils
from programs.writer.multi_writer import MultiWriter
from programs.writer.mdf2020writer import MDF2020Writer
# from programs.writer.cef_2020.dhch_to_mdf2020_writer import DHCH_MDF2020_Writer
from programs.validator.end2end_validator import E2EValidatorPerson, E2EValidatorUnit

__FeasDict__ = Dict[str, pyspark.accumulators.Accumulator]


class TopdownEngine(DASEngineHierarchical):
    """
    Implements engines that employ by-level noise infusion, followed by topdown optimization.
    Examples are: this engine itself, and subclass HDMMEngine
    """
    # def initializeAndCheckParameters(self):
    #     """
    #     Superclass, then
    #     Set per geolevel budgets and per-query budgets
    #     """
    #     super().initializeAndCheckParameters()
    #
    #     # # Set budgets on each geolevel
    #     # self.setPerGeolevelBudgets()
    #
    #     # Set workload /budgets of each query
    #     self.setWorkload()
    #
    #     # # Because global_scale is set in config file, total budget is now computed as a consequence of global_scale.
    #     # self.computeTotalBudget()
    #
    # def setWorkload(self):
    #     """Set budgets of each query. In the most generic Topdown the workload is set up manually"""
    #     self.setOptimizersAndQueryOrderings()

    def run(self, original_data: __NodeRDD__) -> Tuple[__NodeRDD__, __FeasDict__]:
        """
        This is the main function of the topdown engine.
        The function makeOrLoadNoisy creates or loads noisy measurements
        The function topdown implements the topdown algorithm.

        Called from das_framework/driver.py

        Inputs:
                self: refers to the object of the engine class
                block_nodes: RDDs of nodes objects for each of the lowest level geography

        Output:
                nodes_dict: a dictionary containing RDDs of node objects for each of the geolevels
                feas_dict: a dictionary containing meta information about optimization
        """

        super().run(original_data)

        self.optimizeWorkload()
        # print(f"Blocks in engine max part: {das_utils.maxPartPerKey(original_data, lambda n: n.parentGeocode)}")
        #if self.postprocess_only and self.optimization_start_from_level is not None:
        #    nodes_dict = {}
        #else:
        nodes_dict: __NodeDict__ = self.makeOrLoadNoisy(original_data)

        # TODO: These privacy checks should be invoked automatically, either in super().run() or in super().didRun()
        if self.getboolean(CC.CHECK_BUDGET, default=True):
            self.checkBoundedDPPrivacyImpact(nodes_dict)
            for bid, budget in self.checkTotalBudget(nodes_dict).items():
                if abs(budget - self.budget.total_budget) > 1e-7:
                    self.log_warning_and_print(f"USED BUDGET (id {bid}) UNEQUAL TO SET BUDGET!!!")
        else:
            self.log_and_print("Skipping privacy checks")

        nodes_dict, feas_dict = self.topdown(nodes_dict)

        if self.getboolean(CC.RETURN_ALL_LEVELS, default=False):
            # assert self.spine_type == "non_aian_spine", "Cannot return all levels unless spine type is non_aian_spine."
            return nodes_dict, feas_dict

        block_nodes = nodes_dict[self.levels_reversed[-1]]

        # Redefine the geocodes back to geocode16 format if the target geolevel
        print(f'self.setup.geo_bottomlevel: {self.setup.geo_bottomlevel}, self.setup.levels[0]: {self.setup.levels[0]}')
        if self.spine_type != CC.NON_AIAN_SPINE and ((self.setup.geo_bottomlevel is None) or (self.setup.geo_bottomlevel is '') or (self.setup.geo_bottomlevel == self.setup.levels[0])):
            print('Redefining geocodes back to geocode16')
            block_nodes = block_nodes.map(lambda node: node.redefineGeocodes(self.setup.geocode_dict))
            self.das.reader.modified_geocode_dict = self.setup.geocode_dict
            print(f'Set the reader geocode_dict back to the original geocode_dict. self.das.reader.geocode_dict is now: {self.das.reader.modified_geocode_dict}')

        # Unpersist and delete everything but bottom level
        # self.freeMemLevelDict(nodes_dict, down2level=1)

        return block_nodes, feas_dict


    def optimizeWorkload(self):
        """ Nothing to optimize in basic topdown"""
        pass

    def getNodePLB(self, node):
        """
        Return optimized per-geocode privacy loss budget or per-geolevel user-defined privacy budget, according to the option set
        :param node: GeounitNode
        :return: PLB, Fraction or float (soon to be only Fraction)
        """
        if self.spine_type == CC.OPT_SPINE:
            return self.budget.plb_allocation.value[node.geocode]

        if self.spine_type in (CC.AIAN_SPINE, CC.NON_AIAN_SPINE):
            return self.budget.geolevel_prop_budgets_dict[node.geolevel]

        # Should not be needed, b/c it's checked in setup module, at reading the option, but don't want silent return of None
        raise ValueError(f"spine type must be {'/'.join(CC.SPINE_TYPE_ALLOWED)} rather than {self.spine_type}.")

    def noisyAnswers(self, nodes: __NodeDict__, **kwargs) -> __NodeDict__:
        """
        This function is the second part of the topdown engine.
        This function takes the GeounitNode objects at each geolevel, adds differential
        privacy measurements

        Inputs:
            nodes:  a dictionary containing RDDs of node objects for each of the geolevels.
                        No DP measurement or protected data are present on input

        Output:
            nodes: a dictionary containing RDDs of node objects for each of the geolevels
        """
        nodes_dict: __NodeDict__ = nodes

        # for each geolevel take noisy measurements
        for level in self.levels:
            self.annotate(f"Taking noisy measurements at {level}")
            nodes_dict[level] = nodes_dict[level].map(lambda node: self.makeDPNode(node)).persist()
            self.logInvCons(nodes_dict[level])

        return nodes_dict

    def topdown(self, nodes_dict: __NodeDict__) -> Tuple[__NodeDict__, __FeasDict__]:
        """
        This function is the third part of the topdown engine.
        This function and initiates the topdown algorithm.

        Inputs:
            nodes_dict:  a dictionary containing RDDs of node objects for each of the geolevels.
                         No DP measurement or protected data are present on input

        Output:
            nodes_dict: a dictionary containing RDDs of node objects for each of the geolevels
            feas_dict: a dictionary containing meta information about optimization
        """
        self.annotate("topdown starting")
        spark = SparkSession.builder.getOrCreate() if self.use_spark else None
        config = self.config
        optimizers = self.optimization_query_ordering.optimizers
        seq_opt_name, l2_approach, rounder_approach = optimizers
        self.annotate(f"topdown will use sequential optimizer: {seq_opt_name}")
        self.annotate(f"topdown will use l2 optimization approach: {l2_approach}")
        self.annotate(f"topdown will use rounder optimization approach: {rounder_approach}")

        if self.minimal_schema:
            min_schema_dims = tuple([self.vars_schema[x] for x in self.minimal_schema])
        else:
            min_schema_dims = None

        # Do Topdown Imputation.
        if self.optimization_start_from_level is None:
            # Pool measurements with those from lower level, for each level
            nodes_dict = self.poolLowerLevelMeasurements(nodes_dict)
            # This does imputation from root node to root node level.
            toplevel = self.levels[-1]
            nodes_dict[toplevel] = (
                nodes_dict[toplevel]
                .map(lambda node: manipulate_nodes.geoimp_wrapper_root(config=config, parent_shape=(self.hist_shape, self.unit_hist_shape),
                                                                       root_node=node, optimizers=optimizers, min_schema=min_schema_dims))
                .persist()
                )
            level_pairs = zip(self.levels_reversed[:-1], self.levels_reversed[1:])
        else:
            saved_optimized_app_id = self.getconfig("saved_optimized_app_id")
            nodes_dict[self.optimization_start_from_level] = self.loadNoisyAnswers(saved_optimized_app_id, postfix="Optimized", levels2load=(self.optimization_start_from_level,))[self.optimization_start_from_level]
            if self.optimization_start_from_level == self.levels[0]:
                return nodes_dict, {}
            
            level_ind = self.levels_reversed.index(self.optimization_start_from_level)
            level_pairs = zip(self.levels_reversed[level_ind:-1], self.levels_reversed[level_ind+1:])




        self.das.delegate.log_testpoint('T05-001S')
        ### feasibility dictionary
        feas_dict: __FeasDict__ = {}
        num_geounits = 0
        for level, lower_level in level_pairs:
            # make accumulator
            feas_dict[level] = spark.sparkContext.accumulator(0) if self.use_spark else 0

            #print(f"{lower_level} max part: {das_utils.maxPartPerKey(nodes_dict[lower_level], lambda n: n.parentGeocode)}")

            #nodes_dict[level] = das_utils.partitionByGeocode(nodes_dict[level], num_parts)

            num_parts = nodes_dict[lower_level].getNumPartitions()

            parent_rdd = nodes_dict[level].map(lambda node: (node.geocode, node))

            # Find non-zeros of the upper level
            nz_dict = dict(parent_rdd.mapValues(lambda node: node.syn.sparse_array.indices).collect())
            nz = spark.sparkContext.broadcast(nz_dict) if self.use_spark else nz_dict

            # The .collect() above is an eager action, so that's when the upper level actually gets optimized
            self.annotate(f"Geolevel {level} has been optimized")

            print(f"Feasibility dict: {feas_dict}")

            if self.getboolean(CC.SAVEOPTIMIZED, default=True):
                self.annotate(f"Saving {level} optimized data")
                self.saveNoisyAnswers({level: nodes_dict[level]}, False, postfix="Optimized")

                # These re-load the optimized data for the upper level, which has been just saved
                nodes_dict[level] = self.loadNoisyAnswers(self.app_id, postfix="Optimized", levels2load=(level,))[level]
                self.annotate(f"Reloading {level} optimized data")
                parent_rdd = nodes_dict[level].map(lambda node: (node.geocode, node))

            # Set detailed query noisy measurements (aka noisy children) to 0 where parent is 0, and convert to sparse representation
            child_rdd = nodes_dict[lower_level].map(lambda node: (node.parentGeocode, node.filterToNZ(nz.value[node.parentGeocode] if self.use_spark else nz[node.parentGeocode])))

            aian = self.areAIANSumConstraintsOn(lower_level)

            cogrouped = child_rdd.cogroup(parent_rdd)

            # # Convert to serialized list for debugging within python
            # from programs.rdd_like_list import RDDLikeList
            # cogrouped = RDDLikeList(cogrouped.collect())

            nodes_dict[lower_level] = (
                    cogrouped
                    .map(lambda pc_nodes, lev=level: manipulate_nodes.geoimp_wrapper(config=config, parent_child_node=pc_nodes, accum=feas_dict[lev],
                                                                                     optimizers=optimizers, min_schema=min_schema_dims, aian=aian))
                    .flatMap(lambda children: tuple([child for child in children]), preservesPartitioning=True)
                    .repartition(num_parts))

            # # Convert back to Spark RDD
            # nodes_dict[lower_level] = nodes_dict[lower_level].RDD().repartition(num_parts)

            print(f"Feasibility dict: {feas_dict}")

        if self.getboolean(CC.SAVEOPTIMIZED, default=True):
            level = self.levels[0]
            self.annotate(f"Saving {level} optimized data")
            self.saveNoisyAnswers({level: nodes_dict[level]}, False, postfix="Optimized")

            # nodes_dict.pop(level)
            # rdd_rows = nodes_dict[lower_level].count()
            # num_geounits += rdd_rows
            # self.annotate(f"Geolevel {lower_level} RDD has {rdd_rows} rows")

        ### Save the statistics from all levels as a single RDD
        ### Note that collecting statistics is largely a side-effect operation,
        ### so we need to force all of the nodes
        ### to calculate first. count() is an eager action.
        #for level, node_rdd in nodes_dict.items():
        #    self.log_and_print(f"Level {level} RDD has {node_rdd.count()} rows")

        num_geounits  = 0
        self.validate_levels = [x.lower().strip() for x in self.validate_levels]
        for level in self.levels_reversed:
            rdd_rows = nodes_dict[level].count()
            num_geounits += rdd_rows
            self.annotate(f"Geolevel {level} RDD has {rdd_rows} rows")
            if self.validate_levels and level.lower().strip() in self.validate_levels:
                self.annotate(f"Validating {level}")
                self.validate_level(level=level, data=nodes_dict[level])

        dashboard.das_log(extra={CC.NUM_GEOUNITS: num_geounits})

        # Commented: we do it in run() function. Minimal schema needs the other levels after the first run.
        ### unpersist all but bottom level
        # for level in self.levels_reversed[:-1]:
        #     nodes_dict[level].unpersist()
        #     del nodes_dict[level]
        #     gc.collect()

        self.das.delegate.log_testpoint('T05-002S')

        ### return dictionary of nodes RDDs
        self.annotate("topdown done")
        #self.printAndComputeQueryAccuracies(nodes_dict)
        return nodes_dict, feas_dict

    def areAIANSumConstraintsOn(self, lower_level):
        """
        Whether to turn on additional constraints, setting total state invariant on the AIAN- spines (i.e. aian and opt) without
        setting invariants on AIAN and non-AIAN parts of the state individually.
        Note: if "total" constraints for "State" level are on, that means they are set individually, and the sum constraint (i.e. total of
        the whole state) will be redundant
        :param lower_level:
        :return: True if special constraints option is ON, the spine is an AIAN spine and the level is "State"
        """
        if not self.getboolean("aian_sum_constraints", default=True):
            return False
        if self.spine_type == CC.NON_AIAN_SPINE:
            return False
        if lower_level != 'State':  # NOTE: this is hard-coded for a good reason: the special constraints are only needed because policy requires State invariant,
            return False            # AND the AIAN/non-AIAN split happens at the State level. Both conditions are not going to migrate to
                                    # another level simultaneously, so this quirk is reflected in the code also as a quirk.
        return True

    def poolLowerLevelMeasurements(self, nodes_dict: __NodeDict__) -> __NodeDict__:
        """
        For each level, combine  measurements with those from lower level
        :return:
        """

        if self.getboolean(CC.POOL_MEASUREMENTS, default=False):
            for level, upper_level in zip(self.levels[:-1], self.levels[1:]):
                self.log_and_print(f"Pooling measurements from {level} with {upper_level}...")
                from_lower_rdd = (nodes_dict[level]
                                  .map(lambda node: (node.parentGeocode, node))
                                  .reduceByKey(lambda x, y: x.addInReduce(y, add_dpqueries=True))
                                 )


                nodes_dict[upper_level] = (nodes_dict[upper_level]
                                           .map(lambda node: (node.geocode, node))
                                           .cogroup(from_lower_rdd)
                                           .map(manipulate_nodes.findParentChildNodes)
                                           .map(lambda d: (lambda from_this, from_lower:
                                               from_this.mixMeasurements(from_lower[0]))(*d))
                                          )

                # This is a more transparent but a tiny bit slower way to join
                # .join(from_lower_rdd)
                # .map(lambda d: (lambda geocode, nodes: nodes[0].mixMeasurements(nodes[1]))(*d))

        return nodes_dict

    # @staticmethod
    # def combineParentAndChildren(level_rdd, lower_level_rdd):
    #     """
    #     Given RDD of upper geolevel nodes and rdd of lower geolevel nodes, returns RDD of tuples,
    #     where each tuple contains a parent and all its child nodes
    #     :param level_rdd: RDD with all upper level nodes (say, a state)
    #     :param lower_level_rdd: RDD with all lower level nodes (then, counties)
    #     :return: RDD with tuples of nodes, parent-and-children within a tuple
    #     """
    #     parent_rdd = level_rdd.map(lambda node: (node.geocode, node))
    #     # child_rdd = lower_level_rdd.map(lambda node: (node.parentGeocode, node))
    #     spark = SparkSession.builder.getOrCreate()
    #     nz = spark.sparkContext.broadcast(dict(parent_rdd.mapValues(lambda node: node.syn.sparse_array.indices).collect()))
    #
    #     child_rdd = lower_level_rdd.map(lambda node: (node.parentGeocode, node.filterToNZ(nz.value[node.parentGeocode])))
    #
    #     # Timing of this operation has been measured. It is not negligible, but is dwarfed by Gurobi optimization in
    #     # the next step, as you go to bigger problems (a single big state is enough to see that). Usually time for
    #     # union operation is roughly the same as for groupByKey operation
    #
    #     # parent_child_rdd = parent_rdd.union(child_rdd).groupByKey()
    #     # parent_child_rdd = parent_rdd.join(child_rdd)
    #
    #     # parent_child_rdd = child_rdd.groupByKey().join(parent_rdd)
    #
    #     # Cogroup seems to be slightly faster than union().groupByKey().
    #     #parent_child_rdd = parent_rdd.cogroup(child_rdd)
    #     parent_child_rdd = child_rdd.cogroup(parent_rdd)
    #
    #
    #     return parent_child_rdd

    def deleteTrueData(self, nodes: __NodeDict__) -> __NodeDict__:
        nodes_dict = nodes
        for level in self.levels:
            nodes_dict[level] = super().deleteTrueData(nodes_dict[level])
        return nodes_dict

    def freeMemNodes(self, nodes: __NodeDict__) -> None:
        self.freeMemLevelDict(nodes)

    def validate_data(self, level, data, writer, validator):
        mdf_rdd = writer.transformRDDForSaving(data)
        if isinstance(validator, E2EValidatorPerson):
            variable_dict = {"dataframe": mdf_rdd}
        elif isinstance(validator, E2EValidatorUnit):
            variable_dict = {"dataframe": mdf_rdd, "join_method": 'inner'}
        else:
            self.log_warning_and_print(f"Validation skipped, validator is {validator.__class__}")
            return

        if validator.validate(**variable_dict):
            self.annotate(f"Passed validation {level}")
        else:
            raise RuntimeError(f"Failed validation {level}")

    def validate_level(self, level, data):
        LEVEL_VALIDATOR = 'validator'
        validator_class_name, validator_module = self.das.load_module(self.config, LEVEL_VALIDATOR, LEVEL_VALIDATOR,
                                                                      'driver', 'AbstractDASValidator')
        validator = getattr(validator_module, validator_class_name)(config=self.config, setup=self.das.setup_data,
                                                                    name=LEVEL_VALIDATOR, das=self.das)
        # validator = self.das.validator
        if not isinstance(self.das.writer, MultiWriter):
            writers = [ self.das.writer]
        else:
            writers = self.das.writer.writers

        for writer in writers:
            if isinstance(writer, MDF2020Writer):
                self.validate_data(level=level, data=data, writer=writer, validator=validator)
