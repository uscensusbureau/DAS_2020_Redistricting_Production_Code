"""
This class implements obtaining (via config parsing or otherwise) the values for Privacy Loss Budget (PLB)
allocations over queries and geolevels and storing them, along with relevant parameters, such as privacy framework (pure DP, zCPD),
global noise scale, delta etc.
"""

from fractions import Fraction
from typing import Tuple, List, Dict, Iterable, Callable, Union
from configparser import NoOptionError, NoSectionError
from collections import defaultdict
from operator import add
from functools import reduce
import numpy as np
import pandas as pd
import os


from programs.engine.curve import zCDPEpsDeltaCurve
from programs.engine.discrete_gaussian_utility import limit_denominator as dg_limit_denominator
from programs.das_setup import DASDecennialSetup
import programs.queries.querybase as querybase
from programs.schema.schema import sortMarginalNames
from programs.strategies.strategies import StrategySelector
import programs.strategies.print_alloc as print_alloc

from das_utils import checkDyadic

from exceptions import DASConfigError, DASConfigValdationError

from das_framework.driver import AbstractDASModule

from constants import CC

DENOM_MAX_POWER = np.ceil(np.log2(CC.PRIMITIVE_FRACTION_DENOM_LIMIT) / 2)


class Budget(AbstractDASModule):

    def __init__(self, levels, setup: DASDecennialSetup, **kwargs):
        super().__init__(name=CC.BUDGET, **kwargs)

        self.levels = levels
        self.levels_reversed = tuple(reversed(self.levels))
        self.privacy_framework = setup.privacy_framework
        self.only_dyadic_rationals = setup.only_dyadic_rationals

        self.global_scale = self.getfraction(CC.GLOBAL_SCALE)   # DP noise scale. just 1/epsilon, for pure DP methods, For eps, delta-DP more complicated

        self.delta: Fraction = self.getfraction(CC.APPROX_DP_DELTA, default=Fraction(1, int(1e10)))  # Delta for (eps, delta)- like mechanisms
        assert 0. < self.delta <= 1., "Approximate DP delta is outside of (0,1]!"

        # Fractions of how the total <engine> privacy budget is split between geolevels (for pure DP; more complicated allocation for zCDP)
        self.geolevel_prop_budgets: tuple = setup.geolevel_prop_budgets                                      # Shares of budget designated to each geolevel
        assert len(self.levels_reversed) == len(self.geolevel_prop_budgets), f"Length of geolevels ({self.levels_reversed} unequal to length of proportions vector ({self.geolevel_prop_budgets}))"
        self.geolevel_prop_budgets_dict: dict = dict(zip(self.levels_reversed, self.geolevel_prop_budgets))
        self.checkAndPrintGeolevelBudgets()

        # Optimized allocations -- sometimes a geounit can get more budget
        # Spark broadcast dict with geocodes as keys and PLB to each geonode as values
        self.plb_allocation = setup.plb_allocation

        self.schema_obj = setup.schema_obj
        self.unit_schema_obj = setup.unit_schema_obj

        self.query_budget = self.QueryBudget(self)

        self.total_budget = self.computeTotal()

        if self.getboolean(CC.PRINT_PER_ATTR_EPSILONS, default=False):
            self.per_attr_epsilons, self.per_geolevel_epsilons = self.computeAndPrintPerAttributeEpsilon()

        self.saveAllocString2Setup(setup)

        self.saveFullAllocationSemanticsCSV()

    def saveAllocString2Setup(self, setup):
        """Add the allocations string to setup object so that it's accessible by writer or other modules"""
        setup.qalloc_string = f"Global rho: {1. / self.global_scale ** 2}\n" \
                                  f"Global epsilon: {self.total_budget}\n" \
                                  f"delta: {self.delta}\n" \
                                  "Geolevel allocations:\n" + \
                              str([f"{k}: {str(v)}" for k, v in self.geolevel_prop_budgets_dict.items()]) + \
                              "\nWithin-geolevel query allocations:\n" + str(self.query_budget.allocation_df.to_csv())

    def saveFullAllocationSemanticsCSV(self):
        """ Save CSV file (which will be included into .zip uploaded to S3 with total budget, all allocations and per-attribute semantics"""

        def renameQuery(qname):
            print_name = qname if qname != 'detailed' else CC.SCHEMA_CROSS_JOIN_DELIM.join(self.schema_obj.dimnames)
            return print_name + f" (cells: {self.schema_obj.getQuery(qname).numAnswers()})"

        logfilename = os.getenv('LOGFILE_NAME')
        if logfilename is not None:
            budget_names = {CC.PURE_DP: "epsilon", CC.ZCDP: "rho"}
            # Save the CSV
            with open(logfilename.replace(".log", f"_plballoc-fulltable.csv"), "w") as f:
                if self.privacy_framework == CC.ZCDP:
                    rho = 1. / self.global_scale ** 2
                    f.write(f"Global rho,{rho} ({float(rho):.2f})\n")
                f.write(f"Global epsilon,{self.total_budget} ({float(self.total_budget):.2f})\n")
                if self.privacy_framework == CC.ZCDP:
                    f.write(f"delta,{self.delta} ({float(self.delta):.2e})\n\n")
                gldf = pd.DataFrame(self.geolevel_prop_budgets_dict.items())
                gldf.columns = ["", f"{budget_names[self.privacy_framework]} Allocation by Geographic Level"]
                f.write(f"{str(gldf.to_csv(index=False))}\n\n")
                f.write(f"Per Query {budget_names[self.privacy_framework]} Allocation by Geographic Level\n")
                df1 = self.query_budget.allocation_df.copy(deep=True)
                df1['Query'] = list(df1.reset_index()['index'].apply(renameQuery))
                f.write(str(df1.set_index('Query').to_csv()))
                f.write("\n\n")
                f.write(f"Per Query {budget_names[self.privacy_framework]} Allocation of Global {budget_names[self.privacy_framework]}\n")
                f.write(print_alloc.printPercent(print_alloc.multiplyByGLBudgets(df1.set_index('Query'), self.geolevel_prop_budgets_dict.items()), out='csv'))
                f.write("\n\n Per attribute semantics:")
                f.write("\n\n Attribute,epsilon\n")
                for attr, eps in self.per_attr_epsilons.items():
                    f.write(f"{attr},{float(eps):.2f}\n")
                f.write("\n\n Per geography semantics:")
                f.write("\n\n Geographic level,epsilon\n")
                for gl, eps in self.per_geolevel_epsilons.items():
                    f.write(f"Block-within-{gl},{float(eps):.2f}\n")


            # Save a colored table with percentage allocation
            df2 = self.query_budget.allocation_df.copy(deep=True)
            df2 = print_alloc.multiplyByGLBudgets(df2, self.geolevel_prop_budgets_dict.items()).astype(float)
            print_alloc.makeHeatTable(df2, logfilename.replace(".log", f"_plballoc.pdf"))


    def epsilonzCDPCalculator(self, verbose=True):
        """A closure returning function that gets epsilon from a zCDP curve"""
        return lambda geo_allocations_dict: Fraction(zCDPEpsDeltaCurve(geo_allocations_dict, verbose=verbose).get_epsilon(float(self.delta), self.global_scale, bounded=True, tol=1e-7, zcdp=True))

    def epsilonPureDPCalculator(self, verbose=True):
        """A closure returning function that calculates total PLB by summing all the proportions"""
        return lambda geo_allocations_dict: self.total_budget * sum(gprop * sum(qprops)  for gprop, qprops in geo_allocations_dict.values())

    def computeTotal(self):
        """
            Computes global epsilon in use, based on global_scale, delta (if applicable), & query, geolevel proportions.
        """
        dp_query_prop = self.query_budget.dp_query_prop
        self.log_and_print(f"Computing total budget using privacy (accounting) framework {self.privacy_framework}")
        if self.privacy_framework == CC.ZCDP:
            print(f"Sending geolevel_prop_budgets to Curve: {TupleOfFractions(self.geolevel_prop_budgets)}")
            qprop_string = "\n".join((f"{k}:\t\t{TupleOfFractions(v)}" for k, v in dp_query_prop.items()))
            print(f"Sending dp_query_prop to Curve:\n{qprop_string}")
            geo_allocations_dict = {}
            for geolevel, gprop in self.geolevel_prop_budgets_dict.items():
                geo_allocations_dict[geolevel] = gprop, dp_query_prop[geolevel]
                # TODO: add unit_dp_query_props accordingly
            total_budget = self.epsilonzCDPCalculator(verbose=False)(geo_allocations_dict)
            total_budget_n, total_budget_d = dg_limit_denominator((total_budget.numerator, total_budget.denominator),
                                                                  max_denominator=CC.PRIMITIVE_FRACTION_DENOM_LIMIT,
                                                                  mode="upper")
            total_budget = Fraction(total_budget_n, total_budget_d)
            for geolevel, prop in self.geolevel_prop_budgets_dict.items():
                geolevel_noise_precision = 2 * prop / (self.global_scale ** 2)
                self.log_and_print(f"Noise 'precision' for {geolevel}: {geolevel_noise_precision}")

            rho = 1 / self.global_scale ** 2
            self.log_and_print(f"Global rho: {rho} ({float(rho):.2f})")

            self.log_and_print(f"Delta: {self.delta}")
        elif self.privacy_framework == CC.PURE_DP:
            total_budget = 1 / self.global_scale
        else:
            raise NotImplementedError(f"DP primitives/composition rules for {self.privacy_framework} not implemented.")
        self.log_and_print(f"Denominator limit: {CC.PRIMITIVE_FRACTION_DENOM_LIMIT}")
        self.log_and_print(f"Total budget: {total_budget} ({float(total_budget):.2f})")
        self.log_and_print(f"Global scale: {self.global_scale}")

        return total_budget

    def checkAndPrintGeolevelBudgets(self):
        """
        For engines infusing noise at each geolevel (e.g. topdown, hdmm*)
        Check that the by-geolevel privacy budget distribution sums to 1, and print allocations
        """

        budget_names = {CC.PURE_DP: "epsilon", CC.ZCDP: "rho"}
        if self.privacy_framework in (CC.PURE_DP, CC.ZCDP):
            budget_msg = f"{self.privacy_framework} {budget_names[self.privacy_framework]} is split between geolevels"
            budget_msg += f" with proportions: {TupleOfFractions(self.geolevel_prop_budgets)}"
            self.log_and_print(budget_msg)
        else:
            raise NotImplementedError(f"Formal privacy primitives/composition rules for {self.privacy_framework} not implemented.")

        # check that geolevel_budget_prop adds to 1, if not raise exception
        assertSumTo(self.geolevel_prop_budgets, msg="Across-geolevels Budget Proportion")
        assertEachPositive(self.geolevel_prop_budgets, "across-geolevel")

    def computeAndPrintPerAttributeEpsilon(self):
        """
            Ignoring zero-error geolevels, computes and prints per-histogram-attribute (as well as geography) epsilon
            expended. Uses query kronFactors to determine histogram attributes that are relevant; only implemented for
            SumOverGroupedQuery from querybase. In the case of zCDP, an implied per-attribute epsilon, delta-DP is reported.
        """
        msg = f"Computing per-attribute epsilon for each of "
        msg += f"{self.schema_obj.dimnames}, and for Block-in-Geolevel for each Geolevel in "
        msg += f"{list(self.geolevel_prop_budgets_dict.keys())[:-1]}"
        msg += f"\n(NOTE: geolevels with proportion of budget 0 assigned to them are ignored)"
        self.log_and_print(msg)

        # TODO: add support for Bottomup? No geolevel calculations, then; attr calculations the same
        #       before then, throw an exception if Bottomup used?

        # TODO: This is only for dp_queries unit_qp_queries to be integrated in this loop.
        #  Those will use self.unit_schema_obj.dimnames and self.query_budget.unitQueryPropPairs()
        attr_query_props = self.getAttrQueryProps(self.levels, self.schema_obj.dimnames, lambda gl: self.query_budget.queryPropPairs(gl))

        for attr, gl_q_dict in attr_query_props.items():
            for geolevel, q_dict in gl_q_dict.items():
                self.log_and_print(f"Found queries for dim {attr} in {geolevel}:")
                max_qname_len = max(map(len, q_dict))
                for qname, prop in q_dict.items():
                    qstr = qname + ':' + ' ' * (max_qname_len - len(qname))
                    self.log_and_print(f"\t\t\t\t\t{qstr}  {prop}")

        if self.privacy_framework == CC.ZCDP:
            eps_type_printout = " zCDP-implied"
            eps_getter = self.epsilonzCDPCalculator(verbose=False)
            msg_end = f" in (eps, {self.delta})-DP)\n"
        elif self.privacy_framework == CC.PURE_DP:
            eps_type_printout = "pure-DP"
            eps_getter = self.epsilonPureDPCalculator(verbose=False)
            msg_end = "\n"
        else:
            raise NotImplementedError(f"DP primitives/composition rules for {self.privacy_framework} not implemented.")

        per_attr_epsilons, per_geolevel_epsilons = self.getPerAttrEpsilonFromProportions(attr_query_props, eps_getter, self.levels, self.geolevel_prop_budgets_dict, self.query_budget.dp_query_prop)

        msg = []
        for attr, eps in per_attr_epsilons.items():
            msg.append(f"For single attr/dim {attr} semantics, {eps_type_printout} epsilon: {eps} (approx {float(eps):.2f})")
        for level, eps in per_geolevel_epsilons.items():
            msg.append(f"For geolevel semantics protecting {self.levels[0]} within {level}, {eps_type_printout} epsilon: {eps} (approx {float(eps):.2f})")
        self.log_and_print(",\n".join(msg) + msg_end)

        return per_attr_epsilons, per_geolevel_epsilons

    @staticmethod
    def getAttrQueryProps(levels, dimnames, query_iter) -> Dict[str, Dict[str, Dict[str, Fraction]]]:
        """ Packs proportions of the queries that use an attribute into by-attribute-by-geolevel-by-query nested dicts"""
        # Note: This nested dict is used to print it's contents, otherwise there is no need for it, and the accounting
        # can be done in the same loop that makes this nested dict (essentially take this loop and move it into
        # self.getPerAttrEpsilonFromProportions replacing the nested loops over the dict)
        attr_query_props = defaultdict(lambda: defaultdict(dict))
        for i, dimname in enumerate(dimnames):
            for geolevel in levels:
                for query, qprop in query_iter(geolevel):
                    assert isinstance(query, querybase.SumOverGroupedQuery), f"query {query.name} is of unsupported type {type(query)}"
                    q_kron_facs = query.kronFactors()
                    if q_kron_facs[i].shape[1] >= 2:  # Need at least two kron_factors for a record change in this dim to affect query
                        if (q_kron_facs[i].sum(axis=1) > 0).sum() >= 2:  # At least two kron_facs require at least 1 True for sens>0
                            # TODO: this assumes mutually exclusive kron_fracs; keep SumOverGroupedQuery assert until this is lifted
                            attr_query_props[dimname][geolevel][query.name] = qprop
        return attr_query_props

    @staticmethod
    def getPerAttrEpsilonFromProportions(attr_query_props, eps_getter: Callable, levels: List[str], geolevel_prop_budgets_dict: dict, dp_query_prop):
        """
        Takes the nested dict with query proportions by attribute and geolevel and composes those into a total PLB for that attribute.
        Then does similar accounting for the geographic attribute (bottom level / Block)
        """
        per_attr_epsilons = {}
        per_geolevel_epsilons = {}

        for attr, gl_q_props_dict in attr_query_props.items():
            # gl_q_props_dict is dict with {key=geolevel, value={dict with key=query_name, value=proportion}}
            # convert it to a dict with key=geolevel, value = (geoprop, list of qprops)
            geo_allocations_dict = {}
            for geolevel, q_dict in gl_q_props_dict.items():
                if geolevel not in geo_allocations_dict:
                    geo_allocations_dict[geolevel] = geolevel_prop_budgets_dict[geolevel], []
                for prop in q_dict.values():
                    geo_allocations_dict[geolevel][1].append(prop)
            per_attr_epsilons[attr] = eps_getter(geo_allocations_dict)

        geo_allocations_dict = {}
        for geolevel, upper_level in zip(levels[:-1], levels[1:]):  # Start from bottom level, end at second from top
            # Accounting is labeled as "Block-within-Some_higher_level" budget where budget expended on Block up to (excluding) that level is composed
            # hence the need to shift level labels by one
            # TODO: unit_dp_queries should be added
            geo_allocations_dict[geolevel] = geolevel_prop_budgets_dict[geolevel], dp_query_prop[geolevel]
            per_geolevel_epsilons[upper_level] = eps_getter(geo_allocations_dict)

        return per_attr_epsilons, per_geolevel_epsilons

    def checkDyadic(self, *args, **kwargs):
        """ Wrapper that adds denom_max_power"""
        if self.only_dyadic_rationals:
            checkDyadic(*args, **kwargs, denom_max_power=DENOM_MAX_POWER)

    class QueryBudget:
        """
        For engines with queries set in config (e.g. topdown, bottomup)
        Read the queries from config, and set their budget allocations. Check that allocation proportions sum to one
        """

        dp_query_prop: Dict[str, Union[Tuple[Fraction], List[Fraction]]]               # Per geolevel, shares of within-geolevel budgets dedicated to each query
        dp_query_names: Dict[str, Union[Tuple[str], List[str]]]                   # Queries by name, per geolevel
        unit_dp_query_names: Dict[str, Union[Tuple[str], List[str]]]              # Queries for unit histogram by name, per geolevel
        unit_dp_query_prop: Dict[str, Union[Tuple[Fraction], List[Fraction]]]          # Per geolevel, shares of within-geolevel budgets dedicated to each query
        queries_dict: Dict[str, querybase.AbstractLinearQuery]                         # Dictionary with actual query objects

        def __init__(self, budget, **kwargs):
            super().__init__(**kwargs)

            try:
                strategy = StrategySelector.strategies[budget.getconfig(CC.STRATEGY)].make(budget.levels)
            except (NoOptionError, NoSectionError):
                raise DASConfigError("DPQuery strategy has to be set", section=CC.BUDGET, option="strategy")

            self.dp_query_names = strategy[CC.DPQUERIES]
            self.dp_query_prop = strategy[CC.QUERIESPROP]
            self.unit_dp_query_names = strategy[CC.UNITDPQUERIES]
            self.unit_dp_query_prop = strategy[CC.UNITQUERIESPROP]

            # FILL QUERY DICT
            self.queries_dict = {}
            for geolevel in budget.geolevel_prop_budgets_dict:
                self.queries_dict.update(budget.schema_obj.getQueries(self.dp_query_names[geolevel]))
                self.queries_dict.update(budget.unit_schema_obj.getQueries(self.unit_dp_query_names[geolevel]))

            ## CHECKING

            assert len(self.dp_query_names) == len(budget.levels)
            assert len(self.dp_query_prop) == len(budget.levels)
            assert len(self.unit_dp_query_names) in (0, len(budget.levels))
            assert len(self.unit_dp_query_prop) in (0, len(budget.levels))

            max_qname_len = max(map(len, self.queries_dict))

            qallocstr_gprop = ""
            for geolevel, gprop in budget.geolevel_prop_budgets_dict.items():

                # Make a list to check later if it sums up to 1.
                budget_per_each_query: list = []

                budget_per_each_query.extend(list(self.dp_query_prop[geolevel]))

                self.checkUnique(self.dp_query_names[geolevel], CC.DPQUERIES)
                self.checkUnique(self.unit_dp_query_names[geolevel], CC.UNITDPQUERIES)

                budget.checkDyadic(self.dp_query_prop[geolevel], msg="queries")

                qallocstr = f"{geolevel}:\n\t" + "\n\t".join([f"{query.name + ':' + ' ' * (max_qname_len - len(query.name))}  {qprop}" for query, qprop in self.queryPropPairs(geolevel)])
                qallocstr_gprop += f"{geolevel}:\n\t" + "\n\t".join([f"{query.name + ':' + ' ' * (max_qname_len - len(query.name))}  {qprop * gprop}" for query, qprop in
                     self.queryPropPairs(geolevel)])
                if self.unit_dp_query_names[geolevel]:
                    # Add the fractions of per-geolevel budgets dedicated to each query to the list that should sum up to 1.
                    budget_per_each_query.extend(list(self.unit_dp_query_prop[geolevel]))
                    budget.checkDyadic(self.unit_dp_query_prop[geolevel], msg="unit queries")
                    qallocstr += "\n\t".join([f"{query.name + ':' + ' ' * (max_qname_len - len(query.name))}  {qprop}" for query, qprop in self.unitQueryPropPairs(geolevel)])
                    qallocstr_gprop += "\n\t".join([f"{query.name + ':' + ' ' * (max_qname_len - len(query.name))}  {qprop * gprop}" for query, qprop in self.unitQueryPropPairs(geolevel)])

                qallocstr_gprop += "\n"
                assertSumTo(budget_per_each_query, msg="Within-geolevel Budget Proportion")
                assertEachPositive(budget_per_each_query, "queries")

                budget.log_and_print("Within-geolevel query allocations:")
                budget.log_and_print(qallocstr)

            logfilename = os.getenv('LOGFILE_NAME')
            df = print_alloc.makeDataFrame(budget.getconfig(CC.STRATEGY), budget.levels)
            self.allocation_df = df  # Save it for printing out of the budget object
            self.printAllocTables(df, budget)
            self.saveQueryAllocations(df, "_wglev_query_allocations", logfilename)

            dftot = print_alloc.multiplyByGLBudgets(df.copy(deep=True), budget.geolevel_prop_budgets_dict.items())
            budget.log_and_print("All query allocations (i.e. multiplied by geolevel proportion):")
            budget.log_and_print(qallocstr_gprop)
            self.printAllocTables(dftot, budget)
            self.saveQueryAllocations(dftot, "_overall_query_allocations", logfilename)

            # Print all levels, on which the measurements are taken:
            self.printLevelsOfMarginals(budget, set(reduce(add, self.dp_query_names.values())), budget.schema_obj, 'main histogram')
            unique_unit_dp_query_names = [udpqn for udpqn in self.unit_dp_query_names.values() if udpqn]
            if unique_unit_dp_query_names:
                self.printLevelsOfMarginals(budget, set(reduce(add, self.unit_dp_query_names.values())), budget.unit_schema_obj, 'unit histogram')

            self.checkQueryImpactGaps(budget, self.queries_dict)

        @staticmethod
        def saveQueryAllocations(df, fname_append, logfilename):
            """ Saves query allocations into CSV and TEX files which will be included in ZIP uploaded to S3"""
            if logfilename is not None:
                with open(logfilename.replace(".log", f"{fname_append}.csv"), "w") as f:
                    f.write("\n" + str(df.to_csv()) + "\n")
                    f.write("\n" + str(df.astype(float).to_csv()) + "\n")
                    f.write("\n" + print_alloc.printFloat(df, out='csv') + "\n")
                with open(logfilename.replace(".log", f"{fname_append}.tex"), "w") as f:
                    f.write("\n" + str(df.to_latex()) + "\n")
                    f.write("\n" + print_alloc.printFloat(df, out='latex') + "\n")
                    f.write("\n" + print_alloc.printPercent(df, out='latex') + "\n")

        def printAllocTables(self, df, budget):
            """
            Prints query allocations conainted in data frame df
            :param df: Pandas dataframe with query allocations
            :param budget: DAS module that can do log_and_print
            :return:
            """
            budget.log_and_print("As a table:")
            budget.log_and_print("\n" + str(df) + "\n")
            budget.log_and_print("As a CSV:")
            budget.log_and_print("\n" + str(df.to_csv()) + "\n")
            budget.log_and_print("As LaTeX:")
            budget.log_and_print("\n" + str(df.to_latex()) + "\n")
            budget.log_and_print("As a table (floats):")
            budget.log_and_print("\n" + print_alloc.printFloat(df) + "\n")
            budget.log_and_print("As a CSV (floats):")
            budget.log_and_print("\n" + print_alloc.printFloat(df, out='csv') + "\n")
            budget.log_and_print("As LaTeX (floats):")
            budget.log_and_print("\n" + print_alloc.printFloat(df, out='latex') + "\n")
            budget.log_and_print("As a table (percent):")
            budget.log_and_print("\n" + print_alloc.printPercent(df) + "\n")
            budget.log_and_print("As LaTeX (percent):")
            budget.log_and_print("\n" + print_alloc.printPercent(df, out='latex') + "\n")

        def queryPropPairs(self, geolevel):
            """ Generator of query and it's proportion tuples within geolevel"""
            assert len(self.dp_query_names[geolevel]) == len(self.dp_query_prop[geolevel]), f"Lengths of DPquery and their PLB vectors not equal, geolevel {geolevel}"
            for qname, qprop in zip(self.dp_query_names[geolevel], self.dp_query_prop[geolevel]):  # Change to self.query_budget.dp_query_names[geolevel] when we allow different queries in geolevels
                query = self.queries_dict[qname]
                yield query, qprop

        def unitQueryPropPairs(self, geolevel):
            """ Generator of query and it's proportion tuples within geolevel"""
            if self.unit_dp_query_names[geolevel]:
                assert len(self.unit_dp_query_names[geolevel]) == len(self.unit_dp_query_prop[geolevel]), f"Lengths of Unit DPquery and their PLB vectors not equal, geolevel {geolevel}"
                for qname, qprop in zip(self.unit_dp_query_names[geolevel], self.unit_dp_query_prop[geolevel]):  # Change to self.query_budget.dp_query_names[geolevel] when we allow different queries in geolevels
                    query = self.queries_dict[qname]
                    yield query, qprop

        @staticmethod
        def checkUnique(querynames, option_name):
            sorted_marginals_names = sortMarginalNames(querynames)
            if len(sorted_marginals_names) > len(set(sorted_marginals_names)):
                raise DASConfigValdationError(f"Some of the queries {sorted_marginals_names} are slated to be measured more than once",
                                              section=CC.BUDGET, options=(option_name,))

        @staticmethod
        def checkQueryImpactGaps(das_module, queries_dict: Dict[str, querybase.AbstractLinearQuery]):
            """Calculates impact of query on each cell of the histogram. Raises errors if there are impact gaps."""
            das_module.log_and_print(f"###\nImpact of DP queries ([{CC.BUDGET}]/strategy) to be measured:")
            # total_impact = 0
            # for qname, prop in zip(das_module.dp_query_names, das_module.dp_query_prop):  # WARNING: names and prop vectors should be passed as arguments if total is used
            #     query = das_module.queries_dict[qname]
            for qname, query in queries_dict.items():
                # This is just the sum
                # impact = (np.ones(query.numAnswers()) @ np.abs(query.matrixRep()))  # factor of eps/sens doesn't matter here
                impact = np.abs(query.matrixRep()).sum(axis=0)
                # total_impact += impact * prop  # to do this, need to do composition, multiplying by proportion, like here,  only works for pure, epsilon-DP
                impmin, impmax = impact.min(), impact.max()
                das_module.log_and_print(f"{qname} ~ Impact\n {'':50} Min: {impmin}, Max: {impmax}, All: {impact}", cui=False)

                if abs(impmin - impmax) > 1e-7:
                    das_module.log_and_print(query, cui=False)
                    raise DASConfigValdationError(f"There is an impact gap underutilizing parallel composition in query {qname}", section=CC.BUDGET,
                                                  options=("strategy",))

                # Having both below is redundant, but for clarity and future flexibility including both
                if impmin != 1:
                    das_module.log_and_print(query, cui=False)
                    raise DASConfigValdationError(f"Some histogram cells are under-measured in query {qname}", section=CC.BUDGET,
                                                  options=("strategy",))
                if impmax != 1:
                    das_module.log_and_print(query, cui=False)
                    raise DASConfigValdationError(f"Some histogram cells are measured more than once in query {qname}", section=CC.BUDGET,
                                                  options=("strategy",))

            # das_module.log_and_print(f"TOTAL ~ Impact\n {'':50} Min: {total_impact.min()}, Max: {total_impact.max()}, All: {total_impact}", cui=False)
            # if abs(total_impact.min() != total_impact.max()) > 1e-7:
            #     raise DASConfigValdationError(f"There is an impact gap underutilizing parallel composition in DP queries", section=CC.BUDGET,
            #                                   options=(config_option,))

        @staticmethod
        def printLevelsOfMarginals(das_module, queries, schema, qset_name):
            """Print levels of every marginal of the queries"""
            dpq_marginals = set()
            for qname in queries:
                dpq_marginals = dpq_marginals.union(qname.split(CC.SCHEMA_CROSS_JOIN_DELIM))
            das_module.log_and_print(f"###\nLevels of the marginals of {qset_name} DP queries to be measured:")
            for qname in dpq_marginals:
                if qname != 'detailed':
                    das_module.log_and_print(f"{qname} levels:\n------------------------\n" +
                                       "\n".join(schema.getQueryLevel(qname)) +
                                       "\n---------------------------------", cui=False)


def assertSumTo(values: Iterable, sumto=1., dec_place=CC.BUDGET_DEC_DIGIT_TOLERANCE, msg="The ") -> None:
    """
    Assert that sum of the values in the iterable is equal to the desired value with set tolerance
    :param values: iterable, sum of which is to be checked
    :param sumto: float, what it should sum to, default=1.
    :param dec_place: int, tolerance of the sum check, defined by decimal place (approximately, calculated by powers of 2)
    :param msg: Custom error message prefix
    :return:
    """
    error_msg = f"{msg} values {values} sum to {sum(values)} instead of {sumto}"
    assert(abs(sum(values) - sumto)) < 2 ** (-dec_place * 10. / 3.), error_msg    # f-string won't evaluate properly if is in assert


def assertEachPositive(values: Iterable, msg=""):
    """ Assert that each element of values iterable is positive"""
    error_msg = f"Negative proportion factor present in {msg} budget allocation: {values}"
    assert np.all(np.array(values) >= 0), error_msg  # f-string won't evaluate properly if is in assert


class TupleOfFractions(tuple):
    def __new__(cls, t):
        ft = super().__new__(cls, t)

        from math import gcd
        lcm = t[0].denominator
        for f in t[1:]:
            d = f.denominator
            lcm = lcm * d // gcd(lcm, d)
        ft.lcm = lcm
        return ft

    def __repr__(self):
        return ", ".join(f"{self.lcm // f.denominator * f.numerator}/{self.lcm}" for f in self)
