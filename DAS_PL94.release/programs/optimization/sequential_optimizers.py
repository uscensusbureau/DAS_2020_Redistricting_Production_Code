"""This file implements the following classes:

L2PlusRounderWithBackup
  A sequentialOptimizer that runs the GeoRound
  optimizer and, if it fails, invokes the failsafe mechanism.

  This optimizer is called from manipulate_nodes:geoimp_wrapper()

"""

import json
import time
from abc import ABCMeta
from typing import Tuple, Union

import numpy as np
from functools import reduce
from operator import add

# das-created imports
from programs.optimization.optimizer import AbstractOptimizer
from programs.optimization.geo_optimizers import L2GeoOpt
from programs.optimization.simple_rounder import GeoRound
from programs.optimization.multipass_rounder import DataIndependentNpassRound
from programs.optimization.multipass_query_rounder import DataIndependentNpassQueryRound
from programs.optimization.l2_dataIndep_npass_optimizer import DataIndQueriesL2NPass
from programs.optimization.l2_dataIndep_npass_optimizer_improv import DataIndQueriesL2NPassImprov
from programs.optimization.maps import toGRBfromStrStatus

from programs.queries.constraints_dpqueries import Constraint, StackedConstraint
from programs.queries.querybase import MultiHistQuery, StubQuery

# constants file
from constants import CC


class SequentialOptimizer(AbstractOptimizer, metaclass=ABCMeta):
    """
    This specifies a class that runs a single or sequence of optimizations.
    Superclass Inputs:
        config: a configuration object
    Creates:
        grb_env should NOT be provided, because this one creates its own.
    """
    def __init__(self, **kwargs):
        assert 'grb_env' not in kwargs
        super().__init__(**kwargs)
        self.gurobiEnvironment()

    def gurobiEnvironment(self):
        """
        This method is a wrapper for a function that creates a new gurobi Environment
        This wrapper exists for a reason. DO NOT try to cut it out in __init__ or getGurobiEnvironment
        """
        self.grb_env     = self.getGurobiEnvironment()



class L2PlusRounderWithBackup(SequentialOptimizer):
    """
    L2GeoOpt + geoRound w/ degree-of-infeasibility failsafe
        Inputs:
            identifier: a string giving identifying information about the optimization (e.g. the parent geocode)
            parent: a numpy multi-array of the parent histogram (or None if no parent)
            parent_shape: the shape of the parent histogram or would be shape if parent histogram is None
            childGeoLen: int giving the number of child geographies
            constraints: a list of StackedConstraint objects (see constraints_dpqueries.py)
            NoisyChild: numpy multidimensional array of noisy measurments of the detailed child cells
            noisy_child_weights: float giving the coefficient for the optimization function for each NoisyChild cell
            DPqueries: a list of StackedDPquery objects (see constraints_dpqueries.py)
            query_weights: a list of floats giving the coefficent for the optimization function for each cell of each query or None
            min_schema: list of minimal schema dimensions
            constraints_schema: list of attributes that
            opt_tol: bool; True by default. Should data-ind npass optimizer use optimized tolerances?
            const_tol_val: float; optional, used by data-ind npass optimizer
            opt_tol_slack: float; optional, used by data-ind npass optimizer
        Superclass Inputs:
            config: a configuration object
    """

    # Key is config option value, value is tuple (OptimizerClass, requires OLS pre-pass)
    l2_optimizers_dict = {
        CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS_PLUS: (DataIndQueriesL2NPassImprov, False),
        CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS: (DataIndQueriesL2NPass, False),
        CC.SINGLE_PASS_REGULAR: (L2GeoOpt, False),
    }

    rounder_optimizers_dict = {
        CC.CELLWISE_ROUNDER: GeoRound,
        CC.MULTIPASS_ROUNDER: DataIndependentNpassRound,
        CC.MULTIPASS_QUERY_ROUNDER: DataIndependentNpassQueryRound,
    }

    def __init__(self, *, identifier, child_geolevel, parent, parent_shape, childGeoLen, constraints, NoisyChild, noisy_child_weights,
                    DPqueries, rounder_queries, min_schema, opt_dict, child_groups=None,
                    L2_DPqueryOrdering=None, L2_Constrain_to_Ordering=None, Rounder_DPqueryOrdering=None,
                    optimizers, **kwargs):

        """
        :param identifier: the geocode
        :param geolevel: the geolevel. e.g. 'us', 'state', 'county'
        """
        super().__init__(**kwargs)
        self.identifier: str = identifier # geocode
        self.child_geolevel: str = child_geolevel
        self.parent: Tuple[Union[np.ndarray, None], ...] = parent
        self.parent_shape: Tuple[Tuple[int, ...], ...] = parent_shape
        self.childGeoLen: int = childGeoLen
        self.constraints = constraints
        self.NoisyChild: np.ndarray = NoisyChild
        self.noisy_child_weights = noisy_child_weights
        self.DPqueries = DPqueries
        self.rounder_queries = rounder_queries
        self.min_schema: Tuple[Tuple[int, ...], ...] = min_schema
        self.opt_dict = opt_dict
        self.L2_DPqueryOrdering = L2_DPqueryOrdering
        self.L2_Constrain_to_Ordering = L2_Constrain_to_Ordering
        self.Rounder_DPqueryOrdering = Rounder_DPqueryOrdering
        self.nnls: bool = self.getboolean(CC.NONNEG_LB, default=True)
        self.opt_tol = True
        self.const_tol_val = 5.0
        self.opt_tol_slack = 0.01
        self.optimizers = optimizers
        optimal_only =  {toGRBfromStrStatus('OPTIMAL')}
        self.acceptable_l2_statuses = optimal_only.union(set(map(toGRBfromStrStatus, self.gettuple(CC.L2_ACCEPTABLE_STATUSES, default=()))))
        self.acceptable_rounder_statuses = optimal_only.union(set(map(toGRBfromStrStatus, self.gettuple(CC.ROUNDER_ACCEPTABLE_STATUSES, default=()))))

        print(f"NNLS will accept optimization status flags: {self.acceptable_l2_statuses}")
        print(f"Rounder will accept optimization status flags: {self.acceptable_rounder_statuses}")

        self.child_groups = child_groups

    def createL2opt(self, backup_feas, l2opt, nnls, ols_result=None):
        """ Creates L2GeoOpt object with or without backup feasibility on"""

        return l2opt(das=self.das, config=self.config, grb_env=self.grb_env,
                     identifier=self.identifier, child_geolevel=self.child_geolevel,
                        parent=self.parent, parent_shape=self.parent_shape,
                        childGeoLen=self.childGeoLen,
                        NoisyChild=self.NoisyChild, noisy_child_weights=self.noisy_child_weights,
                        constraints=self.constraints, DPqueries=self.DPqueries, min_schema=self.min_schema, nnls=nnls,
                        backup_feas=backup_feas, child_groups=self.child_groups, opt_dict=self.opt_dict,
                        dpq_order=self.L2_DPqueryOrdering,
                        const_tol=self.const_tol_val, opt_tol=self.opt_tol, opt_tol_slack=self.opt_tol_slack,
                        ols_result=ols_result, acceptable_l2_statuses=self.acceptable_l2_statuses)

    def createRounder(self, l2opt, backup_feas, rounder_opt_class):
        """ Creates Rounder object based on answer from an L2GeoOpt, with or without backup feasibility on """
        return rounder_opt_class(das=self.das, config=self.config, grb_env=self.grb_env, identifier=self.identifier, child_geolevel=self.child_geolevel,
                                 parent=self.parent, parent_shape=self.parent_shape,
                                 constraints=self.constraints,
                                 childGeoLen=self.childGeoLen, child=l2opt.answer,
                                 min_schema=self.min_schema, backup_feas=backup_feas, child_groups=self.child_groups,
                                 DPqueries=self.DPqueries, rounder_queries=self.rounder_queries,
                                 dpq_order=self.Rounder_DPqueryOrdering, acceptable_rounder_statuses=self.acceptable_rounder_statuses)

    def run(self):
        """
        Runs on the CORE nodes, not on the master.
        The main method running the chain of optimizations:
        Outputs:
            L2opt.answer: numpy multi-array, the un-rounded solution
            Rounder.answer: numpy multi-array, the rounded solution
        """
        # Standard L2
        l2_approach, rounder_approach = self.optimizers
        l2_opt_class, requires_ols_prepass = self.l2_optimizers_dict[l2_approach]
        rounder_opt_class = self.rounder_optimizers_dict[rounder_approach]
        failsafe_invoked = False

        if l2_approach in (CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS,
                           CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS_PLUS):
            tol_type = self.getconfig(CC.DATA_IND_NPASS_TOL_TYPE, default=CC.OPT_TOL)
            if tol_type == CC.CONST_TOL:
                self.const_tol_val = self.getfloat(CC.CONST_TOL_VAL, default=5.0)
                self.opt_tol = False
            elif tol_type == CC.OPT_TOL:
                self.opt_tol_slack = self.getfloat(CC.OPT_TOL_SLACK, default=0.01)
            else:
                raise ValueError(f"Unknown tolerance type for data-independent user-specified-queries multipass.")

        if not requires_ols_prepass:
            L2opt = self.createL2opt(backup_feas=False, l2opt=l2_opt_class, nnls=self.nnls)
            L2opt.run()
        else:
            L2opt_ols = self.createL2opt(backup_feas=False, l2opt=L2GeoOpt, nnls=False)
            L2opt_ols.run()
            L2opt = self.createL2opt(backup_feas=False, l2opt=l2_opt_class, nnls=self.nnls, ols_result=L2opt_ols.answer)
            L2opt.run()

        if L2opt.mstatus in self.acceptable_l2_statuses:  # and False or self.parent[0] is None:
            # TODO: Implement accounting for suboptimal solutions
            # Standard L1
            Rounder = self.createRounder(L2opt, backup_feas=False, rounder_opt_class=rounder_opt_class)
            Rounder.run()
            self.checkModelStatus(Rounder.mstatus, "Rounder_standard", CC.L1_FEAS_FAIL)
        else:
            # top node optimization can't be saved by backup feasibility relaxing parent constraints, throw an error
            if self.parent[0] is None:
                raise RuntimeError(f"Top node optimization failed with status {L2opt.mstatus}")

            failsafe_invoked = True
            t_failsafe_start = time.time()

            # L2 did not find solution with acceptable status; invoke the failsafe
            if not requires_ols_prepass:
                L2opt = self.createL2opt(backup_feas=True, l2opt=l2_opt_class, nnls=self.nnls)
                L2opt.run()
            else:
                L2opt = self.createL2opt(backup_feas=True, l2opt=L2GeoOpt, nnls=False)
                L2opt.run()
                L2opt = self.createL2opt(backup_feas=True, l2opt=l2_opt_class, nnls=self.nnls, ols_result=L2opt.answer)
                L2opt.run()

            self.checkModelStatus(L2opt.mstatus, "L2_failsafe", CC.L2_FEAS_FAIL)

            self.addGurobiStatistics(min_schema=self.min_schema)

            # Failsafe L1
            Rounder = self.createRounder(L2opt, backup_feas=True, rounder_opt_class=rounder_opt_class)
            Rounder.run()
            self.checkModelStatus(Rounder.mstatus, "Rounder_failsafe", CC.L1_FEAS_FAIL)

            # TODO: explicitly refers only first histogram below
            self.addGurobiStatistics(L1_failsafe_change=int(np.sum(np.abs(self.parent[0] - Rounder.answer[0].sum((-1,))))),
                                     failsafe_invoked=True,
                                     t_failsafe = time.time() - t_failsafe_start)

        self.addGurobiStatistics(L2opt_mstatus=L2opt.mstatus, msg='L2 model END')
        self.sendGurobiStatistics()
        del self.grb_env
        return L2opt.answer, Rounder.answer, failsafe_invoked

    def checkModelStatus(self, m_status, phase, msg):
        """
        Checks the status of the model, if not optimal, raises an exception
        Inputs:
            m_status: gurobi model status code
            phase: str indicating the (L2 or Rounder) phase
            msg: str, a message to print if not optimal
        """

        # TODO: Implement accounting for suboptimal solutions
        if m_status not in self.acceptable_l2_statuses:
            del self.grb_env
            raise Exception(f"{msg} Failure encountered in geocode {self.identifier} after {phase}. Model status was {m_status}.")


class L2PlusRounderWithBackup_interleaved(L2PlusRounderWithBackup):

    def __init__(self, *args, **kwargs):
        """
        Inputs:
            DPqueryOrdering: {str:int} indicating pass in which to target each DPquery
        """
        super().__init__(*args, **kwargs)

        self.L2_revDPqueryOrdering = self.reverseQueryOrdering(self.L2_DPqueryOrdering)
        self.L2_DPqueryOrdering_pairs = self.getDPqueryOrderingAsPairs(self.L2_DPqueryOrdering)
        self.L2_outerPassNums = sorted(self.L2_DPqueryOrdering.keys())

        self.revL2_Constrain_to_Ordering = self.reverseQueryOrdering(self.L2_Constrain_to_Ordering)
        self.L2_Constrain_to_Ordering_pairs = self.getDPqueryOrderingAsPairs(self.L2_Constrain_to_Ordering)
        self.L2_Constrain_to_outerPassNums = sorted(self.L2_Constrain_to_Ordering.keys())

        self.Rounder_revDPqueryOrdering = self.reverseQueryOrdering(self.Rounder_DPqueryOrdering)
        self.Rounder_DPqueryOrdering_pairs = self.getDPqueryOrderingAsPairs(self.Rounder_DPqueryOrdering)
        self.Rounder_outerPassNums = sorted(self.Rounder_DPqueryOrdering.keys())

        assert self.Rounder_outerPassNums == self.L2_outerPassNums, "L2, Rounder must agree on outer passes"
        assert self.L2_Constrain_to_outerPassNums == self.L2_outerPassNums, "Constraint-to, Rounder must agree on outer passes"
        self.outerPassNums = self.L2_outerPassNums

        print(f"dataIndQueriesL2NPass has DPqueryOrdering:")
        for key in self.L2_DPqueryOrdering.keys():
            print(f"{key} -> {self.L2_DPqueryOrdering[key]}")
        print(f"dataIndQueriesL2NPass has revDPqueryOrdering:")
        for key in self.L2_revDPqueryOrdering.keys():
            print(f"{key} -> {self.L2_revDPqueryOrdering[key]}")

    def createL2opt(self, backup_feas, l2opt, nnls, ols_result=None,
                    DPqueryOrdering=None, Constrain_to_Ordering=None, DPqueries=None):
        """ Creates L2GeoOpt object with or without backup feasibility on"""
        if DPqueries is None:
            DPqueries = self.DPqueries
        if DPqueryOrdering is None:
            DPqueryOrdering = self.L2_DPqueryOrdering
        if Constrain_to_Ordering is None:
            Constrain_to_Ordering = self.L2_DPqueryOrdering
        return l2opt(das=self.das, config=self.config, grb_env=self.grb_env,
                     identifier=self.identifier, child_geolevel=self.child_geolevel,
                        parent=self.parent, parent_shape=self.parent_shape,
                        childGeoLen=self.childGeoLen,
                        NoisyChild=self.NoisyChild, noisy_child_weights=self.noisy_child_weights,
                        constraints=self.constraints, DPqueries=DPqueries, min_schema=self.min_schema, nnls=nnls,
                        backup_feas=backup_feas, child_groups=self.child_groups, opt_dict=self.opt_dict,
                        dpq_order=DPqueryOrdering,
                        constrain_to_order=Constrain_to_Ordering,
                        const_tol=self.const_tol_val, opt_tol=self.opt_tol, opt_tol_slack=self.opt_tol_slack,
                        ols_result=ols_result, acceptable_l2_statuses=self.acceptable_l2_statuses)

    def createRounder(self, l2opt, backup_feas, rounder_opt_class, DPqueryOrdering=None, DPqueries=None):
        """ Creates Rounder object based on answer from an L2GeoOpt, with or without backup feasibility on """
        if DPqueries is None:
            DPqueries = self.rounder_queries
        if DPqueryOrdering is None:
            DPqueryOrdering = self.Rounder_DPqueryOrdering
        return rounder_opt_class(das=self.das, config=self.config, grb_env=self.grb_env,
                                 identifier=self.identifier, child_geolevel=self.child_geolevel,
                                 parent=self.parent, parent_shape=self.parent_shape,
                                 constraints=self.constraints,
                                 childGeoLen=self.childGeoLen, child=l2opt.answer,
                                 min_schema=self.min_schema, backup_feas=backup_feas, child_groups=self.child_groups,
                                 DPqueries=None, rounder_queries=DPqueries,
                                 dpq_order=DPqueryOrdering, acceptable_rounder_statuses=self.acceptable_rounder_statuses)

    def run(self):
        """
        :return:
        """
        for outerPassNum in self.outerPassNums:
            L2_answer, Rounder_answer = self.interleave_optimizations(outerPassNum=outerPassNum)
        failsafe_invoked = False # Failsafe is not supported by interleaved optimizer
        return L2_answer, Rounder_answer, failsafe_invoked

    def interleave_optimizations(self, outerPassNum=None):
        """
        Perform optimization for a single outer pass
        :param outerPassNum:
        :return: L2 answer and rounded answer
        """
        l2_approach, rounder_approach = self.optimizers
        l2_opt_class, requires_ols_prepass = self.l2_optimizers_dict[l2_approach]
        rounder_opt_class = self.rounder_optimizers_dict[rounder_approach]

        # Note: Not using L2 with OLS pre-prepass, if needed, copy the prepass from L2PlusRounderWithBackup
        L2_dpq_names = reduce(add, self.L2_DPqueryOrdering[outerPassNum].values())
        L2_queries = [list(filter(lambda dpq: dpq.name in L2_dpq_names, dpq_hist_set)) for dpq_hist_set in self.DPqueries]
        L2opt = self.createL2opt(backup_feas=False, l2opt=l2_opt_class, DPqueries=L2_queries,
                                nnls=True, DPqueryOrdering=self.L2_DPqueryOrdering[outerPassNum],
                                Constrain_to_Ordering=self.L2_Constrain_to_Ordering[outerPassNum])
        L2opt.run()
        if L2opt.mstatus not in self.acceptable_l2_statuses:
            raise NotImplementedError("ERROR: Interleaved optimization does not currently support failsafe!")

        rq_names = reduce(add, self.Rounder_DPqueryOrdering[outerPassNum].values())
        rounder_queries = [list(filter(lambda rq: rq.name in rq_names, rq_hist_set)) for rq_hist_set in self.rounder_queries]
        Rounder = self.createRounder(L2opt, backup_feas=False, rounder_opt_class=rounder_opt_class, DPqueries=rounder_queries,
                                        DPqueryOrdering=self.Rounder_DPqueryOrdering[outerPassNum])
        Rounder.run()
        self.checkModelStatus(Rounder.mstatus, "Rounder_standard", CC.L1_FEAS_FAIL)
        answer = Rounder.answer
        print(f"outerPass {outerPassNum} ---> L2: {L2_dpq_names}, L2_queries:: {L2_queries}, Rounder: {rq_names}, rounder_queries:: {rounder_queries}")

        for ihist, dp_queries in enumerate(self.rounder_queries):
            for st_dpq in dp_queries:
                final_rounder_pass = sorted(self.Rounder_DPqueryOrdering[outerPassNum].keys())[-1]
                if self.Rounder_revDPqueryOrdering[st_dpq.name] == (outerPassNum, final_rounder_pass):
                    q = st_dpq.query
                    queries = [StubQuery((int(np.prod(hist.shape) / self.childGeoLen), q.numAnswers()), "stub") for hist in answer]
                    coeffs = [0] * len(answer)
                    queries[ihist] = q
                    coeffs[ihist] = 1
                    multi_query = MultiHistQuery( tuple(queries), tuple(coeffs), f"{q.name}_multi")
                    if outerPassNum != self.outerPassNums[-1]:
                        constraints_indices_equal = []
                        for sc_child_ind, child_num in enumerate(st_dpq.indices):
                            pass_answer = multi_query.answer(np.hstack([hist.reshape((int(np.prod(hist.shape) / self.childGeoLen), self.childGeoLen))[:, child_num] for hist in answer]))
                            constraints_indices_equal.append((Constraint(multi_query, pass_answer, "=", f"outerPassNum_{outerPassNum}Constr_{q.name}_{child_num}"), child_num))
                        self.constraints.append(StackedConstraint(constraints_indices_equal))

        return L2opt.answer, Rounder.answer

    def reverseQueryOrdering(self, inputDict):
        """
            Takes a dict
            a = {int -> [str, ..., str]
            And inverts-plus decomposes it to
            b = {str -> int}
            such that set(b.keys()) == union_{k in a.keys()} set(a[k])
            str's are assumed to be distinct.
        """
        # from itertools import groupby
        # allStrs = reduce(add, [[dpqNamesList for dpqNamesList in innerDict.values()] for innerDict in inputDict.values()])
        # dpqNameFrequencies = [len(list(group)) for key, group in groupby(allStrs)]
        # assert max(dpqNameFrequencies) == 1, "dpqNames in inputDict must be distinct."

        # As we allow for a constrain_to dict, now, dpqNames no longer need to be distinct.
        reversedDict = {}
        for outerPassNum in inputDict.keys():
            for innerPassNum in inputDict[outerPassNum].keys():
                for dpqName in inputDict[outerPassNum][innerPassNum]:
                    reversedDict[dpqName] = (outerPassNum, innerPassNum)
        return reversedDict

    def getDPqueryOrderingAsPairs(self, inputDict):
        pairedDict = {}
        for outerPassNum in inputDict.keys():
            for innerPassNum in inputDict[outerPassNum].keys():
                pairedDict[(outerPassNum, innerPassNum)] = inputDict[outerPassNum][innerPassNum]
        return pairedDict
