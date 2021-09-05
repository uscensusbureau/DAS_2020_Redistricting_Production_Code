from typing import Callable, List, Iterable
import numpy as np
from programs.optimization.simple_rounder import GeoRound

from constants import CC, DETAILED


class DataIndependentNpassRound(GeoRound):
    """
        This class solves the integer rounding problem for geography imputation in a sequence of passes,
        where |Q * (nnls - round(nnls)) + Q * rounder_binaries)| is the obj fxn term for each Q in the current pass.
        Takes a non-negative continuous/fractional solution and finds a nearby nonnegative integer solution.

        NOTE: only known to be feasible/tractable when restricted to a single-pass, with 2 hierarchical queries.
        NOTE2: obj fxn for individual histogram cells is treated separately, as [1 - 2 * (nnls - round(nnls))] * binary

        Inputs:
            child: a numpy array that is a non-negative solution to the L2 problem
            backup_feas: bool, if true run in backup feasibility mode
            min_schema: list giving dimensions of minimal schema or None
    """

    # variables used in run() function
    model_name = CC.MULTIPASS_ROUNDER

    def __init__(self, *, DPqueries, rounder_queries, dpq_order, **kwargs):
        super().__init__(**kwargs)
        self.DPqueries = DPqueries  # Not used
        self.rounder_queries = rounder_queries
        self.dpq_order = dpq_order
        self.rev_dpq_order = self.reverseQueryOrdering(self.dpq_order)
        self.pass_nums = sorted(self.dpq_order.keys())
        self.penalty_constrs = {}  # References to constraints for later removal from model
        print(f"{self.model_name} rev_dpq_order in geocode {self.identifier}: {self.rev_dpq_order}")

    def optimizationPassesLoop(self, model, obj_fxns, two_d_vars, n_list, child_sub, parent_mask):
        """
            --- Primary entry point for understanding optimization logic ---
            Multi pass optimization loop: optimize, add constraints to this optimization, optimize further etc.
        """
        for pass_index, pass_num in enumerate(self.pass_nums):
            print(f"Rounder dataInd multipass starting run for pass_index, pass_num {pass_index}, {pass_num}")

            # If not first pass, set up constraints to previous passes
            if pass_index > 0:
                # Change this to self.filterQuery if you want to have queries that are not in ordering list and get KeyError
                q_set_list = [filter(lambda rq: self.pass_nums[pass_index - 1] in self.rev_dpq_order[rq.name], rq_hist_set) for rq_hist_set in self.rounder_queries]
                #q_set_list = self.filterQueries(lambda rqname: self.rev_dpq_order[rqname] == self.pass_nums[pass_index - 1], self.rounder_queries)
                self.addDPQueriesToModel(model, two_d_vars, obj_fxns, parent_mask, pass_index, child_sub, mode="constraints", q_set_list=q_set_list)

            # Add multipass penalties
            obj_fxn = self.buildL1PenaltyObjFxn(model, two_d_vars, child_sub, parent_mask, pass_index, pass_num)

            # Remove constraints to passes before latest
            if pass_index - 2 in self.pass_nums:
                # Change this to self.filterQuery if you want to have queries that are not in ordering list and get KeyError
                #q_set_list = [filter(lambda rq: self.rev_dpq_order[rq.name] <= self.pass_nums[pass_index - 2], rq_hist_set) for rq_hist_set in self.rounder_queries]
                q_set_list = self.filterQueries(lambda rqname: np.all(np.array(list(self.rev_dpq_order[rqname])) <= self.pass_nums[pass_index - 2]), self.rounder_queries)
                self.removeMultipassConstraints(model, q_set_list)

            self.setObjAndSolve(model, obj_fxn)
            if not (model.Status in self.acceptable_statuses):
                print(f"{self.model_name} solve for pass_index, pass_num {pass_index}, {pass_num} in node w/ id {self.identifier} returned unacceptable status: {model.Status}. Acceptable statuses: {self.acceptable_statuses}")
                break

    def buildObjFxnAddQueries(self, model, two_d_vars, n_list, child_sub, parent_mask):
        """
            Return empty stub obj fxns; build actual obj fxns right before each pass (due to need to add L1 penalty constraints)

            (no input args needed, but args passed for consistency with fxn signature in parent)
        """
        obj_fxns = {pass_num: 0 for pass_num in self.pass_nums}
        return obj_fxns

    def buildL1PenaltyObjFxn(self, model, two_d_vars, child_sub, parent_mask, pass_index, pass_num):
        """
            Build obj fxn for current pass, & adding corresponding DP queries
        """
        print(f"Received pass numbers: {self.pass_nums}")
        obj_fxn = 0
        # Filter queries to those that are targeted in current pass
        # (Change this to self.filterQuery if you want to have queries that are not in ordering list and get KeyError)
        #q_set_list = [filter(lambda dpq: self.rev_dpq_order[dpq.name] == pass_num, rq_hist_set) for rq_hist_set in self.rounder_queries]
        q_set_list = self.filterQueries(lambda dpqname: pass_num in self.rev_dpq_order[dpqname], self.rounder_queries)
        obj_fxn += self.addDPQueriesToModel(model, two_d_vars, obj_fxn, parent_mask, pass_index, child_sub, mode="penalties", q_set_list=q_set_list)
        if DETAILED in self.rev_dpq_order and self.rev_dpq_order[DETAILED] == pass_num:
            obj_fxn += self.buildCellwiseObjFxn(two_d_vars, child_sub)
        return obj_fxn

    def addDPQueriesToModel(self, model, two_d_vars, obj_fxn, parent_mask, pass_index=0, child_sub=None, mode=None, q_set_list=None):
        """
            Appends DPQueries for current pass or constraints for preceding pass dpqs to obj fxn.
        """
        pass_num = self.pass_nums[pass_index]
        obj_fxn_terms = 0
        for ihist, queries in enumerate(q_set_list):
            for st_q in queries:
                query = st_q.query
                matrix_rep = query.matrixRep()
                n_ans = query.numAnswers()
                matrix_rep = self.padQueryMatrix(ihist, matrix_rep, n_ans)
                csrQ_unmasked = matrix_rep.tocsr()
                csrQ = csrQ_unmasked[:, parent_mask].tocsr()
                # print(f"In {self.identifier}: in mode {mode} for pass_index {pass_index} and dpq {st_q.name}")
                for sc_child_ind, child_num in enumerate(st_q.indices):
                    x = two_d_vars[:, child_num]  # Rounder binary optimization variables
                    x_frac = child_sub[:, child_num]  # Fractional part of the NNLS solution
                    if len(x_frac) == 0:  # If L2 sol is integer for child_num, child_sub has len 0
                        continue
                    if mode == "constraints":
                        self.addMultipassConstraints(model, csrQ, x, x_frac, child_num, pass_num, st_q.name)
                    elif mode == "penalties":
                        obj_fxn_terms += self.addMultipassPenalties(model, csrQ, x, x_frac, n_ans, child_num, pass_num, st_q.name, csrQ_unmasked=csrQ_unmasked)
                    else:
                        raise ValueError(f"Mode is '{mode}'. Mode should be `constraints' or 'penalties'")
        return obj_fxn_terms

    def removeMultipassConstraints(self, model, q_set_list):
        for ihist, queries in enumerate(q_set_list):
            for st_q in queries:
                print(f"Removing {st_q.name} L1 penalty constraints from model...")
                model.remove(self.penalty_constrs[st_q.name][0])  # + L1 constraint
                model.remove(self.penalty_constrs[st_q.name][1])  # - L1 constraint

    def addMultipassPenalties(self, model, csrQ, x, x_frac, n_ans, child_num, pass_num, qname, csrQ_unmasked=None):
        import gurobipy as gb

        L1_penalty = model.addMVar(int(n_ans), vtype=gb.GRB.CONTINUOUS, lb=0.0, name=f"dpq_pass#{pass_num}_L1PenVar_{qname}_childNum{child_num}")

        constr_base_name = f"dpq_pass#{pass_num}L1PenConstr_{qname}_{child_num}_"
        print(f"In {self.identifier}, {constr_base_name} shapes: {csrQ.shape}, {x.shape}, {x_frac.shape}")

        con1 = model.addConstr(csrQ @ x - np.around(csrQ @ x_frac) <= L1_penalty, name=constr_base_name + "+")
        con2 = model.addConstr(-csrQ @ x + np.around(csrQ @ x_frac) <= L1_penalty, name=constr_base_name + "-")
        self.penalty_constrs[qname] = [con1, con2]
        return np.ones(n_ans) @ L1_penalty

    def addMultipassConstraints(self, model, csrQ, x, x_frac, child_num, pass_num, qname):
        print(f"{qname} unrounded: {(csrQ @ x_frac)}")
        print(f"{qname} rounded: {np.around(csrQ @ x_frac)}")
        constr_base_name = f"dpq_pass#{pass_num}MultipassConstr_{qname}_{child_num}_"
        model.addConstr(csrQ @ x - np.around(csrQ @ x.X) == 0.0, name=constr_base_name + "=")
        # model.addConstr( csrQ @ x - csrQ @ x_frac <= tol, name=constr_base_name+"+")
        # model.addConstr(-csrQ @ x + csrQ @ x_frac <= tol, name=constr_base_name+"-")

    def filterQueries(self, filter_func: Callable, query_set_list: List[Iterable]) -> List[Iterable]:
        """
        Take list of sets of queries (each entry of the list corresponds to a set of queries for a single histogram,
        length of the list is thus the number of histograms) and filter it according to criterion calculated by the
        filter_func, and return the filtered
        """
        filtered_qsetlist = []
        # Loop over histograms
        for rq_hist_set in query_set_list:
            # Loop over queries inside a per-histogram set
            filtered_qset = []
            for rq in rq_hist_set:
                if rq.name in self.rev_dpq_order and filter_func(rq.name):
                    filtered_qset.append(rq)
            filtered_qsetlist.append(filtered_qset)
        return filtered_qsetlist
