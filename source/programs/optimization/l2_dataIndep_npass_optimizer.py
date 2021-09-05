from typing import Callable, List, Iterable
from programs.optimization.geo_optimizers import L2GeoOpt
from copy import deepcopy
import numpy as np


class DataIndQueriesL2NPass(L2GeoOpt):
    """
        Class for defining multi-pass data-independent-queries-specified-by-user NNLS solver.
    """
    def __init__(self, *, dpq_order, constrain_to_order=None, const_tol=5.0, opt_tol=True, opt_tol_slack=0.01, **kwargs):
        """
        Inputs:
            dpq_order:              {int:[str,...,str]} indicating pass in which to target each DPquery
            constrain_to_order:     {int:[str,...,str]} indicating pass in which to constraint value of each DPquery
        """
        super().__init__(**kwargs)
        self.dpq_order = dpq_order  # {int:[str,...,str]}, pass # -> [...,dpq_names,...]
        self.rev_dpq_order = self.reverseQueryOrdering(self.dpq_order)  # {str : set([int,...,int])}
        if constrain_to_order is not None:
            self.constrain_to_order = constrain_to_order
        else:
            self.constrain_to_order = self.dpq_order
        self.rev_constrain_to_order = self.reverseQueryOrdering(self.constrain_to_order)

        assert set(self.constrain_to_order.keys()) == set(self.dpq_order.keys()), f"All passes must appear >=1 time per order. constrain-to order: {self.constrain_to_order.keys()}, dpq_order: {self.dpq_order.keys()}"
        self.pass_nums = sorted(self.dpq_order.keys())  # Config file integers numbering each multi-pass pass
        self.cached_nnls_ests = {}  # Cached references to relevant pass_num NNLS estimates of each query
        self.const_tol = const_tol  # If not optimizing = as <=, >= tol per problem, tolerance to use instead
        self.opt_tol = opt_tol      # Should we optimize tol for float = modeled as pair of <=, >= in current problem?
        self.opt_tol_slack = opt_tol_slack  # If using opt_tol, what extra additive slack should be added?
        self.var_tol_constrs = {}    # Keeps references to opt_tol constraints, for later removal

    def optimizationPassesLoop(self, model, obj_fxns, two_d_vars, n_list, child_sub, parent_mask):
        """
            --- Primary entry point for understanding optimization logic ---
            Multi-pass optimization loop: optimize, add constraints for current pass target queries, optimize further, etc.
                (called by run method of parent class)
        """
        for pass_index, pass_num in enumerate(self.pass_nums):
            print(f"L2 dataInd multipass starting run for pass_index, pass_num {pass_index}, {pass_num}")
            tol = self.const_tol   # Default slack/tolerance for float = modeled as pair of <=, >= constraints
            if self.opt_tol:
                tol = self.findOptimalTol(model, two_d_vars, parent_mask, pass_index) + self.opt_tol_slack  # Find opt tolerance
            self.addMultiPassDPQueryConstraints(model, two_d_vars, parent_mask, pass_index, tol)  # Constrain to prior pass ests
            self.setObjAndSolve(model, obj_fxns[pass_num])                                        # Optimize
            self.cacheCurrentPassNnlsEstimates(two_d_vars, parent_mask, pass_index)               # Cache current pass ests
            if not (model.Status in self.acceptable_statuses):
                print(f"Main L2 dataInd multipass solve for pass_index, pass_num {pass_index}, {pass_num} in node w/ id {self.identifier} returned unacceptable status: {model.Status}. Acceptable statuses: {self.acceptable_statuses}")
                break

    def buildObjFxnAddQueries(self, model, two_d_vars, n_list, child_sub, parent_mask):
        """
            Build obj fxns for all passes, & add corresponding DP queries (called by run method of parent class)
        """
        obj_fxns = {pass_num: 0 for pass_num in self.pass_nums}
        obj_fxns[self.pass_nums[-1]] = self.buildObjFxn(two_d_vars, n_list, child_sub, self.child_obj_term_wt)
        for pass_num in self.pass_nums:
            q_set_list = [filter(lambda dpq: pass_num in self.rev_dpq_order[dpq.name], dpq_hist_set) for dpq_hist_set in self.DPqueries]
            # q_set_list = self.filterQueries(lambda dpqname: pass_num in self.rev_dpq_order[dpqname], self.DPqueries)
            obj_fxns = self.addDPQueriesToModel(model, two_d_vars, obj_fxns, parent_mask, q_set_list=q_set_list, pass_num=pass_num)
        return obj_fxns

    def childDPQueryTerm(self, model, obj_fxns, A, x, b, weight, n_ans, qname, lb, pass_num=None):
        """
            Appends DPQuery term corresponding to pass :pass_num: to obj fxn for that pass
        """
        obj_fxns[pass_num] += self.addObjFxnTerm(model, A, x, b, weight, int(n_ans), f"dpq_objFxn_#{pass_num}_{qname}", lb)
        return obj_fxns

    def addMultiPassDPQueryConstraints(self, model, two_d_vars, parent_mask, pass_index, tol):
        """
            Appends DPQuery constraints to model. In pass # i, the pass # i-1 estimates for the DPqueries
                {dpq : self.pass_nums[pass_index-1] in self.rev_dpq_order[dpq.name]}
            are appended as constraints to model, with a constant L1 tolerance, like:
                |dpQuery @ currentHistogram - dpQueryCachedNnlsEstimate| <= tol
        Inputs:
            :param model:       GRB model, to which constraints & vars are added
            :param two_d_vars:  2-d (parent_masked_vars_per_geo X #_child_geos) GRB MVar variable object
            :param parent_mask: np 1-d boolean array, True on indexes corresponding to cells w/ >0.0 parent estimates; these
                                indicate which child variables need to be explicitly represented in two_d_vars &  in DP
                                query-as-matrix column sets
            :param pass_index:    integer indicating index of current pass_num
            :param tol:         tolerance for approximating = by pair of <=, => in DPquery constraints
        """
        pass_num = self.pass_nums[pass_index]
        for ihist, dp_queries in enumerate(self.DPqueries):
            for st_dpq in dp_queries:
                if pass_index > 0 and self.pass_nums[pass_index-1] in self.rev_constrain_to_order[st_dpq.name]:
                    query = st_dpq.query
                    matrix_rep = query.matrixRep()
                    n_ans = query.numAnswers()
                    # Add empty columns for preceding and succeeding histograms to the matrix
                    matrix_rep = self.padQueryMatrix(ihist, matrix_rep, n_ans)
                    # Drop columns corresponding to variables already zero'd out in parent
                    csrQ = matrix_rep[:, parent_mask].tocsr()
                    for sc_child_ind, child_num in enumerate(st_dpq.indices):
                        x = two_d_vars[:, child_num]
                        cached_est = self.cached_nnls_ests[(st_dpq.name, child_num)]
                        base_name = f"dpq_pass#{pass_num}Constr_{st_dpq.name}_{child_num}"
                        model.addConstr( csrQ @ x - cached_est <= tol, name=base_name+"_+")
                        model.addConstr(-csrQ @ x + cached_est <= tol, name=base_name+"_-")

    def findOptimalTol(self, model, two_d_vars, parent_mask, pass_index):
        """
            In pass # i, the pass # i-1 estimates for the DPqueries
                {dpq : self.dpq_order[dpq.name] == self.pass_nums[pass_index-1]}
            are appended as constraints to model, with a variable L1 tolerance, like:
                |dpQuery @ currentHistogram - dpQueryCachedEstimate| <= tol
            We use this to solve for the smallest tol that achieves feasibility. This optimal tolerance is then re-used
            by addMultiPassDPQueryConstraints.

        Inputs:
            :param model:       GRB model, to which constraints & vars are added
            :param two_d_vars:  2-d (parent_masked_vars_per_geo X #_child_geos) GRB MVar variable object
            :param parent_mask: np 1-d boolean array, True on indexes corresponding to cells w/ >0.0 parent estimates; these
                                indicate which child variables need to be explicitly represented in two_d_vars &  in DP
                                query-as-matrix column sets
            :param pass_index:     integer indicating index of current pass_num

        Outputs:
            opt_tol: GRB estimated minimum tolerance consistent with feasibility
        """
        import gurobipy as gb
        pass_num = self.pass_nums[pass_index]
        obj_fxn = 0
        var_name = f"dpq_pass#{pass_num}_multipassToleranceVar"
        tol = model.addVar(lb=0.0, vtype=gb.GRB.CONTINUOUS, name=var_name)
        obj_fxn += np.ones(1) @ gb.MVar([tol]) # Obj fxn is just the scalar bound on slack, tol

        for ihist, dp_queries in enumerate(self.DPqueries):
            for st_dpq in dp_queries:
                if pass_index > 0 and self.pass_nums[pass_index-1] in self.rev_constrain_to_order[st_dpq.name]:
                    query = st_dpq.query
                    matrix_rep = query.matrixRep()
                    n_ans = query.numAnswers()
                    # Add empty columns for preceding and succeeding histograms to the matrix
                    matrix_rep = self.padQueryMatrix(ihist, matrix_rep, n_ans)
                    # Drop columns corresponding to variables already zero'd out in parent
                    csrQ = matrix_rep[:, parent_mask].tocsr()
                    for sc_child_ind, child_num in enumerate(st_dpq.indices):
                        tol_vec = gb.MVar([tol for _ in range(n_ans)])  # n_ans-len 1-d np.array, to match csrQ @ x shape
                        x = two_d_vars[:, child_num]
                        c1_name = f"dpq_varTol_pass#{pass_num}Constr_{st_dpq.name}_{child_num}_+"
                        c2_name = f"dpq_varTol_pass#{pass_num}Constr_{st_dpq.name}_{child_num}_-"
                        c1 = model.addConstr( csrQ @ x - csrQ @ x.X <= tol_vec, name=c1_name)
                        c2 = model.addConstr(-csrQ @ x + csrQ @ x.X <= tol_vec, name=c2_name)
                        self.var_tol_constrs[c1_name] = c1
                        self.var_tol_constrs[c2_name] = c2  # Used to remove var-tol constraints after finding opt_tol
        model.Params.Crossover = 0   # Disable crossover; unneeded here, & sometimes generates very large solve times
        model.Params.Method = 2      # Disabling crossover requires non-concurrent solve approach; we use barrier-only, here
        initial_barIterLimit = model.Params.BarIterLimit
        model.Params.BarIterLimit = 50          # Solution is generally acceptable after 50 iters, & tols provide limited control
        self.setObjAndSolve(model, obj_fxn)
        model.Params.BarIterLimit = initial_barIterLimit    # Restore original BarIterLimit
        model.Params.Method = -1     # Restore solve Method to automatic
        model.Params.Crossover = -1  # Restore crossover to default (automatic)
        opt_tol = deepcopy(tol.X) + model.ConstrResidual  # In case of suboptimal termination, primal infeasibility in findOptTol model may be important contributor to required tolerance estimate
        model.remove(tol)
        for name, con in self.var_tol_constrs.items():
            model.remove(con)
        print(f"In {self.identifier} for NNLS multipass pass # {pass_num}, solved for optimal tolerance: {opt_tol}")
        return opt_tol

    def cacheCurrentPassNnlsEstimates(self, two_d_vars, parent_mask, pass_index):
        """
            Caches NNLS estimates for recently completed pass # pass_index, i.e., for the DPqueries
                {dpq : self.dpq_order[dpq.name] == self.pass_nums[pass_index]}

        Inputs:
            :param two_d_vars:  2-d (parent_masked_vars_per_geo X #_child_geos) GRB MVar variable object
            :param parent_mask: np 1-d boolean array, True on indexes corresponding to cells w/ >0.0 parent estimates; these
                                indicate which child variables need to be explicitly represented in two_d_vars &  in DP
                                query-as-matrix column sets
            :param pass_index:    integer indicating index of current pass_num
        """
        pass_num = self.pass_nums[pass_index]
        for ihist, dp_queries in enumerate(self.DPqueries):
            for st_dpq in dp_queries:
                print(f"In pass # {pass_num}, considering whether to cache {st_dpq.name}...")
                if self.pass_nums[pass_index] in self.rev_dpq_order[st_dpq.name]:
                    query = st_dpq.query
                    matrix_rep = query.matrixRep()
                    n_ans = query.numAnswers()
                    # Add empty columns for preceding and succeeding histograms to the matrix
                    matrix_rep = self.padQueryMatrix(ihist, matrix_rep, n_ans)
                    # Drop columns corresponding to variables already zero'd out in parent
                    csrQ = matrix_rep[:, parent_mask].tocsr()
                    for sc_child_ind, child_num in enumerate(st_dpq.indices):
                        x = two_d_vars[:, child_num]
                        self.cached_nnls_ests[(st_dpq.name, child_num)] = csrQ @ x.X
                        print(f"In pass # {pass_num}, added cached estimate for {st_dpq.name}, {child_num}: {csrQ @ x.X}")

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
