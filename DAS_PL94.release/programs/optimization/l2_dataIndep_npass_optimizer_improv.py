# import time
from copy import deepcopy
import numpy as np

from programs.optimization.geo_optimizers import L2GeoOpt
from constants import DETAILED

# from constants import CC

class DataIndQueriesL2NPassImprov(L2GeoOpt):
    """
        Class for defining improved multi-pass data-independent-queries-specified-by-user OLS/NNLS solver.
    """

    def __init__(self, *, opt_dict, const_tol=1.0, opt_tol=False, opt_tol_slack=0.01, **kwargs):
        """
        Inputs:
            :param opt_dict:    dictionary object containing information for how to make multiple optimization passes
        """
        super().__init__(**kwargs)
        self.opt_dict = opt_dict
        self.passNums = sorted(self.opt_dict["npass_info"].keys())
        self.const_tol = const_tol  # If not optimizing = as <=, >= tol per problem, tolerance to use instead
        self.opt_tol = opt_tol      # Should we optimize tol for float = modeled as pair of <=, >= in current problem?
        self.opt_tol_slack = opt_tol_slack  # If using opt_tol, what extra additive slack should be added?
        self.var_tol_constrs = {}    # Keeps references to opt_tol constraints, for later removal

        assert len(self.passNums) > 1, "data-independent-queries L2NPass requires queries split in multiple passes."

        # if self.opt_tol:
        #     raise NotImplementedError()
        # else:
        #     self.tol = self.const_tol


    # The .run() function does not need to be overridden since it doesn't change anything from the superclass.

    def optimizationPassesLoop(self, model, obj_fxn, two_d_vars, n_list, child_sub, parent_mask):
        """
        This function performs the looping optimization passes
            Inputs:
                :param model:       GRB model, to which constraints & vars are added
                :param obj_fxn      model objective function
                :param two_d_vars:  2-d (parent_masked_vars_per_geo X #_child_geos) GRB MVar variable object
                :param n_list       a list of how many gurobi variables (=possible non-zero cells in the histogram) there are in each histogram
                :param child_sub    array for per-each-cell terms in the objective function (coming from the detailed query in L2Opt and from L2opt result in Rounder)
                :param parent_mask: np 1-d boolean array, True on indexes corresponding to cells w/ >0.0 parent estimates; these
                                indicate which child variables need to be explicitly represented in two_d_vars &  in DP
                                query-as-matrix column sets
        """
        # Looping passes
        model, obj_fxn = self.multiPassHelper(model, obj_fxn, two_d_vars, parent_mask)

        #If using OLS solves, change the final solve to NNLS
        # #set lower bounds on variables to be 0 (NNLS)
        # var_dim = two_d_vars.shape
        # for a in range(var_dim[0]):
        #     for b in range(var_dim[1]):
        #         two_d_vars[a,b].lb = 0

        #final solve, only if using nnls
        #self.setObjAndSolve(model, obj_fxn)

    def buildObjFxnAddQueries(self, model, two_d_vars, n_list, child_sub, parent_mask):
        return 0

    def multiPassHelper(self, model, obj_fxn, two_d_vars, parent_mask):
        """
        Function that performs the actual multipass solves

        Inputs:
            :param model:       GRB model, to which constraints & vars are added
            :param obj_fxn      model objective function
            :param two_d_vars:  2-d (parent_masked_vars_per_geo X #_child_geos) GRB MVar variable object
            :param parent_mask: np 1-d boolean array, True on indexes corresponding to cells w/ >0.0 parent estimates; these
                                    indicate which child variables need to be explicitly represented in two_d_vars &  in DP
                                    query-as-matrix column sets

        Output:
            model
            obj_fxn
        """
        import gurobipy as gb

        stacked_est_cons = self.opt_dict["Cons"]
        npass_info = self.opt_dict["npass_info"]

        for passIndex, passNum in enumerate(self.passNums):

            print("passIndex")
            print(passIndex)

            est_queries = npass_info[passNum]["Est"]
            est_queries_names = [est_queries[query].name for query in est_queries]

            print("we want to estimate the following queries in this pass")
            print(est_queries_names)

            stacked_constraints = [x for x in stacked_est_cons if x.name in est_queries_names]

            # add estimation query non-negative constriants (NNLS), only needed if doing OLS solves
            # for c in stacked_constraints:
            #    self.addConstraint(c, model, parent_mask, two_d_vars, self.rounder)

            # add new dp queries to objective function
            querynames = self.opt_dict["npass_info"][passNum]["Qadd"]

            q_set_list = [tuple(filter(lambda q: q.name in querynames, dpq_hist_set)) for dpq_hist_set in self.DPqueries]

            print("adding the following dp queries to the model")
            print([[q.name for q in dq_hist_set] for dq_hist_set in q_set_list])

            obj_fxn = self.addDPQueriesToModel(model, two_d_vars, obj_fxn, parent_mask, q_set_list=q_set_list)

            # add constraints from previous pass after finding optimal tol
            #we haven't solved the model yet for this pass, so the solution is still the same as at the end of last pass
            if(passNum > 0):

                # Add dpq estimates as constraints
                joint_sol = np.zeros((int(np.sum(self.hist_sizes)), self.childGeoLen))
                joint_sol[parent_mask, :] = two_d_vars.X

                # finding the optimal tolerance
                if self.opt_tol:
                    opt_tol = self.findOptimalTol(model, two_d_vars, parent_mask, stacked_constraints_last_pass)
                    tol = opt_tol + self.opt_tol_slack
                else:
                    tol = self.const_tol

                for c in stacked_constraints_last_pass:
                    if c.name == DETAILED:  # don't add a detailed constraint, unnecessary
                        continue

                    #### Way #1
                    new_rhsList = [c.query.answer(joint_sol[:, i]) for i in range(self.childGeoLen)]
                    # replace the rhsList
                    # replace the sign
                    c.rhsList = [x - tol for x in new_rhsList]
                    c.sign = "ge"
                    self.addConstraint(c, model, parent_mask, two_d_vars, False)
                    c.rhsList = [x + tol for x in new_rhsList]
                    c.sign = "le"
                    self.addConstraint(c, model, parent_mask, two_d_vars, False)

                    ##### Way #2
                    # # The same as above, is simpler, but for some reason adding constraints in a single loop yields slightly different result
                    # matrix_rep = c.query.matrixRep()[:, parent_mask]
                    # for child_num in c.indices:
                    #     rhs = c.query.answer(joint_sol[:, child_num])
                    #     x = two_d_vars[:, child_num]
                    #     model.addConstr(matrix_rep @ x >= rhs - tol, name=c.name + '+')
                    #     model.addConstr(matrix_rep @ x <= rhs + tol, name=c.name + '-')

                    ##### Way #3
                    # # This one, with no loops, gives yet another slightly different answers
                    # matrix_rep = c.query.matrixRep()[:, parent_mask]
                    # optimized_all_children = joint_sol[parent_mask, :].ravel("F")
                    # x = gb.MVar(two_d_vars[:, c.indices].vararr.ravel("F"))  # Stack optimization vars for children into 1 vector
                    # num_chld = len(c.indices)  # How many children have this query
                    # Acoo = matrix_rep.tocoo()
                    # col = np.tile(Acoo.col, num_chld) + np.repeat(np.arange(num_chld), len(Acoo.col)) * Acoo.shape[1]
                    # row = np.tile(Acoo.row, num_chld) + np.repeat(np.arange(num_chld), len(Acoo.row)) * Acoo.shape[0]
                    # data = np.tile(Acoo.data, num_chld)
                    # import scipy.sparse as ss
                    # A = ss.coo_matrix((data, (row, col)), shape=(matrix_rep.shape[0] * num_chld, matrix_rep.shape[1] * num_chld)).tocsr()
                    # model.addConstr(A @ x >= A @ optimized_all_children - tol, name=c.name + '+')
                    # model.addConstr(A @ x <= A @ optimized_all_children + tol, name=c.name + '-')

            # solve
            self.setObjAndSolve(model, obj_fxn)
            if model.Status != gb.GRB.OPTIMAL:
                raise RuntimeError(f"Non-optimal Gurboi status{model.Status}")

            #save a copy of the target query constraints to be used in the next pass (not 100% deep copy is necessary, but just to be sure
            stacked_constraints_last_pass = deepcopy(stacked_constraints)

        return model, obj_fxn

    def findOptimalTol(self, model, two_d_vars, parent_mask, stacked_constraints):
        """
            In pass # i, the pass # i-1 estimates for the DPqueries
                {dpq : self.dpq_order[dpq.name] == self.pass_nums[pass_index-1]}
            are appended as constraints to model, with a variable L1 tolerance, like:
                |dpQuery @ currentHistogram - dpQueryCachedEstimate| <= tol
            We use this to solve for the smallest tol that achieves feasibility.

        Inputs:
            :param model:       GRB model, to which constraints & vars are added
            :param two_d_vars:  2-d (parent_masked_vars_per_geo X #_child_geos) GRB MVar variable object
            :param parent_mask: np 1-d boolean array, True on indexes corresponding to cells w/ >0.0 parent estimates; these
                                indicate which child variables need to be explicitly represented in two_d_vars &  in DP
                                query-as-matrix column sets
            :param stacked_constraints: stacked constraint object
        Outputs:
            opt_tol: GRB estimated minimum tolerance consistent with feasibility
        """
        import gurobipy as gb
        obj_fxn = 0
        var_name = "multipassTol"
        tol = model.addVar(lb=0.0, vtype=gb.GRB.CONTINUOUS, name=var_name)
        obj_fxn += np.ones(1) @ gb.MVar([tol])  # Obj fxn is just the scalar bound on slack, tol

        for c in stacked_constraints:
            if c.name != DETAILED:  # don't add a detailed constraint, unnecessary
                st_dpq = c
                query = st_dpq.query
                matrix_rep = query.matrixRep()
                n_ans = query.numAnswers()
                csrQ = matrix_rep[:, parent_mask].tocsr()
                for sc_child_ind, child_num in enumerate(st_dpq.indices):
                    tol_vec = gb.MVar([tol for i in range(n_ans)])  # n_ans-len 1-d np.array, to match csrQ @ x shape
                    x = two_d_vars[:, child_num]
                    c1_name = f"dpq_varTolConstr_{st_dpq.name}_{child_num}_+"
                    c2_name = f"dpq_varTolConstr_{st_dpq.name}_{child_num}_-"
                    c1 = model.addConstr(csrQ @ x - csrQ @ x.X <= tol_vec, name=c1_name)
                    c2 = model.addConstr(-csrQ @ x + csrQ @ x.X <= tol_vec, name=c2_name)
                    self.var_tol_constrs[c1_name] = c1
                    self.var_tol_constrs[c2_name] = c2  # Used to remove var-tol constraints after finding opt_tol

        self.setObjAndSolve(model, obj_fxn)
        opt_tol = deepcopy(tol.X)

        print(f"In {self.identifier} for NNLS multipass, solved for optimal tolerance: {opt_tol}")

        model.remove(tol)
        for name, con in self.var_tol_constrs.items():
            model.remove(con)

        return opt_tol
