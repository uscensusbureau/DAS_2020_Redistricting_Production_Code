import numpy as np
from programs.optimization.multipass_rounder import DataIndependentNpassRound

from constants import CC


class DataIndependentNpassQueryRound(DataIndependentNpassRound):
    """
        This class solves the integer rounding problem for geography imputation in a sequence of passes,
        where [1 - 2 *(Q * nnls - floor(Q * nnls))] * query_rounding_binary is the obj fxn term for each Q in the current pass.
        Takes a non-negative continuous/fractional solution and finds a nearby nonnegative integer solution.

        NOTE: only known to be feasible/tractable when restricted to a single-pass, with 2 hierarchical queries.
        NOTE2: obj fxn for individual histogram cells is treated separately, as [1 - 2 * (nnls - round(nnls))] * binary

        Inputs:
            child: a numpy array that is a non-negative solution to the L2 problem
            backup_feas: bool, if true run in backup feasibility mode
            min_schema: list giving dimensions of minimal schema or None
    """

    # variables used in run() function
    model_name = CC.MULTIPASS_QUERY_ROUNDER
    nnls: np.ndarray  ## Upstream L2 solution, not subset and with all histograms stacked together, each flattened to a row

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.q_binaries = {}  # Query rounding variables
        # Rounder child_sub is nnls - floor(nnls), but Query Rounder requires original nnls sol
        self.nnls = np.vstack([c.reshape(np.prod(ps), self.childGeoLen) for c, ps in zip(self.child, self.parent_shape)])

    def removeMultipassConstraints(self, model, q_set_list):
        for ihist, queries in enumerate(q_set_list):
            for st_q in queries:
                print(f"Removing {st_q.name} L1 penalty constraints from model...")
                model.remove(self.penalty_constrs[st_q.name][0])  # Single q_binary constr

    def addMultipassPenalties(self, model, csrQ, x, x_frac, n_ans, child_num, pass_num, qname, csrQ_unmasked=None):
        """
        Calculates L1 optimization terms for rounding and adds auxiliary constraints.
        The notations below are as follows:
        _f index means "floor", _l index is "leftover", i.e. x_l = x - x_f
        A quantity can also come as a numeric value (_num), or a gurobi variable to be optimized (_bvar).
        The optimization terms are |x_l_bvar - x_l_num| = (1-2*x_l_num)*x_bvar + const (for each child).
            :param x is x_l_bvar, gurobi binary variables, optimization of which the the goal of the rounder
        """
        import gurobipy as gb

        # Query answer on binary rounder/leftover variables should be equal to the binary rounder/leftover of the answer
        # Q @ x = Q @ (x_f + x_l) = Q @ x_f + Q @ x_l
        # also
        # Q @ x = (Q @ x)_f + (Q @ x)_l =>
        # => Q @ x_f + Q @ x_l = (Q @ x)_f + (Q @ x)_l, or using numerical values and binary optimization variables we have:
        # Q @ x_f_num + Q @ x_l_bvar = (Q @ x_num)_f + (Q @ x)_l_bvar

        # These are leftover binary optimization vars. I.e. 0 or 1 that is added to the floored answer to the query: (Q @ x)_l
        var_name = f"q_pass#{pass_num}_qBinary_{qname}_childNum{child_num}"
        q_binaries = model.addMVar(int(n_ans), vtype=gb.GRB.BINARY, lb=0.0, obj=0.0, name=var_name)   # (Q @ x)_l_bvar
        self.q_binaries[var_name] = q_binaries
        nnls_est = csrQ_unmasked @ self.nnls[:, child_num]      # Q @ NNLS, or  Q @ x_num == Q @ (x_f_num + x_l_num)
        # Can't use child_sub b/c Q @ NNLS - floor(Q @ NNLS) != floor(Q @ [NNLS-floor(NNLS)])
        nnls_floor_est = csrQ_unmasked @ np.floor(self.nnls[:, child_num])  # Q @ x_f_num
        constr_name = f"q_pass#{pass_num}qBinaryConsistencyConstr_{qname}_{child_num}_"
        # print(f"In {self.identifier}, {constr_name} shapes: {csrQ.shape}, {x.shape}, {child_sub[:, child_num].shape}")
        # print(f"And unmasked: {n_ans, }")

        # This constraint is as shown above: Q @ x_f_num + Q @ x_l_bvar == (Q @ x_num)_f + (Q @ x)_l_bvar
        constr = model.addConstr(nnls_floor_est + csrQ @ x == np.floor(nnls_est) + q_binaries, name=constr_name)
        self.penalty_constrs[qname] = [constr]  # List unnecessary, but mirroring structure in multipass_rounder.py

        # The optimization term is |var - leftover| = {leftover if var=0; 1-leftover if var=1} = leftover*(1-var) + (1-leftover)*var =
        # = leftover + var - 2*leftover*var = leftover + (1-2*leftover)*var = (1-2*leftover)*var + const
        # obj_coeffs: 1 - 2 *[Q @ NNLS - floor(Q @ NNLS)) ] (per child) -- this is the leftover, the numerical value
        obj_coeffs = 1. - 2. * (nnls_est - np.floor(nnls_est))  # 1 - 2 * x_l_num
        return obj_coeffs @ q_binaries


    def addMultipassConstraints(self, model, csrQ, x, x_frac, child_num, pass_num, qname):

        ## NOTE: The only reason it overrides superclass function is different set of prints

        # print(f"{st_q.name} unrounded: {(csrQ @ child_sub[:, child_num])}")
        # print(f"{st_q.name} rounded: {np.around(csrQ @ child_sub[:, child_num])}")
        constr_base_name = f"q_pass#{pass_num}MultipassConstr_{qname}_{child_num}_"
        model.addConstr(csrQ @ x - np.around(csrQ @ x.X) == 0.0, name=constr_base_name + "=")
