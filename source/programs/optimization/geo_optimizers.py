## Note:
##
## Do not import gurobipy at top level.
## sys.path() is set depending on which version of gurobi we are using.
## path is set in AbstractOptimizer.

from typing import Tuple, List, Union
import scipy.sparse as ss
import numpy as np

# das-created imports
from programs.optimization.optimizer import GeoOptimizer
from programs.queries.querybase import MultiHistQuery
from programs.optimization.maps import toGRBfromStrStatus

# constants file
from constants import CC


def ASSERT_TYPE(var, a_type):
    if not isinstance(var, a_type):
        raise RuntimeError("var is type {} but should be type {}".format(type(var), a_type))


class L2GeoOpt(GeoOptimizer):
    """
    This function solves the L2 estimation problem for geography imputation. Takes DP
    measurements and finds a nearby (non-negative if nnls) possibly fractional solution.
        Inputs:
            NoisyChild: numpy multidimensional array of noisy measurements of the detailed child cells
            noisy_child_weights: float giving the coefficient for the optimization function for each NoisyChild cell
            DPqueries: a list of StackedDPquery objects (see constraints_dpqueries.py)
            query_weights: a list of floats giving the coefficient for the optimization function for each cell of each query or None
            nnls: bool if True, impose >=0 constraint on estimated counts
            backup_feas: bool if true run in backup feasibility mode
            min_schema: list of minimal schema dimensions
        Superclass Inputs:
            config: a configuration object
            grb_env: a gurobi environment object
            identifier: a string giving identifying information about the optimization (e.g. the parent geocode)
            parent: a numpy multi-array of the parent histogram (or None if no parent)
            parent_shape: the shape of the parent histogram or would be shape if parent histogram is None
            constraints: a list of StackedConstraint objects (see constraints_dpqueries.py)
            childGeoLen: int giving the number of child geographies
    """
    rounder = False
    model_name = "L2"
    parent_constraints_name = CC.PARENT_CONSTRAINTS
    backup_obj_fxn_constr_name = CC.BACKUP

    def __init__(self, *, NoisyChild=None, noisy_child_weights=None, DPqueries=None, nnls=True,
                 acceptable_l2_statuses=None, acceptable_rounder_statuses=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.nnls: bool                         = nnls
        self.NoisyChild: np.ndarray             = NoisyChild
        self.noisy_child_weights                 = noisy_child_weights
        self.DPqueries                          = DPqueries
        self.child_obj_term_wt                  = self.noisy_child_weights
        if acceptable_l2_statuses is not None:
            self.acceptable_statuses             = acceptable_l2_statuses
        #self.acceptable_statuses        = acceptable_statuses if acceptable_statuses is not None else toGRBfromStrStatus('OPTIMAL')

    def newModel(self, model_name):
        """
            Override & re-use GeoOptimizer (inherited from Optimizer) newModel() to add L2 specific params.
        """
        model = super().newModel(model_name)
        model.Params.Method             = self.getint(CC.L2_GRB_METHOD,       section=CC.GUROBI, default=-1)
        model.Params.Presolve           = self.getint(CC.L2_GRB_PRESOLVE,        section=CC.GUROBI, default=-1)
        model.Params.PreSparsify        = self.getint(CC.L2_GRB_PRESPARSIFY,     section=CC.GUROBI, default=-1)
        return model

    def getNZSubsets(self) -> Tuple[None, np.ndarray, List[int], Tuple[Union[np.ndarray, None], ...], np.ndarray, Union[None, np.ndarray]]:
        """
        Find non-zero cells in the parent histograms, return list of their numbers, mask indicating these NZs with True,
        and joint flattened parent histograms array subset to these non-zeros.
        The corresponding cells in children will be optimization variables (the others are 0, since optimization is non-negative)

        :return:
            child_floor: = None, not used by L2Opt, but is here for the uniformity
            child_sub: numpy array of children subset to the parent_mask (i.e. possible non-zeros)
            n_list: number of possible non-zeros (will become number of gurobi vars) for each histogram
            parent: list of parent arrays (1 per histogram) or Nones, for top node. For L2Opt just returns self.parent straight from the input
            parent_mask: numpy array of booleans, indicating the indexes of possible non-zeros in the joint flattened histograms array
            parent_sub: joint flattened array of parent histograms subset to parent_mask (or None-s for the top node)
        """
        n_list, parent_mask, parent_sub = self.findDesiredIndices(self.backup_feas, self.min_schema, self.parent_shape, arrays=self.parent)
        child_sub = self.NoisyChild[parent_mask, :] if self.NoisyChild is not None else None
        return None, child_sub, n_list, self.parent, parent_mask, parent_sub

    def buildMainVars(self, model, n, name="main_cells"):
        """
        Builds the primary variables for the model
        Inputs:
            model: gurobi model object
            n: int, length of first dim of variables (sum of n-s of individual histograms)
            name: str giving the variables a name
        Output:
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
        returns type -> gb.tupledict, but we can't annotate as such because gb is not loaded when this file is imported.
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        lb = 0 if self.nnls else -gb.GRB.INFINITY
        # First index is cell (among non-zero), second index is child
        two_d_vars: gb.MVar = model.addMVar((int(n), int(self.childGeoLen)), vtype=gb.GRB.CONTINUOUS, lb=lb, name=name)
        return two_d_vars

    def addDPQueriesToModel(self, model, two_d_vars, obj_fxn, parent_mask, q_set_list=None, **kwargs):
        """
        Adds the DP queries information to the objective function
        Inputs:
            model: gurobi model object
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            obj_fxn: grb expression used as obj fxn (dictionary of obj_fxns in multipass methods)
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model (applies to joint flattened histogram array)
            q_set_list: List (by histogram) over query sets which are to be added (so that they can be pre-filtered, for multipass).
                        Default is self.DPqueries
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        ASSERT_TYPE(two_d_vars, gb.MVar)
        lb = 0 if self.nnls else -gb.GRB.INFINITY
        if q_set_list is None:
            q_set_list = self.DPqueries
        for ihist, dp_queries in enumerate(q_set_list):
            if dp_queries is None:
                continue
            for st_dpq in dp_queries:
                query: MultiHistQuery = st_dpq.query
                name = st_dpq.name
                matrix_rep = query.matrixRep()
                n_ans = query.numAnswers()
                # weight = 1. / st_dpq.Var
                # Add empty columns for preceding and succeeding histograms to the matrix
                # ihist is the number of histogram, needed to pad the query matrix with zeros appropriately
                matrix_rep = self.padQueryMatrix(ihist, matrix_rep, n_ans)
                matrix_rep = matrix_rep[:, parent_mask]
                weight = ss.diags(np.hstack([np.repeat(1. / v, n_ans) for v in st_dpq.VarList]))
                dp_answer_all_children = np.hstack(st_dpq.DPanswerList).ravel()  # Stack dp_answers for children into 1 vector

                # Stack optimization vars for children into 1 vector

                try:
                    # Note: we replaced ._vararr() with .tolist(); need to validate this
                    x = gb.MVar(np.array(two_d_vars[:, st_dpq.indices].tolist()).ravel("F"))
                    # TODO: @zhura301 to validate; the assert does not work:
                    # assert x==gb.MVar(two_d_vars[:, st_dpq.indices]._vararr.ravel("F"))
                except AttributeError as e:
                    # Legacy method before .tolist() was created in gurobi 9.1
                    x = gb.MVar(two_d_vars[:, st_dpq.indices].vararr.ravel("F"))  # Stack optimization vars for children into 1 vector

                # Using "F" for Fortran style ravel indexing so that variables are in rows and answers in cols (or vice versa)
                # Make a matrix that will act on stacked variables from all children. Its dimensions are dimensions of matrix_rep,
                # multiplied by number of children on each axis, since it acts on stacked child histograms and returns stacked
                # child answers.
                # It is a block diagonal matrix with blocks being matrix_rep, or matrix_rep stacked along the
                # diagonal num_chld times.
                # We'll make it as COO matrix, which takes in 3 arrays: rows and columns of non-zero
                # elements, and their values. We perform stacking by repeating these arrays num_child times, and
                # shifting indices appropriately.
                num_chld = len(st_dpq.indices)  # How many children have this query
                Acoo = matrix_rep.tocoo()
                col = np.tile(Acoo.col, num_chld) + np.repeat(np.arange(num_chld), len(Acoo.col)) * Acoo.shape[1]
                row = np.tile(Acoo.row, num_chld) + np.repeat(np.arange(num_chld), len(Acoo.row)) * Acoo.shape[0]
                data = np.tile(Acoo.data, num_chld)
                A = ss.coo_matrix((data, (row, col)), shape=(matrix_rep.shape[0] * num_chld, matrix_rep.shape[1] * num_chld)).tocsr()
                obj_fxn = self.childDPQueryTerm(model, obj_fxn, A, x, dp_answer_all_children, weight, n_ans * num_chld, name, lb, **kwargs)
        return obj_fxn

    def childDPQueryTerm(self, model, obj_fxn, A, x, b, weight, n_ans, qname, lb, **kwargs):
        """
        This function is trivial and just calls the addObjFxnTerm, but in descendant classes can be more complicated
        """
        obj_fxn += self.addObjFxnTerm(model, A, x, b, weight, int(n_ans), f"dpq_aux_{qname}", lb)
        return obj_fxn

    @staticmethod
    def addObjFxnTerm(model, A, x, b, wt, n_aux, name, lb=0):
        """
        Calculate quadratic terms for the objective function, of the form wt * (A @ x - b)^2.
        Gurobi would square the whole (A @ x - b), exploding number of terms, so an auxiliary variable
        Ax is created, constrained to be equal to A @ x, and then entered into obj_fxn
        Returns the quadratic term as gurobi MQuadExpr
        """
        import gurobipy as gb
        Ax = model.addMVar(n_aux, vtype=gb.GRB.CONTINUOUS, lb=lb, name=name)
        model.addConstr(A @ x == Ax, name=f"{name}_cons")  # Constrain the Ax auxiliary variable to be equal to query answer
        #return wt * (Ax @ Ax - 2 * b @ Ax)                 # Quadratic obj_fxn terms for query answers: wt * (Ax - b)^2, constant dropped
        return (Ax @ wt @ Ax - 2 * b @ wt @ Ax)  # Quadratic obj_fxn terms for query answers: wt * (Ax - b)^2, constant dropped

    @staticmethod
    def buildObjFxn(two_d_vars, n_list, child_sub, obj_fxn_weight_list):
        """
        Adds squared error to the objective fxnction for the noisychild
        Inputs:
            :param n_list: List of number of non-zero variables in each histogram
            :param two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi MVar variable object
            :param child_sub: noisy children as numpy array (top node) or csr_matrix num_children X num_cells
            :param obj_fxn_weight_list: list of floats, coefficient for each cell of the objective function, one per histogram
        """
        import gurobipy as gb
        ASSERT_TYPE(two_d_vars, gb.MVar)
        obj_fxn = 0  # gb.MQuadExpr()
        if child_sub is not None:
            for ihist, weight in enumerate(zip(*obj_fxn_weight_list)):
                if np.sum(np.abs(weight)) == 0:
                    continue
                start = 0 if ihist == 0 else np.cumsum(n_list)[ihist - 1]
                end = np.cumsum(n_list)[ihist]
                # TODO: check ravel order
                w = ss.diags(np.expand_dims(np.array(weight), 1).repeat(end - start, 1).T.ravel())

                try:
                    # Note: we replaced ._vararr() with .tolist(); need to validate this
                    x = gb.MVar(np.array(two_d_vars[start:end, :].tolist()).ravel())
                    # TODO: @zhura to validate; the assert does not work
                    # assert x==gb.MVar(two_d_vars[start:end, :]._vararr.ravel())
                except AttributeError as e:
                    # Legacy method before .tolist() was created in gurobi 9.1
                    x = gb.MVar(two_d_vars[start:end, :].vararr.ravel())

                obj_fxn += (x @ w @ x - 2 * child_sub[start:end, :].ravel() @ w @ x)
        return obj_fxn

    def reformSolution(self, two_d_vars, parent_mask, child_floor=None):
        """
        Translate the variables solution back to the NoisyChild shape
        Inputs:
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi MVar variable object
            n: int, length of first dimension of two_d_vars
            child_floor: numpy multi array, floor(child), needed for rounder, None here
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model
        Outputs:
            result: list of Numpy Multiarray, The optimized solution back in the form of list of arrays with histogram shapes (times number of children)
        """
        import gurobipy as gb
        ASSERT_TYPE(two_d_vars, gb.MVar)
        joint_sol = np.zeros((int(np.sum(self.hist_sizes)), self.childGeoLen))
        joint_sol[parent_mask, :] = two_d_vars.X
        if self.nnls:
            joint_sol[joint_sol < 1e-7] = 0

        return self.splitHistograms(joint_sol)
