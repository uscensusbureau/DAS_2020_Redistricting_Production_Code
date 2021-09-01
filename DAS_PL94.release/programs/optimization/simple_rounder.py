import numpy as np

from constants import CC
from programs.optimization.geo_optimizers import ASSERT_TYPE
from programs.optimization.optimizer import GeoOptimizer


class GeoRound(GeoOptimizer):
    """
        This class solves the integer rounding problem for geography imputation. Takes a
        non-negative possibly fractional solution and finds a nearby non-negative integer solution.
        Inputs:
            child: a numpy array that is a non-negative solution to the L2 problem
            backup_feas: bool, if true run in backup feasibility mode
            min_schema: list giving dimensions of minimal schema or None
    """

    # variables used in run() function
    rounder = True
    model_name = "Rounder"
    parent_constraints_name = CC.PARENT_CONSTRAINTS_ROUND
    backup_obj_fxn_constr_name = CC.ROUND_BACKUP
    child_obj_term_wt = None

    def __init__(self, *, child, acceptable_rounder_statuses, **kwargs):
        super().__init__(**kwargs)
        self.child = child
        self.acceptable_statuses = acceptable_rounder_statuses

    def getNZSubsets(self):
        """
           Calculate the child_floor, child_leftover, and parent_diff, i.e. the part of parent histograms which is leftover of what floored
           children sum to
           Outputs:
               child_floor: numpy multi array, floor(child)
               child_sub: child_leftover: numpy multi array, child-child_floor, subset to parent_mask (where the possible NZ are)
               n_list: how many possible non zeros (=> gurobi variables) in each histogram (and each children)
               parent_diff: numpy multi array, parent - sum of child_floor across geographies
               parent_mask: where the possible non-zeros are
               parent_diff_sub: parent_diff subset to the parent_mask
        """
        # Flatten each child histogram and stack them together into a joint array
        child = np.vstack([c.reshape(np.prod(ps), self.childGeoLen) for c, ps in zip(self.child, self.parent_shape)])
        child_floor = np.floor(child)
        child_leftover = child - child_floor

        if self.parent[0] is not None:
            # Flatten each parent histogram and stack them together into a joint array, and substract the sum of floored children
            parent_diff = np.hstack([parent.reshape(np.prod(ps)) for parent, ps in zip(self.parent, self.parent_shape)]) - np.sum(child_floor, len(child.shape) - 1)
            # Reshape back, since it's assumed it's a list in findDesiredIndices and backup_feas (where it's also assumed to have multishape, not flattened)
            parent_diff = [parent_diff[hstart:hend].reshape(shape) for hstart, hend, shape in zip(self.hstarts, self.hends, self.parent_shape)]
        else:
            parent_diff = [None] * len(self.parent)

        n_list, parent_mask, parent_diff_sub = self.findDesiredIndices(self.backup_feas, self.min_schema, self.parent_shape, arrays=parent_diff)

        child_sub = child_leftover[parent_mask, :]

        return child_floor, child_sub, n_list, parent_diff, parent_mask, parent_diff_sub

    def buildMainVars(self, model, n, name="main_cells"):
        """
        Builds the main variables for the models
        Inputs:
            model: gurobi model objects
            n: int, length of first dimension of variables
            name: str, give the variables a name
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        # First index is cell (among non-zero), second index is child
        two_d_vars: gb.MVar = model.addMVar((int(n), int(self.childGeoLen)), vtype=gb.GRB.BINARY, lb=0.0, ub=1.0, name=name)
        return two_d_vars

    @staticmethod
    def buildObjFxn(two_d_vars, n_list, child_sub, obj_fxn_weight_list):
        return GeoRound.buildCellwiseObjFxn(two_d_vars, child_sub)

    @staticmethod
    def buildCellwiseObjFxn(two_d_vars, child_sub):
        """
        Adds the main objective function for the rounder
        Inputs:
            obj_fxn: gurobi expression for the objective function
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi MVar variable object
            child_sub: two-d numpy multi array, child-child_floor subset to mask
            n_list: list of number of gurobi vars in each histogram
        """
        import gurobipy as gb
        ASSERT_TYPE(two_d_vars, gb.MVar)
        obj_fxn = 0  # gb.MLinExpr()
        for child_num in range(two_d_vars.shape[1]):
            # |var - leftover| = {leftover if var=0; 1-leftover if var=1} = leftover*(1-var) + (1-leftover)*var =
            # = leftover + var - 2*leftover*var = leftover + (1-2*leftover)*var = (1-2*leftover)*var + const; const dropped
            obj_fxn += (1 - 2 * child_sub[:, child_num]) @ two_d_vars[:, child_num]
        return obj_fxn

    def reformSolution(self, two_d_vars, parent_mask: np.ndarray, child_floor=None):
        """
        Take the solution from the model and turn it back into the correctly shaped numpy multi array
        Inputs:
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi MVar variable object
            n: int, length of first dimension of two_d_vars
            child_floor: numpy multi array, floor(child)
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model
        Outputs:
            result: list of Numpy Multiarray, The optimized solution back in the form of list of arrays with histogram shapes (times number of children)
        """
        import gurobipy as gb
        ASSERT_TYPE(two_d_vars, gb.MVar)
        joint_result = child_floor.reshape((int(np.sum(self.hist_sizes)), self.childGeoLen))
        if two_d_vars.shape[0] > 0:
            joint_result[parent_mask, :] = joint_result[parent_mask, :] + two_d_vars.X
        joint_result = joint_result.astype(int)

        return self.splitHistograms(joint_result)