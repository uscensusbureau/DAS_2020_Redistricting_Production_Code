
## This module is problematic, because it should not import gurobipy when read.
##

from operator import le, ge, eq

def toGRBFromStr():
    """ Module for program-wide constant maps (e.g., dicts that should never change) """
    import gurobipy as gb
    return {"=": gb.GRB.EQUAL,
            "le": gb.GRB.LESS_EQUAL,
            "ge": gb.GRB.GREATER_EQUAL}


def OperatorFromStr():
    """ Module for program-wide constant maps (e.g., dicts that should never change) """
    return {"=": eq,
            "le": le,
            "ge": ge}


def toGRBfromStrStatus(str_status):
    import gurobipy as gb
    return {
        'OPTIMAL': gb.GRB.OPTIMAL,
        'SUBOPTIMAL': gb.GRB.SUBOPTIMAL,
        'LOADED': gb.GRB.LOADED,
        'INFEASIBLE': gb.GRB.INFEASIBLE,
        'INF_OR_UNBD': gb.GRB.INF_OR_UNBD,
        'UNBOUNDED': gb.GRB.UNBOUNDED,
        'CUTOFF': gb.GRB.CUTOFF,
        'ITERATION_LIMIT': gb.GRB.ITERATION_LIMIT,
        'NODE_LIMIT': gb.GRB.NODE_LIMIT,
        'TIME_LIMIT': gb.GRB.TIME_LIMIT,
        'SOLUTION_LIMIT': gb.GRB.SOLUTION_LIMIT,
        'INTERRUPTED': gb.GRB.INTERRUPTED,
        'NUMERIC': gb.GRB.NUMERIC,
        'INPROGRESS': gb.GRB.INPROGRESS,
        'USER_OBJ_LIMIT': gb.GRB.USER_OBJ_LIMIT,
    }[str_status]
