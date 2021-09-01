import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints

from constants import CC

# Master to-do list for constraints
#[] min age = 0, max age = 115
#[] hh, spouse, parent, parent-in-law, child-in-law, housemate/roommate, unmarried parter can't be 0-14 age
#[] child, adopted child, stepchild can't be >=90
#[] parent/parent-in-law can't be <=29
#[]


class ConstraintsCreator(querybase_constraints.ConstraintsCreatorwHHGQ):
    """
    This class is what is used to create constraint objects from what is listed in the config file
    Inputs:
        hist_shape: the shape of the underlying histogram
        invariants: the list of invariants (already created)
        constraint_names: the names of the constraints to be created (from the list below)
    """
    attr_hhgq = CC.ATTR_REL
    schemaname = CC.SCHEMA_SF1
    hh_cats = 15

    # Used only in unit tests
    implemented = (
            "total",
            "hhgq_total_lb",
            "hhgq_total_ub",
            # "nurse_nva_0",
            "no_refhh_under_15",
            "no_kids_over_89",
            "no_parents_under_30",
            # "no_foster_over_20",
            # "no_spouse_range",,
        )

    def setHhgqDimsAndCaps(self):
        """
        Set dimension of hhgq axis and caps
        """
        # 43 HHGQ cats minus 15 HH cats + 1 for all HH(non-GQ)
        self.gqt_dim = self.hist_shape[self.schema.dimnames.index(self.attr_hhgq)] - self.hh_cats + 1
        # ub cap on number of of people in a gq or hh
        self.hhgq_cap = np.repeat(99999, self.gqt_dim)
        #self.hhgq_cap[1] = 0
        # low bound
        self.hhgq_lb = np.repeat(1, self.gqt_dim)  # 1 person per building (TODO: we don't have vacant in the histogram, it's going to change)
        # housing units include vacant ones, so lower bound is 0
        self.hhgq_lb[0] = 0
        #self.hhgq_lb[1] = 0

    def no_parents_under_30(self, name):
        self.addToConstraintsDict(name, (CC.REL_PARENTS, CC.AGE_LT30_TOTAL),  np.array(0), "=")

    def no_kids_over_89(self, name):
        self.addToConstraintsDict(name, (CC.REL_CHILDREN, CC.AGE_GT89_TOTAL), np.array(0), "=")

    def no_refhh_under_15(self, name):
        """
        The following can't be under 15:
        'Householder (0)', 'Husband/Wife (1)', 'Father/Mother (6)', 'Parent-in-law (8)', 'Son/Daughter-in-law (9)', 'Housemate, Roommate (12)',
        'Unmarried Partner (13)'
        """
        self.addToConstraintsDict(name, (CC.REL_CANTBEKIDS, CC.AGE_LT15_TOTAL), np.array(0), "=")

    # Total population per geounit must remain invariant
    def total(self, name):
        """
        Total population per geounit must remain invariant
        """
        self.checkInvariantPresence(name, ('tot',))
        # Total means summing over all variables, hence all_axes
        self.addToConstraintsDict(name, "total", self.invariants["tot"].astype(int), "=")

    def hhgq_total_lb(self, name):
        """
        Lower bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=False)
        self.addToConstraintsDict(name, CC.HHGQ_GQLEVELS, rhs, "ge")

    def hhgq_total_ub(self, name):
        """
        Upper bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=True)
        self.addToConstraintsDict(name, CC.HHGQ_GQLEVELS, rhs, "le")

    def calculateRightHandSideHhgqTotal(self, upper):
        """
        Just calling super class for now.
        Leaving it here to attract attention to this fact.

        The calculation is as follows:
            Upper:
                Number of people in each gqhh type is <= total -(#other gq facilities),
                or has no one if there are no facilities of type
            Lower:
                Number of ppl in each gqhh type is >= number of facilities of that type,
                or is everyone if it is the only facility type

        The only constraint used in this calculation is "1 person per each facility" (i.e. number of occupied housing
        units is invariant). In this case (SF1), for GQ type 0, it will be the householder, but this does not seem to affect any of these
        calculations.
        It has different answers for when total population is kept invariant or not.
        Total number of housing units and numbers of units of each GQ type have to be kept invariant.
        """
        return super().calculateRightHandSideHhgqTotal(upper)
