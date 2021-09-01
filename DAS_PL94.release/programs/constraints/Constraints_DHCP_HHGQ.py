"""
Constraints for DHCP_HHGQ schema.
ConstraintCreator class is an actual DHCP_HHGQ constraint creator.

ConstraintCreator inherits from ConstraintsCreatorDHCGeneric the actual methods creating constraints,
but has its own __init__
"""
import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints

from constants import CC


class ConstraintsCreatorDHCGeneric(querybase_constraints.ConstraintsCreatorwHHGQ):
    """
    This class implements creation of constraints for PL94, PL94_12, 1940 (possibly, etc..)
    """
    # Used only in unit tests
    implemented = (
        "total",
        "hhgq_total_lb",
        "hhgq_total_ub",
        "nurse_nva_0",
        "hhgq1_lessthan15",
        "hhgq2_greaterthan25",
        "hhgq3_lessthan20",
        "hhgq5_lt16gt65",
        "hhgq6_lt17gt65",
    )
        
    def hhgq1_lessthan15(self,name):
        """
        Structural zero: No <15 ages in adult correctional facilities
        """
        self.addToConstraintsDict(name, (CC.HHGQ_CORRECTIONAL_TOTAL, CC.AGE_LT15_TOTAL), np.array(0), "=")

    def hhgq2_greaterthan25(self,name):
        """
        Structural zero: No >25 ages in juvenile facilities
        """
        self.addToConstraintsDict(name, (CC.HHGQ_JUVENILE_TOTAL, CC.AGE_GT25_TOTAL), np.array(0), "=")

    def hhgq3_lessthan20(self,name):
        """
        Structural zero: No <20 ages in nursing facilities
        """
        self.addToConstraintsDict(name, (CC.HHGQ_NURSING_TOTAL, CC.AGE_LT20_TOTAL), np.array(0), "=")

    #No age restrictions for other institutional facilities
    
    def hhgq5_lt16gt65(self,name):
        """
        Structural zero: No <16 or >65 ages in college housing
        """
        self.addToConstraintsDict(name, (CC.HHGQ_COLLEGE_TOTAL, CC.AGE_LT16GT65_TOTAL), np.array(0), "=")

    def hhgq6_lt17gt65(self,name):
        """
        Structural zero: No <17 or >65 ages in military housing
        """
        self.addToConstraintsDict(name, (CC.HHGQ_MILITARY_TOTAL, CC.AGE_LT17GT65_TOTAL), np.array(0), "=")

    #No age restrictions for other non-institutional facilities

    def setHhgqDimsAndCaps(self):
        """
        Set dimension of hhgq axis and caps
        """
        super().setHhgqDimsAndCaps()
        # households include vacant housing units, so lower bound is 0
        self.hhgq_lb[0] = 0

    def nurse_nva_0(self, name):
        """
        Structural zero: no minors in nursing facilities (GQ code 301)
        """
        self.addToConstraintsDict(name, (CC.HHGQ_NURSING_TOTAL, CC.NONVOTING_TOTAL), np.array(0), "=")

    def total(self, name):
        """
        Total population per geounit must remain invariant
        """
        self.checkInvariantPresence(name, ('tot',))
        self.addToConstraintsDict(name, "total", self.invariants["tot"].astype(int), "=")

    def hhgq_total_lb(self, name):
        """
        Lower bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=False)
        self.addToConstraintsDict(name, self.attr_hhgq, rhs, "ge")

    def hhgq_total_ub(self, name):
        """
        Upper bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=True)
        self.addToConstraintsDict(name, self.attr_hhgq, rhs, "le")

    
class ConstraintsCreator(ConstraintsCreatorDHCGeneric):
    """
        This class is what is used to create constraint objects from what is listed in the config file, for PL94 schema
        see base class for keyword arguments description

        The variables in DHCP_HHGQ are: hhgq voting hispanic cenrace ( schema:  {'hhgq': 0, 'sex': 1, 'age': 2,  'hispanic': 3, 'cenrace': 4, 'citizen': 5} )
        With ranges: 0-7, 0-1, 0-115, 0-1, 0-62, 0-1 respectively
        Thus the histograms are 6-dimensional, with dimensions (8,2,116,2,63,2)
    """
    schemaname = CC.SCHEMA_REDUCED_DHCP_HHGQ
