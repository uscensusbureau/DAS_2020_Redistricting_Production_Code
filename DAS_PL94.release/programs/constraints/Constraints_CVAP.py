"""
Constraints for CVAP schema.
It doesn't need any constraints, so this just creates an instance of the generic class
without implementing any actual constraints
"""
from programs.constraints.querybase_constraints import AbstractConstraintsCreator
from constants import CC


class ConstraintsCreator(AbstractConstraintsCreator):
    """
        No actual constraints implemented, but parent class checks for histogram shapes and empty ConstraintDict class return are used.
    """
    schemaname = CC.SCHEMA_CVAP

    # This constraint is imposed in the writer. But could be imposed in the optimizer. However, raw CVAP counts are probabilistic
    # and PL94 counts are protected, so there is no guarantee the constraint even holds in the input data.
    def cvap_ub(self, name):
        """
        Upper bound on CVAP comes from PL94 tables. Experimental.
        """
        self.checkInvariantPresence(name, ('pl94counts',))
        self.addToConstraintsDict(name, "detailed", self.invariants["pl94counts"].astype(int), "le")
