import programs.constraints.querybase_constraints as querybase_constraints
import programs.constraints.Constraints_Household2010
from constants import CC

class ConstraintsCreator(programs.constraints.Constraints_Household2010.ConstraintsCreatorHousehold2010Generic):
    """
    This class is what is used to create constraint objects from what is listed in the config file, for household schema
    see base class for keyword arguments description
    Schema: H1
    Schema dims: 2
    """
    schemaname = CC.SCHEMA_H1
    
    def total(self, name):
        """
        Total number of households is less than total number of housing units
        """
        self.checkInvariantPresence(name, ('tot',))
        self.addToConstraintsDict(name, "total", self.invariants["tot"].astype(int), "=")
