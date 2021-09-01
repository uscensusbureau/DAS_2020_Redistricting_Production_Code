import numpy as np
from programs.constraints.Constraints_Household2010 import ConstraintsCreatorHousehold2010Generic

from constants import CC

class ConstraintsCreator(ConstraintsCreatorHousehold2010Generic):
    """
       This class is what is used to create constraint objects from what is listed in the config file, for household schema
       see base class for keyword arguments description
       Schema: sex, age, hisp, race, size, hhtype, elderly, multi
       Schema dims: 2, 9, 2, 7, 8, 24, 4, 2
       """

    schemaname = CC.SCHEMA_HOUSEHOLDSMALL



    # def tot_hu(self, name):
    #     """
    #     Total number of housing units (tenured + vacant) in the units table is invariant
    #     """
    #     self.checkInvariantPresence(name, ('tot_hu',))
    #     self.addUnitConstraintsToDict(name, CC.HHGQ_UNIT_HOUSING, self.invariants["tot_hu"].astype(int), "=")



    def hhsex2tenure4test(self, name):
        main_query = self.schema.getQuery(CC.SEX_VECT)
        unit_query = self.unit_schema.getQuery(CC.TEN_2LEV)
        self.addToConstraintsDictMultiHist(name, (main_query, unit_query), (1, -1), np.array([0,0]), "=")
