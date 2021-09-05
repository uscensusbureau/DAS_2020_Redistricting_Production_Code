from programs.constraints.Constraints_Household2010 import ConstraintsCreatorHousehold2010Generic

from constants import CC

class ConstraintsCreator(ConstraintsCreatorHousehold2010Generic):
    """
       This class is what is used to create constraint objects from what is listed in the config file, for household schema
       see base class for keyword arguments description
       Schema: sex, age, hisp, race, size, hhtype, elderly, multi
       Schema dims: 2, 9, 2, 7, 8, 24, 4, 2
       """

    schemaname = CC.SCHEMA_HOUSEHOLD2010_TENVACS
