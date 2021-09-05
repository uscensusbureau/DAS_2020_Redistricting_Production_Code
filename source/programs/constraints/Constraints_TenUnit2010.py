import programs.constraints.querybase_constraints as querybase_constraints
import programs.constraints.Constraints_Household2010
from constants import CC

class ConstraintsCreator(programs.constraints.Constraints_Household2010.ConstraintsCreatorHousehold2010Generic):
    """
    This class is what is used to create constraint objects from what is listed in the config file, for household schema
    see base class for keyword arguments description
    Schema: sex, age, hisp, race, size, hhtype, elderly, multi, tenure
    Schema dims: 2, 9, 2, 7, 8, 24, 4, 2, 4
    """
    schemaname = CC.SCHEMA_TEN_UNIT_2010
