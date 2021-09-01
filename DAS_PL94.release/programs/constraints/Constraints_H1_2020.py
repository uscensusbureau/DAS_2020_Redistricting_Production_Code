#import programs.constraints.querybase_constraints as querybase_constraints
import programs.constraints.Constraints_H1
from constants import CC

class ConstraintsCreator(programs.constraints.Constraints_H1.ConstraintsCreator):
    """
    This class is what is used to create constraint objects from what is listed in the config file, for household schema
    see base class for keyword arguments description
    Schema: H1
    Schema dims: 2
    """
    schemaname = CC.SCHEMA_H1_2020
