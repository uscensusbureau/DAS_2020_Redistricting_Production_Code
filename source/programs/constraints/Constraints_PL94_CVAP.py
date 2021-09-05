"""
Constraints for PL94_CVAP schema.
ConstraintCreator class is an actual PL94_CVAP constraint creator.

ConstraintCreator inherits from ConstraintsCreatorPL94Generic the actual methods creating constraints,
but has its own __init__
"""
import programs.constraints.Constraints_PL94 as Constraints_PL94
from constants import CC


class ConstraintsCreator(Constraints_PL94.ConstraintsCreatorPL94Generic):
    """
        This class is what is used to create constraint objects from what is listed in the config file, for PL94_CVAP schema
        see base class for keyword arguments description

        The variables in PL94 are: hhgq voting hispanic cenrace ( schema:  {'hhgq': 0, 'voting': 1, 'hispanic': 2, 'cenrace': 3, 'citizen': 4} )
        With ranges: 0-7, 0-1, 0-1, 0-62, 0-1 respectively
        Thus the histograms are 4-dimensional, with dimensions (8,2,2,63,2)
    """
    schemaname = CC.SCHEMA_PL94_CVAP
