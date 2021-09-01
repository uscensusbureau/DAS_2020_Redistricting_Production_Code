"""
Constraints for PL94_2020 schema.

"""
import programs.constraints.Constraints_PL94 as Constraints_PL94

from constants import CC

class ConstraintsCreator(Constraints_PL94.ConstraintsCreatorPL94Generic):
    """
        This class is what is used to create constraint objects from what is listed in the config file, for PL94_2020 schema
        see base class for keyword arguments description

        The variables in PL94 are: hhgq voting hispanic cenrace ( schema:  {'hhgq': 0, 'voting': 1, 'hispanic': 2, 'cenrace': 3} )
        With ranges: 0-7, 0-1, 0-1, 0-62 respectively
        Thus the histograms are 4-dimensional, with dimensions (8,2,2,63)

    """
    schemaname = CC.SCHEMA_PL94_2020

    def setHhgqDimsAndCaps(self):
        """
        Set dimension of hhgq axis and caps
        """
        super().setHhgqDimsAndCaps()
        # Unit table includes vacant housing units, so lower bound is 0
        self.hhgq_lb[0] = 0
