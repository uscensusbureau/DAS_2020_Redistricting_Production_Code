from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC

class HHTenShortAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHTENSHORT

    @staticmethod
    def getLevels():
        return {
            "Owned"   : [0],
            "Rented"  : [1],
        }

    @staticmethod
    def recodeTenure2Levels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = CC.TEN_2LEV
        groups = {
            "Owned": [0],
            "Not owned": [1]
        }
        return name, groups
