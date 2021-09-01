from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC

class TenureAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_TENURE

    @staticmethod
    def getLevels():
        return {
            "Mortgage": [0],
            "Owned"   : [1],
            "Rented"  : [2],
            "No Pay"  : [3]
        }
