from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC

class H1Attr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_H1

    @staticmethod
    def getLevels():
        return {
            "Vacant"   : [0],
            "Occupied"  : [1],
        }

    @staticmethod
    def recodeVacantCount():
        name = CC.VACANT_COUNT
        groupings = {'Vacant' : [0]}
        return name, groupings
