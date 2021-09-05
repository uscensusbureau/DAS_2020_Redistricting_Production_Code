from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC

class VacancyAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_VACS

    @staticmethod
    def getLevels():
        return {

            "Not vacant": [0],
            "Vacant, for rent": [1],
            "Vacant, rented, not occupied": [2],
            "Vacant, for sale only Pay": [3],
            "Vacant, sold, not occupied": [4],
            "Vacant, for seasonal, recreational, or occasional use": [5],
            "Vacant, for migrant workers": [6],
            "Vacant, other": [7],

        }

    @staticmethod
    def recodeVacant():
        """
        collapses the VACS dimension to 2 levels (occupied and vacant) by
        grouping together the 7 vacancy types)
        """
        name = CC.VACANT
        groups = {
            "Occupied": [0],
            "Vacant": list(range(1, 8))
        }
        return name, groups
