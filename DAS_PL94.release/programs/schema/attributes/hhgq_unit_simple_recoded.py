"""
Simplified Householder/GQType attribute for household universe schemas, with only 7 GQ levels. Doesn't have a separate level for vacant units,
those are bundled together with occupied into level 0/Housing Unit by the recoder during the reading.
For 2020 Final product, use HHGQAttr in hhgq.py for household/unit universe, and RelGQAttr in relgq.py for person universe.
"""

from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHGQUnitSimpleRecodedAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHGQ_UNIT_SIMPLE_RECODED

    @staticmethod
    def getLevels():
        return {
            "Housing Unit": [0],
            "Correctional GQ": [1],
            "Juvenile GQ": [2],
            "Nursing": [3],
            "Other Inst": [4],
            "College Dorm": [5],
            "Military": [6],
            "Other NonInst": [7]
        }

    @staticmethod
    def recodeHousingUnit():
        name = CC.HHGQ_UNIT_HOUSING
        groupings = {
            "Housing Units": [0]
        }
        return name, groupings

    @staticmethod
    def recodeHhgqVector():
        name = CC.HHGQ_UNIT_VECTOR
        return name, HHGQUnitSimpleRecodedAttr.getLevels()

    @staticmethod
    def recodeTotalGQ():
        name = CC.HHGQ_UNIT_TOTALGQ
        groupings = {
            "GQ": list(range(1,8))
        }
        return name, groupings

    @staticmethod
    def recodeHhgqSpineTypes():
        name = CC.HHGQ_SPINE_TYPES
        groupings =  {
            'GQ 101-106 Correctional facilities for adults' : [1],
            'GQ 201-203 Juvenile facilities' : [2],
            'GQ 301 Nursing facilities/Skilled-nursing facilities' : [3],
            'GQ 401-405 Other institutional facilities' : [4],
            'GQ 501 College/University student housing' : [5],
            'GQ 601-602 Military quarters' : [6],
            'GQ 701-702, 704, 706, 801-802, 900-901, 903-904 Other noninstitutional facilities' : [7]
        }
        return name, groupings


    @staticmethod
    def recodeGqlevels():
        """
        Only the GQ levels, without the household
        """
        name = CC.HHGQ_GQLEVELS
        groups = {k: v for k, v in HHGQUnitSimpleRecodedAttr.getLevels().items() if v[0] > 1}
        return name, groups
