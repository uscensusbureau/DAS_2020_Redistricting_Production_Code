"""
Simplified Householder/GQType attribute for household universe schemas, with only 7 GQ levels. Has a level for vacant units.
For 2020 Final product, use HHGQAttr in hhgq.py for household/unit universe, and RelGQAttr in relgq.py for person universe.
"""
from programs.schema.attributes.hhgq_unit_demoproduct import HHGQUnitDemoProductAttr
from constants import CC


class HHGQUnitSimpleAttr(HHGQUnitDemoProductAttr):

    @staticmethod
    def getName():
        return CC.ATTR_HHGQ_UNIT_SIMPLE

    @staticmethod
    def getLevels():
        return {
            'Occupied'  : [0],
            'Vacant': [1],
            "Correctional GQ": [2],
            "Juvenile GQ": [3],
            "Nursing": [4],
            "Other Inst": [5],
            "College Dorm": [6],
            "Military": [7],
            "Other NonInst": [8]
        }

    @staticmethod
    def recodeHhgqVector():
        name = CC.HHGQ_UNIT_VECTOR
        hhgq_levels = HHGQUnitSimpleAttr.getLevels()
        groupings = HHGQUnitSimpleAttr.getHhgqVectorGroupings(hhgq_levels)
        return name, groupings

    @staticmethod
    def recodeGqlevels():
        """
        Only the GQ levels, without the household
        """
        name = CC.HHGQ_GQLEVELS
        groups = HHGQUnitSimpleAttr.getGqVectGropings(HHGQUnitSimpleAttr.getLevels())
        return name, groups