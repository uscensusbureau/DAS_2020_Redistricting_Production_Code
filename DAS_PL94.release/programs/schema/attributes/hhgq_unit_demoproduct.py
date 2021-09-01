"""
Attribute for unit GQ levels, used in the household univers in the Demo Product. Outdated.
For 2020 Final product, use HHGQAttr in hhgq.py
"""

from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHGQUnitDemoProductAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHGQ_UNIT

    # THIS CLASS IS OUTDATED, WAS ONLY USED FOR THE DEMO PRODUCT. NOW THE SAME HHGQ LEVELS ARE USED FOR BOTH PERSON AND HOUSEHOLD UNIVERSES
    # For 2020 Final product, use HHGQAttr in hhgq.py

    @staticmethod
    def getLevels():
        return {
            'Occupied': [0],
            'Vacant': [1],
            'Federal detention centers': [2],
            'Federal prisons': [3],
            'State prisons': [4],
            'Local jails and other municipal confinement facilities': [5],
            'Correctional residential facilities': [6],
            'Military disciplinary barracks and jails': [7],
            'Group homes for juveniles (non-correctional)': [8],
            'Residential treatment centers for juveniles (non-correctional)': [9],
            'Correctional facilities intended for juveniles': [10],
            'Nursing facilities/skilled-nursing facilities': [11],
            'Mental (psychiatric) hospitals and psychiatric units in other hospitals': [12],
            'Hospitals with patients who have no usual home elsewhere': [13],
            'In-patient hospice facilities': [14],
            'Military treatment facilities with assigned patients': [15],
            'Residential schools for people with disabilities': [16],
            'College/university student housing': [17],
            'Military quarters': [18],
            'Military ships': [19],
            'Emergency and transitional shelters (with sleeping facilities) for people experiencing homelessness': [20],
            'Soup kitchens': [21],
            'Regularly scheduled mobile food vans': [22],
            'Targeted non-sheltered outdoor locations': [23],
            'Group homes intended for adults': [24],
            'Residential treatment centers for adults': [25],
            'Maritime/merchant vessels': [26],
            'Workers’ group living quarters and job corps centers': [27],
            'Living quarters for victims of natural disasters': [28],
            'Religious group quarters and domestic violence shelters': [29]
        }

    @staticmethod
    def recodeHousingUnit():
        name = CC.HHGQ_UNIT_HOUSING
        groupings = {
                "Housing Units": [0, 1]
        }
        return name, groupings

    @staticmethod
    def recodeHhgqVector():
        name = CC.HHGQ_UNIT_VECTOR
        hhgq_levels = HHGQUnitDemoProductAttr.getLevels()
        groupings = HHGQUnitDemoProductAttr.getHhgqVectorGroupings(hhgq_levels)
        return name, groupings

    @staticmethod
    def getHhgqVectorGroupings(hhgq_levels):
        groupings = {
            "Housing Units": [0, 1],
        }
        # Add the reverse_level_dict starting from 3rd entry (number 2, after 0 and 1 that are housing units), reversing it back
        groupings.update(HHGQUnitDemoProductAttr.getGqVectGropings(hhgq_levels))
        return groupings

    @staticmethod
    def recodeTotalGQ():
        name = CC.HHGQ_UNIT_TOTALGQ
        groupings = {
            "Group Quarters": list(range(2, len(HHGQUnitDemoProductAttr.getLevels())))
        }
        return name, groupings

    @staticmethod
    def getGqVectGropings(hhgq_levels):
        return {k: v for k, v in hhgq_levels.items() if v[0] > 1}

    @staticmethod
    def recodeGqlevels():
        """
        Only the GQ levels, without the household
        """
        name = CC.HHGQ_GQLEVELS
        groups = HHGQUnitDemoProductAttr.getGqVectGropings(HHGQUnitDemoProductAttr.getLevels())
        return name, groups

    @staticmethod
    def recodeVacancy():
        """
        Should really be in vacancy attr, but HHGQ=1 is actually vacant units
        """
        name = CC.VACANT
        groupings = {
            "Vacant": [1]
        }
        return name, groupings


    # This are recodes from CEF/MDF code to our internal schema code for GQTYPE. Used in reader. This and its reverse are temporarily hanging out here,
    # until a better place is found (maybe as a schema recode?)
    reader_recode = {
        "101": 2,
        "102": 3,
        "103": 4,
        "104": 5,
        "105": 6,
        "106": 7,
        "201": 8,
        "202": 9,
        "203": 10,
        "301": 11,
        "401": 12,
        "402": 13,
        "403": 14,
        "404": 15,
        "405": 16,
        "501": 17,
        "601": 18,
        "602": 19,
        "701": 20,
        "702": 21,
        "704": 22,
        "706": 23,
        "801": 24,
        "802": 25,
        "900": 26,
        "901": 27,
        "903": 28,
        "904": 29,
    }

    # This reverses the recode above, used in MDF2020Writer.
    mdf_recode = {(schema_code - 1): mdf_code for mdf_code, schema_code in reader_recode.items()}

    @staticmethod
    def cef2das(gqtype):
        try:
            return HHGQUnitDemoProductAttr.reader_recode[gqtype]
        except KeyError:
            raise ValueError(f"GQTYPE value of '{gqtype}' is not supported in {HHGQUnitDemoProductAttr.getName()} hhgq reader recode", gqtype)

    @staticmethod
    def das2mdf(schema_code):
        return HHGQUnitDemoProductAttr.mdf_recode[schema_code]