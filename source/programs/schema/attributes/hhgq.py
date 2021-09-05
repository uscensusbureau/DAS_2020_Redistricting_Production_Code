"""
Main and the most current class for Household/GQType unit types, to be used in final 2020 product.
The GQ levels correspond (i.e. are identical) to GQ part of the levels of RelGQAttr in relgq.py
"""

from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHGQAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHGQ

    @staticmethod
    def getLevels():
        return {
            'Household': [0],
            'GQ 101 Federal detention centers': [1],
            'GQ 102 Federal prisons': [2],
            'GQ 103 State prisons': [3],
            'GQ 104 Local jails and other municipal confinements': [4],
            'GQ 105 Correctional residential facilities': [5],
            'GQ 106 Military disciplinary barracks': [6],
            'GQ 201 Group homes for juveniles': [7],
            'GQ 202 Residential treatment centers for juveniles': [8],
            'GQ 203 Correctional facilities intended for juveniles': [9],
            'GQ 301 Nursing facilities': [10],
            'GQ 401 Mental hospitals': [11],
            'GQ 402 Hospitals with patients who have no usual home elsewhere': [12],
            'GQ 403 In-patient hospice facilities': [13],
            'GQ 404 Military treatment facilities': [14],
            'GQ 405 Residential schools for people with disabilities': [15],
            'GQ 501 College/university student housing': [16],
            'GQ 601 Military quarters': [17],
            'GQ 602 Military ships': [18],
            'GQ 701 Emergency and transitional shelters': [19],
            'GQ 801 Group homes intended for adults': [20],
            'GQ 802 Residential treatment centers for adults': [21],
            'GQ 900 Maritime/merchant vessels': [22],
            "GQ 901 Workers' group living quarters and job corps centers": [23],
            'GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)': [24]
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
        groupings = HHGQAttr.getLevels()
        return name, groupings

    # @staticmethod
    # def getHhgqVectorGroupings(hhgq_levels):
    #     groupings = {
    #         "Housing Units": [0],
    #     }
    #     # Add the reverse_level_dict starting from 3rd entry (number 2, after 0 and 1 that are housing units), reversing it back
    #     groupings.update(HHGQAttr.getGqVectGropings(hhgq_levels))
    #     return groupings

    @staticmethod
    def recodeTotalGQ():
        name = CC.HHGQ_UNIT_TOTALGQ
        groupings = {
            "Group Quarters": list(range(1, len(HHGQAttr.getLevels())))
        }
        return name, groupings

    # @staticmethod
    # def getGqVectGropings(hhgq_levels):
    #     return {k: v for k, v in hhgq_levels.items() if v[0] > 1}

    @staticmethod
    def recodeHhgqMajorTypes():
        """
            For the PL94 workload, the inner cells are formed from the 7 major types.
        """
        name = CC.HHGQ_MAJOR_TYPES
        groupings = {
            'GQ 101-106 Correctional facilities for adults' : list(range(1,6+1)),
            'GQ 201-203 Juvenile facilities' : list(range(7,9+1)),
            'GQ 301 Nursing facilities/Skilled-nursing facilities' : [10],
            'GQ 401-405 Other institutional facilities' : list(range(11,15+1)),
            'GQ 501 College/University student housing' : [16],
            'GQ 601-602 Military quarters' : [17,18],
            'GQ 701-702, 704, 706, 801-802, 900-901, 903-904 Other noninstitutional facilities' : [19, 20, 21, 22, 23, 24]
        }
        return name, groupings

    @staticmethod
    def recodeHhgqSpineTypes():
        """
            The currently used GQ types in the geographic spine optimization routines are the 7 major GQ types.
        """
        name = CC.HHGQ_SPINE_TYPES
        groupings = {
            'GQ 101-106 Correctional facilities for adults' : list(range(1,6+1)),
            'GQ 201-203 Juvenile facilities' : list(range(7,9+1)),
            'GQ 301 Nursing facilities/Skilled-nursing facilities' : [10],
            'GQ 401-405 Other institutional facilities' : list(range(11,15+1)),
            'GQ 501 College/University student housing' : [16],
            'GQ 601-602 Military quarters' : [17,18],
            'GQ 701-702, 704, 706, 801-802, 900-901, 903-904 Other noninstitutional facilities' : [19, 20, 21, 22, 23, 24]
        }
        return name, groupings

    @staticmethod
    def recodeHhgqInstNoninst():
        """
            For the PL94 workload, the middle marginals are Institutionalized, Non-institutionalized.
        """
        name = CC.HHGQ_INST_NONINST
        groupings = {
                'GQ 101-106, 201-203, 301, 405-105 Institutionalized population' : list(range(1,15+1)),
                'GQ 501, 601-602, 701-702, 704, 706, 801-802, 900-901, 903-904 Noninstitutionalized population' : list(range(16,24+1)),
        }
        return name, groupings

    @staticmethod
    def recodeGqlevels():
        """
        Only the GQ levels, without the household
        """
        name = CC.HHGQ_GQLEVELS
        groupings = {k: v for k, v in HHGQAttr.getLevels().items() if v[0] > 0}
        # groupings = HHGQAttr.getGqVectGropings(HHGQAttr.getLevels())
        return name, groupings

    # This are recodes from CEF/MDF code to our internal schema code for GQTYPE. Used in reader. This and its reverse are temporarily hanging out here,
    # until a better place is found (maybe as a schema recode?)
    reader_recode = {
        "101": 1,
        "102": 2,
        "103": 3,
        "104": 4,
        "105": 5,
        "106": 6,
        "201": 7,
        "202": 8,
        "203": 9,
        "301": 10,
        "401": 11,
        "402": 12,
        "403": 13,
        "404": 14,
        "405": 15,
        "501": 16,
        "601": 17,
        "602": 18,
        "701": 19,
        "702": 24,  # combined into GQ 997
        "704": 24,  # combined into GQ 997
        "706": 24,  # combined into GQ 997
        "801": 20,
        "802": 21,
        "900": 22,
        "901": 23,
        "903": 24,  # combined into GQ 997
        "904": 24,  # combined into GQ 997
        "997": 24,  # just in case this 3-digit code exists, associate it with the GQ 997 index
    }

    # This reverses the recode above, used in MDF2020Writer.
    # TODO: Not going to work, now that there isn't 1-to-1 correspondence
    mdf_recode = {(schema_code - 1): mdf_code for mdf_code, schema_code in reader_recode.items()}

    @staticmethod
    def cef2das(gqtype):
        try:
            return HHGQAttr.reader_recode[gqtype]
        except KeyError:
            raise ValueError(f"GQTYPE value of '{gqtype}' is not supported in {HHGQAttr.getName()} hhgq reader recode", gqtype)

    @staticmethod
    def das2mdf(schema_code):
        return HHGQAttr.mdf_recode[schema_code]
