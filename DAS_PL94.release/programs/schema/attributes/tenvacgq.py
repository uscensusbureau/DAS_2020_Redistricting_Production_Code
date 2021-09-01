"""
Class stacking attributes Tenure, Vacancy and HHGQ.
Or, HHGQ expanded into more levels:
0 (household) is expanded in types of tenure
1 (vacant) is expanded to types of vacancy
"Not vacant" and "Not a GQ" levels are, therefore, removed.
"""

from programs.schema.attributes.abstractattribute import AbstractAttribute
from programs.schema.attributes.hhgq import HHGQAttr
from programs.schema.attributes.vacs import VacancyAttr
from programs.schema.attributes.tenure import TenureAttr

from constants import CC


class TenureVacancyGQAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_TENVACGQ

    @staticmethod
    def getTVGQLevels():
        """ Get levels of the Tenure, Vacancy and HHGQ attributes, which are being stacked to produce this one"""
        tenure_levels = [key for key in TenureAttr.getLevels()]
        vacs_levels = [key for key in VacancyAttr.getLevels()][1:]  # Not getting level 0 which is "Non Vacant", those are accounted for by Tenure and HHGQ
        gq_levels = [key for key in HHGQAttr.getLevels()][1:]  # Not getting level 0 which is "Household", those are accounted for by Tenure
        return gq_levels, tenure_levels, vacs_levels

    @staticmethod
    def getLevels():
        gq, ten, vac = TenureVacancyGQAttr.getTVGQLevels()

        keys = ten + vac + gq

        return {key: [num] for num, key in enumerate(keys)}

        # # The following is produced by running the above
        # return {
        #     'Mortgage': [0],
        #     'Owned': [1],
        #     'Rented': [2],
        #     'No Pay': [3],
        #     'Vacant, for rent': [4],
        #     'Vacant, rented, not occupied': [5],
        #     'Vacant, for sale only Pay': [6],
        #     'Vacant, sold, not occupied': [7],
        #     'Vacant, for seasonal, recreational, or occasional use': [8],
        #     'Vacant, for migrant workers': [9],
        #     'Vacant, other': [10],
        #     'GQ 101 Federal detention centers': [11],
        #     'GQ 102 Federal prisons': [12],
        #     'GQ 103 State prisons': [13],
        #     'GQ 104 Local jails and other municipal confinements': [14],
        #     'GQ 105 Correctional residential facilities': [15],
        #     'GQ 106 Military disciplinary barracks': [16],
        #     'GQ 201 Group homes for juveniles': [17],
        #     'GQ 202 Residential treatment centers for juveniles': [18],
        #     'GQ 203 Correctional facilities intended for juveniles': [19],
        #     'GQ 301 Nursing facilities': [20],
        #     'GQ 401 Mental hospitals': [21],
        #     'GQ 402 Hospitals with patients who have no usual home elsewhere': [22],
        #     'GQ 403 In-patient hospice facilities': [23],
        #     'GQ 404 Military treatment facilities': [24],
        #     'GQ 405 Residential schools for people with disabilities': [25],
        #     'GQ 501 College/university student housing': [26],
        #     'GQ 601 Military quarters': [27],
        #     'GQ 602 Military ships': [28],
        #     'GQ 701 Emergency and transitional shelters': [29],
        #     'GQ 801 Group homes intended for adults': [30],
        #     'GQ 802 Residential treatment centers for adults': [31],
        #     'GQ 900 Maritime/merchant vessels': [32],
        #     "GQ 901 Workers' group living quarters and job corps centers": [33],
        #     'GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)': [34]
        # }

    @staticmethod
    def recodeHhgqSpineTypes():
        """
            The currently used GQ types in the geographic spine optimization routines are the 7 major GQ types.
        """
        name = CC.HHGQ_SPINE_TYPES
        groupings = {
            'GQ 101-106 Correctional facilities for adults' : list(range(11, 16+1)),
            'GQ 201-203 Juvenile facilities' : list(range(17, 19+1)),
            'GQ 301 Nursing facilities/Skilled-nursing facilities' : [20],
            'GQ 401-405 Other institutional facilities' : list(range(21, 25+1)),
            'GQ 501 College/University student housing' : [26],
            'GQ 601-602 Military quarters' : [27, 28],
            'GQ 701-702, 704, 706, 801-802, 900-901, 903-904 Other noninstitutional facilities' : list(range(29, 34+1))
        }
        return name, groupings

    @staticmethod
    def recodeVacant():
        """
        returns number of vacant units
        """
        name = CC.VACANT
        # groups = {
        #     "Vacant": list(range(4, 11))
        # }
        gq, ten, vac = TenureVacancyGQAttr.getTVGQLevels()
        groups = {
            "Vacant": range(len(ten), len(ten) + len(vac))  # Start from where tenure levels end, end where vacancy levels end
        }
        return name, groups
        # ('vacant', {'Vacant': [4, 5, 6, 7, 8, 9, 10]})

    @staticmethod
    def recodeHouseholds():
        """
        returns number of household units
        """
        name = CC.HHGQ_HOUSEHOLD_TOTAL
        gq, ten, vac = TenureVacancyGQAttr.getTVGQLevels()
        groups = {
            "Households": range(len(ten))  # Cover all tenure levels
        }
        return name, groups
        # ('householdTotal', {'Households': [0, 1, 2, 3]})

    @staticmethod
    def recodeOwned():
        """
        Returns number of owned units (i.e. mortgage or owned, but not rented or no pay)
        """
        name = CC.TEN_OWNED
        groups = {
            "Owned": [0, 1]  # Cover first two tenure levels
        }
        return name, groups

    @staticmethod
    def recodeTenure2Levels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = CC.TEN_2LEV
        groups = {
            "Owned": [0, 1],
            "Not owned": [2, 3]
        }
        return name, groups

    @staticmethod
    def recodeTenureLevels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = CC.ATTR_TENURE
        gq, ten, vac = TenureVacancyGQAttr.getTVGQLevels()
        groups = {k: v for k, v in TenureVacancyGQAttr.getLevels().items() if v[0] < len(ten)}
        return name, groups
        # ('tenure', {'Mortgage': [0], 'Owned': [1], 'Rented': [2], 'No Pay': [3]})


    @staticmethod
    def recodeVacancyLevels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = CC.ATTR_VACS
        gq, ten, vac = TenureVacancyGQAttr.getTVGQLevels()
        groups = {k: v for k, v in TenureVacancyGQAttr.getLevels().items() if len(ten) <= v[0] < len(ten) + len(vac)}
        return name, groups
        # ('vacs',
        #  {'Vacant, for rent': [4],
        #   'Vacant, rented, not occupied': [5],
        #   'Vacant, for sale only Pay': [6],
        #   'Vacant, sold, not occupied': [7],
        #   'Vacant, for seasonal, recreational, or occasional use': [8],
        #   'Vacant, for migrant workers': [9],
        #   'Vacant, other': [10]})

    @staticmethod
    def recodeHousingUnit():
        name = CC.HHGQ_UNIT_HOUSING
        # groupings = {
        #     "Housing Units": list(range(11))
        # }
        gq, ten, vac = TenureVacancyGQAttr.getTVGQLevels()
        groupings = {
            "Housing Units": list(range(len(ten) + len(vac)))
        }
        return name, groupings
        # ('housing_units', {'Housing Units': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})

    @staticmethod
    def recodeTotalGQ():
        name = CC.HHGQ_UNIT_TOTALGQ
        # groupings = {
        #     "Group Quarters": list(range(11, 35))
        # }
        gq, ten, vac = TenureVacancyGQAttr.getTVGQLevels()
        groupings = {
            "Group Quarters": list(range(len(ten) + len(vac), len(ten) + len(vac) + len(gq)))
        }
        return name, groupings
        # ('total_gq',{'Group Quarters': [11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34]})

    @staticmethod
    def recodeHhgqVector():
        name = CC.HHGQ_UNIT_VECTOR
        groupings = TenureVacancyGQAttr.getGqVectGropings()
        return name, groupings
        # ('hhgq_vector',
        #  {'Housing Units': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        #   'GQ 101 Federal detention centers': [11],
        #   'GQ 102 Federal prisons': [12],
        #   'GQ 103 State prisons': [13],
        #   'GQ 104 Local jails and other municipal confinements': [14],
        #   'GQ 105 Correctional residential facilities': [15],
        #   'GQ 106 Military disciplinary barracks': [16],
        #   'GQ 201 Group homes for juveniles': [17],
        #   'GQ 202 Residential treatment centers for juveniles': [18],
        #   'GQ 203 Correctional facilities intended for juveniles': [19],
        #   'GQ 301 Nursing facilities': [20],
        #   'GQ 401 Mental hospitals': [21],
        #   'GQ 402 Hospitals with patients who have no usual home elsewhere': [22],
        #   'GQ 403 In-patient hospice facilities': [23],
        #   'GQ 404 Military treatment facilities': [24],
        #   'GQ 405 Residential schools for people with disabilities': [25],
        #   'GQ 501 College/university student housing': [26],
        #   'GQ 601 Military quarters': [27],
        #   'GQ 602 Military ships': [28],
        #   'GQ 701 Emergency and transitional shelters': [29],
        #   'GQ 801 Group homes intended for adults': [30],
        #   'GQ 802 Residential treatment centers for adults': [31],
        #   'GQ 900 Maritime/merchant vessels': [32],
        #   "GQ 901 Workers' group living quarters and job corps centers": [33],
        #   'GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)': [34]})

    @staticmethod
    def getGqVectGropings():
        gq, ten, vac = TenureVacancyGQAttr.getTVGQLevels()
        groupings = {
            "Housing Units": list(range(len(ten) + len(vac))),
        }
        groupings.update({k: v for k, v in TenureVacancyGQAttr.getLevels().items() if v[0] >= len(ten) + len(vac)})
        return groupings

    @staticmethod
    def cef2das(tenure, vac, gqtype):
        g, t, v = TenureVacancyGQAttr.getTVGQLevels()
        if gqtype == "000" or gqtype == "   ":
            if int(vac) == 0:
                if int(tenure) == 0:
                    raise ValueError("Tenure not set for non-vacant unit")
                return int(tenure) - 1
            return int(vac) + len(t) - 1

        try:
            return HHGQAttr.reader_recode[gqtype] + len(t) + len(v) - 1
        except KeyError:
            raise ValueError(f"GQTYPE value of '{gqtype}' is not supported in {HHGQAttr.getName()} hhgq reader recode", gqtype)
