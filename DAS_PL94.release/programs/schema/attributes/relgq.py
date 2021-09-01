from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class RelGQAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_RELGQ

    @staticmethod
    def getLevels():
        return {
            "Householder living alone"                                        : [0],
            "Householder not living alone"                                    : [1],
            "Opposite-sex spouse"                                             : [2],
            "Opposite-sex unmarried partner"                                  : [3],
            "Same-sex spouse (Use allocation flag to determine)"              : [4],
            "Same-sex unmarried partner"                                      : [5],
            "Biological son or daughter"                                      : [6],
            "Adopted son or daughter"                                         : [7],
            "Stepson or stepdaughter"                                         : [8],
            "Brother or sister"                                               : [9],
            "Father or mother"                                                : [10],
            "Grandchild"                                                      : [11],
            "Parent-in-law"                                                   : [12],
            "Son-in-law or daughter-in-law"                                   : [13],
            "Other relative"                                                  : [14],
            "Roommate or housemate"                                           : [15],
            "Foster child (If roomer/boarder and age<21)"                     : [16],
            "Other nonrelative (also include if roomer/boarder and age>=21)"  : [17],
            "GQ 101 Federal detention centers"                                : [18],
            "GQ 102 Federal prisons"                                          : [19],
            "GQ 103 State prisons"                                            : [20],
            "GQ 104 Local jails and other municipal confinements"             : [21],
            "GQ 105 Correctional residential facilities"                      : [22],
            "GQ 106 Military disciplinary barracks"                           : [23],
            "GQ 201 Group homes for juveniles"                                : [24],
            "GQ 202 Residential treatment centers for juveniles"              : [25],
            "GQ 203 Correctional facilities intended for juveniles"           : [26],
            "GQ 301 Nursing facilities"                                       : [27],
            "GQ 401 Mental hospitals"                                         : [28],
            "GQ 402 Hospitals with patients who have no usual home elsewhere" : [29],
            "GQ 403 In-patient hospice facilities"                            : [30],
            "GQ 404 Military treatment facilities"                            : [31],
            "GQ 405 Residential schools for people with disabilities"         : [32],
            "GQ 501 College/university student housing"                       : [33],
            "GQ 601 Military quarters"                                        : [34],
            "GQ 602 Military ships"                                           : [35],
            "GQ 701 Emergency and transitional shelters"                      : [36],
            "GQ 801 Group homes intended for adults"                          : [37],
            "GQ 802 Residential treatment centers for adults"                 : [38],
            "GQ 900 Maritime/merchant vessels"                                : [39],
            "GQ 901 Workers' group living quarters and job corps centers"     : [40],
            "GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)": [41]
        }

    @staticmethod
    def recodegqhh():
        name = CC.HHGQ_GQLEVELS

        reldict = RelGQAttr.getLevels()
        groupings = dict([("Household", list(range(0, 18)))])
        groupings.update({k: v for k, v in reldict.items() if v[0] >= 18})

        return name, groupings

    # @staticmethod
    # def recodeRELGQ_hhgq_gqExpanded():
    #     """
    #     Query for collapsing the relationship to householder levels to "Householder" and
    #     keeping all GQ levels expanded
    #     """
    #     name = "hhgq_gqExpanded"
    #     groups = {
    #         "Householder"                                                     : list(range(0,18)),
    #         "GQ 101 Federal detention centers"                                : [18],
    #         "GQ 102 Federal prisons"                                          : [19],
    #         "GQ 103 State prisons"                                            : [20],
    #         "GQ 104 Local jails and other municipal confinements"             : [21],
    #         "GQ 105 Correctional residential facilities"                      : [22],
    #         "GQ 106 Military disciplinary barracks"                           : [23],
    #         "GQ 201 Group homes for juveniles"                                : [24],
    #         "GQ 202 Residential treatment centers for juveniles"              : [25],
    #         "GQ 203 Correctional facilities intended for juveniles"           : [26],
    #         "GQ 301 Nursing facilities"                                       : [27],
    #         "GQ 401 Mental hospitals"                                         : [28],
    #         "GQ 402 Hospitals with patients who have no usual home elsewhere" : [29],
    #         "GQ 403 In-patient hospice facilities"                            : [30],
    #         "GQ 404 Military treatment facilities"                            : [31],
    #         "GQ 405 Residential schools for people with disabilities"         : [32],
    #         "GQ 501 College/university student housing"                       : [33],
    #         "GQ 601 Military quarters"                                        : [34],
    #         "GQ 602 Military ships"                                           : [35],
    #         "GQ 701 Emergency and transitional shelters"                      : [36],
    #         "GQ 801 Group homes intended for adults"                          : [37],
    #         "GQ 802 Residential treatment centers for adults"                 : [38],
    #         "GQ 900 Maritime/merchant vessels"                                : [39],
    #         "GQ 901 Workers' group living quarters and job corps centers"     : [40],
    #         "GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)": [41]
    #     }
    #     return name, groups

    
    @staticmethod
    def recodeRELGQ_relgq0():
        """
        Query for calculating the total of the 'Householder living alone' population
        """
        name = 'relgq0'
        groups = {
            "Householder living alone Population Total": [0]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq1():
        """
        Query for calculating the total of the 'Householder not living alone' population
        """
        name = 'relgq1'
        groups = {
            "Householder not living alone Population Total": [1]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_householders():
        """
        Query for calculating the total number of householders
        """
        name = 'householders'
        groups = {
            "Householders living alone or not alone": [0, 1]
         }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq2():
        """
        Query for calculating the total of the 'Opposite-sex spouse' population
        """
        name = 'relgq2'
        groups = {
            "Opposite-sex spouse Population Total": [2]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq3():
        """
        Query for calculating the total of the 'Opposite-sex unmarried partner' population
        """
        name = 'relgq3'
        groups = {
            "Opposite-sex unmarried partner Population Total": [3]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq4():
        """
        Query for calculating the total of the 'Same-sex spouse (Use allocation flag to determine)' population
        """
        name = 'relgq4'
        groups = {
            "Same-sex spouse (Use allocation flag to determine) Population Total": [4]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq5():
        """
        Query for calculating the total of the 'Same-sex unmarried partner' population
        """
        name = 'relgq5'
        groups = {
            "Same-sex unmarried partner Population Total": [5]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq6():
        """
        Query for calculating the total of the 'Biological son or daughter' population
        """
        name = 'relgq6'
        groups = {
            "Biological son or daughter Population Total": [6]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq7():
        """
        Query for calculating the total of the 'Adopted son or daughter' population
        """
        name = 'relgq7'
        groups = {
            "Adopted son or daughter Population Total": [7]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq8():
        """
        Query for calculating the total of the 'Stepson or stepdaughter' population
        """
        name = 'relgq8'
        groups = {
            "Stepson or stepdaughter Population Total": [8]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq9():
        """
        Query for calculating the total of the 'Brother or sister' population
        """
        name = 'relgq9'
        groups = {
            "Brother or sister Population Total": [9]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq10():
        """
        Query for calculating the total of the 'Father or mother' population
        """
        name = 'relgq10'
        groups = {
            "Father or mother Population Total": [10]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq11():
        """
        Query for calculating the total of the 'Grandchild' population
        """
        name = 'relgq11'
        groups = {
            "Grandchild Population Total": [11]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq12():
        """
        Query for calculating the total of the 'Parent-in-law' population
        """
        name = 'relgq12'
        groups = {
            "Parent-in-law Population Total": [12]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq13():
        """
        Query for calculating the total of the 'Son-in-law or daughter-in-law' population
        """
        name = 'relgq13'
        groups = {
            "Son-in-law or daughter-in-law Population Total": [13]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq14():
        """
        Query for calculating the total of the 'Other relative' population
        """
        name = 'relgq14'
        groups = {
            "Other relative Population Total": [14]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq15():
        """
        Query for calculating the total of the 'Roommate or housemate' population
        """
        name = 'relgq15'
        groups = {
            "Roommate or housemate Population Total": [15]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq16():
        """
        Query for calculating the total of the 'Foster child (If roomer/boarder and age<21)' population
        """
        name = 'relgq16'
        groups = {
            "Foster child (If roomer/boarder and age<21) Population Total": [16]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq17():
        """
        Query for calculating the total of the 'Other nonrelative (also include if roomer/boarder and age>=21)' population
        """
        name = 'relgq17'
        groups = {
            "Other nonrelative (also include if roomer/boarder and age>=21) Population Total": [17]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq18():
        """
        Query for calculating the total of the 'GQ 101 Federal detention centers' population
        """
        name = 'relgq18'
        groups = {
            "GQ 101 Federal detention centers Population Total": [18]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq19():
        """
        Query for calculating the total of the 'GQ 102 Federal prisons' population
        """
        name = 'relgq19'
        groups = {
            "GQ 102 Federal prisons Population Total": [19]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq20():
        """
        Query for calculating the total of the 'GQ 103 State prisons' population
        """
        name = 'relgq20'
        groups = {
            "GQ 103 State prisons Population Total": [20]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq21():
        """
        Query for calculating the total of the 'GQ 104 Local jails and other municipal confinements' population
        """
        name = 'relgq21'
        groups = {
            "GQ 104 Local jails and other municipal confinements Population Total": [21]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq22():
        """
        Query for calculating the total of the 'GQ 105 Correctional residential facilities' population
        """
        name = 'relgq22'
        groups = {
            "GQ 105 Correctional residential facilities Population Total": [22]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq23():
        """
        Query for calculating the total of the 'GQ 106 Military disciplinary barracks' population
        """
        name = 'relgq23'
        groups = {
            "GQ 106 Military disciplinary barracks Population Total": [23]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq24():
        """
        Query for calculating the total of the 'GQ 201 Group homes for juveniles' population
        """
        name = 'relgq24'
        groups = {
            "GQ 201 Group homes for juveniles Population Total": [24]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq25():
        """
        Query for calculating the total of the 'GQ 202 Residential treatment centers for juveniles' population
        """
        name = 'relgq25'
        groups = {
            "GQ 202 Residential treatment centers for juveniles Population Total": [25]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq26():
        """
        Query for calculating the total of the 'GQ 203 Correctional facilities intended for juveniles' population
        """
        name = 'relgq26'
        groups = {
            "GQ 203 Correctional facilities intended for juveniles Population Total": [26]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq27():
        """
        Query for calculating the total of the 'GQ 301 Nursing facilities' population
        """
        name = 'relgq27'
        groups = {
            "GQ 301 Nursing facilities Population Total": [27]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq28():
        """
        Query for calculating the total of the 'GQ 401 Mental hospitals' population
        """
        name = 'relgq28'
        groups = {
            "GQ 401 Mental hospitals Population Total": [28]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq29():
        """
        Query for calculating the total of the 'GQ 402 Hospitals with patients who have no usual home elsewhere' population
        """
        name = 'relgq29'
        groups = {
            "GQ 402 Hospitals with patients who have no usual home elsewhere Population Total": [29]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq30():
        """
        Query for calculating the total of the 'GQ 403 In-patient hospice facilities' population
        """
        name = 'relgq30'
        groups = {
            "GQ 403 In-patient hospice facilities Population Total": [30]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq31():
        """
        Query for calculating the total of the 'GQ 404 Military treatment facilities' population
        """
        name = 'relgq31'
        groups = {
            "GQ 404 Military treatment facilities Population Total": [31]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq32():
        """
        Query for calculating the total of the 'GQ 405 Residential schools for people with disabilities' population
        """
        name = 'relgq32'
        groups = {
            "GQ 405 Residential schools for people with disabilities Population Total": [32]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq33():
        """
        Query for calculating the total of the 'GQ 501 College/university student housing' population
        """
        name = 'relgq33'
        groups = {
            "GQ 501 College/university student housing Population Total": [33]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq34():
        """
        Query for calculating the total of the 'GQ 601 Military quarters' population
        """
        name = 'relgq34'
        groups = {
            "GQ 601 Military quarters Population Total": [34]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq35():
        """
        Query for calculating the total of the 'GQ 602 Military ships' population
        """
        name = 'relgq35'
        groups = {
            "GQ 602 Military ships Population Total": [35]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq36():
        """
        Query for calculating the total of the 'GQ 701 Emergency and transitional shelters' population
        """
        name = 'relgq36'
        groups = {
            "GQ 701 Emergency and transitional shelters Population Total": [36]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq37():
        """
        Query for calculating the total of the 'GQ 801 Group homes intended for adults' population
        """
        name = 'relgq37'
        groups = {
            "GQ 801 Group homes intended for adults Population Total": [37]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq38():
        """
        Query for calculating the total of the 'GQ 802 Residential treatment centers for adults' population
        """
        name = 'relgq38'
        groups = {
            "GQ 802 Residential treatment centers for adults Population Total": [38]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq39():
        """
        Query for calculating the total of the 'GQ 900 Maritime/merchant vessels' population
        """
        name = 'relgq39'
        groups = {
            "GQ 900 Maritime/merchant vessels Population Total": [39]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq40():
        """
        Query for calculating the total of the 'GQ 901 Workers' group living quarters and job corps centers' population
        """
        name = 'relgq40'
        groups = {
            "GQ 901 Workers' group living quarters and job corps centers Population Total": [40]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_relgq41():
        """
        Query for calculating the total of the 'GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)' population
        """
        name = 'relgq41'
        groups = {
            "GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904 Population Total)": [41]
        }
        return name, groups

    @staticmethod
    def recodeRELGQ_hhgqsimple():
        name = CC.HHGQ_8LEV
        groupings = {
            "HOUSEHOLD": list(range(0, 18)),
            "Correctional facilities for adults": list(range(18,24)),
            "Juvenile facilities": list(range(24,27)),
            "Nursing facilities/Skilled-nursing facilities": [27],
            "Other institutional facilities": list(range(28, 33)),
            "College/University student housing": [33],
            "Military quarters": [34],
            "Other noninstitutional facilities": list(range(35,42))
        }
        return name, groupings

    @staticmethod
    def recodeRELGQ_spousesUnmarriedPartners():
        name = CC.SPOUSES_UNMARRIED_PARTNERS
        groupings = {
            "Spouses & Unmarried Partners": list(range(2,6)),
        }
        return name, groupings

    # @staticmethod
    # def recodeSpouseOrPartner():
    #     name = CC.RELGQ_SPOUSE_OR_PARTNER
    #     groupings = {
    #         "Spouse or unmarried partner": [2, 3, 4, 5]
    #     }
    #     return name, groupings

    @staticmethod
    def recodeRELGQ_peopleInHouseholds():
        name = CC.PEOPLE_IN_HOUSEHOLDS
        groupings = {
            "People in Households" : list(range(0, 18)),
        }
        return name, groupings


