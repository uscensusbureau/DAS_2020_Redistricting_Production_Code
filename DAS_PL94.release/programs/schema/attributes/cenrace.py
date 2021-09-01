"""
census race abstract attribute module
"""
from programs.schema.attributes.abstractattribute import AbstractAttribute
import programs.cenrace as cenrace
from constants import CC


class CenraceAttr(AbstractAttribute):
    """Census Race Class"""
    @staticmethod
    def getName():
        """Get Attribute Name"""
        return CC.ATTR_CENRACE

    @staticmethod
    def getLevels():
        """Get Levels"""
        return {
                'white'                           : [0],
                'black'                           : [1],
                'aian'                            : [2],
                'asian'                           : [3],
                'nhopi'                           : [4],
                'sor'                             : [5],
                'white-black'                     : [6],
                'white-aian'                      : [7],
                'white-asian'                     : [8],
                'white-nhopi'                     : [9],
                'white-sor'                       : [10],
                'black-aian'                      : [11],
                'black-asian'                     : [12],
                'black-nhopi'                     : [13],
                'black-sor'                       : [14],
                'aian-asian'                      : [15],
                'aian-nhopi'                      : [16],
                'aian-sor'                        : [17],
                'asian-nhopi'                     : [18],
                'asian-sor'                       : [19],
                'nhopi-sor'                       : [20],
                'white-black-aian'                : [21],
                'white-black-asian'               : [22],
                'white-black-nhopi'               : [23],
                'white-black-sor'                 : [24],
                'white-aian-asian'                : [25],
                'white-aian-nhopi'                : [26],
                'white-aian-sor'                  : [27],
                'white-asian-nhopi'               : [28],
                'white-asian-sor'                 : [29],
                'white-nhopi-sor'                 : [30],
                'black-aian-asian'                : [31],
                'black-aian-nhopi'                : [32],
                'black-aian-sor'                  : [33],
                'black-asian-nhopi'               : [34],
                'black-asian-sor'                 : [35],
                'black-nhopi-sor'                 : [36],
                'aian-asian-nhopi'                : [37],
                'aian-asian-sor'                  : [38],
                'aian-nhopi-sor'                  : [39],
                'asian-nhopi-sor'                 : [40],
                'white-black-aian-asian'          : [41],
                'white-black-aian-nhopi'          : [42],
                'white-black-aian-sor'            : [43],
                'white-black-asian-nhopi'         : [44],
                'white-black-asian-sor'           : [45],
                'white-black-nhopi-sor'           : [46],
                'white-aian-asian-nhopi'          : [47],
                'white-aian-asian-sor'            : [48],
                'white-aian-nhopi-sor'            : [49],
                'white-asian-nhopi-sor'           : [50],
                'black-aian-asian-nhopi'          : [51],
                'black-aian-asian-sor'            : [52],
                'black-aian-nhopi-sor'            : [53],
                'black-asian-nhopi-sor'           : [54],
                'aian-asian-nhopi-sor'            : [55],
                'white-black-aian-asian-nhopi'    : [56],
                'white-black-aian-asian-sor'      : [57],
                'white-black-aian-nhopi-sor'      : [58],
                'white-black-asian-nhopi-sor'     : [59],
                'white-aian-asian-nhopi-sor'      : [60],
                'black-aian-asian-nhopi-sor'      : [61],
                'white-black-aian-asian-nhopi-sor': [62]
            }

    @staticmethod
    def recodeNumraces():
        """
        Group CENRACE levels by the number of races present in the level
        """
        name = CC.CENRACE_NUM

        group = cenrace.getCenraceNumberGroups()
        labels = group.getGroupLabels()
        indices = group.getIndexGroups()
        groups = dict(zip(labels, indices))
        return name, groups

    @staticmethod
    def recodeMajorRaces():
        """
        Group CENRACE into individual single-race categories (alone) and another level that represents all combinations of races
        """
        name = CC.CENRACE_MAJOR
        groups = {
                "White alone": [0],
                "Black or African American alone": [1],
                "American Indian and Alaska Native alone": [2],
                "Asian alone": [3],
                "Native Hawaiian and Other Pacific Islander alone": [4],
                "Some Other Race alone": [5],
                "Two or More Races": list(range(6, 63))
            }
        return name, groups

    @staticmethod
    def recodePriorityCombinations():
        """
            Generates: W, Bl, AIAN, Asian, NHPI, SOR, Bl+W, AIAN+W, Asian+W, AIAN+Bl, Balance, for use in December experiments
        """
        name = CC.CENRACE_11_CATS
        groups = {
                "White alone": [0],
                "Black or African American alone": [1],
                "American Indian and Alaska Native alone": [2],
                "Asian alone": [3],
                "Native Hawaiian and Other Pacific Islander alone": [4],
                "Some Other Race alone": [5],
                "Black or African American and White": [6],
                "American Indian and Alaska Native and White": [7],
                "Asian and White": [8],
                "American Indian and Alaska Native and Black or African American": [11],
                "Balance/remainder": [9, 10] + list(range(12, 63))
            }
        return name, groups


    @staticmethod
    def recodeRacecomb():
        """
        recode race combinations
        """
        name = CC.CENRACE_COMB

        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        groups = {
                'White alone or in combination with one or more other races': groups[0],
                'Black or African American alone or in combination with one or more other races': groups[1],
                'American Indian and Alaska Native alone or in combination with one or more other races': groups[2],
                'Asian alone or in combination with one or more other races': groups[3],
                'Native Hawaiian and Other Pacific Islander alone or in combination with one or more other races': groups[4],
                'Some Other Race alone or in combination with one or more other races': groups[5]
            }
        return name, groups

    @staticmethod
    def recodeWhiteAlone():
        """
        Subset the CENRACE variable to include only the "White" category, alone
        """
        name = CC.CENRACE_WHITEALONE
        groups = {
                "White alone": [0]
            }
        return name, groups

    @staticmethod
    def recodeBlackAlone():
        """
        Subset the CENRACE variable to include only the "Black or African American" category, alone
        """
        name = CC.CENRACE_BLACKALONE
        groups = {
                "Black or African American alone": [1]
            }
        return name, groups

    @staticmethod
    def recodeAianAlone():
        """
        Subset the CENRACE variable to include only the "American Indian and Alaska Native" category, alone
        """
        name = CC.CENRACE_AIANALONE
        groups = {
                "American Indian and Alaska Native alone": [2]
            }
        return name, groups

    @staticmethod
    def recodeAsianAlone():
        """
        Subset the CENRACE variable to include only the "Asian" category, alone
        """
        name = CC.CENRACE_ASIANALONE
        groups = {
                "Asian alone": [3]
            }
        return name, groups

    @staticmethod
    def recodeNhopiAlone():
        """
        Subset the CENRACE variable to include only the "Native Hawaiian and Pacific Islander" category, alone
        """
        name = CC.CENRACE_NHOPIALONE
        groups = {
                "Native Hawaiian and Pacific Islander alone": [4]
            }
        return name, groups

    @staticmethod
    def recodeSorAlone():
        """
        Subset the CENRACE variable to include only the "Some Other Race" category, alone
        """
        name = CC.CENRACE_SORALONE
        groups = {
            "Some Other Race alone": [5]
            }
        return name, groups

    @staticmethod
    def recodeTomr():
        """
        Subset the CENRACE variable to include only the "Two or more races" category, alone
        """
        name = CC.CENRACE_TOMR
        groups = {
           "Two or more races": list(range(6, 63))
            }
        return name, groups

    @staticmethod
    def recode7LevelRace():
        """
        recode 7 level race
        """
        name = CC.RACE7LEV
        groups = {
            "white"                           : [0],
            "black"                           : [1],
            "aian"                            : [2],
            "asian"                           : [3],
            "nhopi"                           : [4],
            "sor"                             : [5],
            "Two or more races": list(range(6, 63))
        }
        return name, groups

    @staticmethod
    def recode7lev_two_comb():
        name = CC.CENRACE_7LEV_TWO_COMB
        groups = {
            "white"                           : [0],
            "black"                           : [1, 6],
            "aian"                            : [2, 7],
            "asian"                           : [3, 8],
            "nhopi"                           : [4, 9],
            "sor"                             : [5, 10],
            "Two or more races": list(range(11, 63))
        }
        return name, groups

    @staticmethod
    def recodeAllraces():
        """
        recode all races
        """
        name = CC.CENRACE_ALLRACES
        groups = {
            "white"                           : [0],
            "black"                           : [1],
            "aian"                            : [2],
            "asian"                           : [3],
            "nhopi"                           : [4],
            "sor"                             : [5]
        }
        return name, groups

    @staticmethod
    def recodeWhiteCombo():
        """
        recode white combo
        """
        name = CC.CENRACE_WHITECOMBO
        groups = {
            "White alone or in combination": [0, 6, 7, 8, 9, 10,
                                              21,
                                              22,
                                              23,
                                              24,
                                              25,
                                              26,
                                              27,
                                              28,
                                              29,
                                              30,
                                              41,
                                              42,
                                              43,
                                              44,
                                              45,
                                              46,
                                              47,
                                              48,
                                              49,
                                              50,
                                              56,
                                              57,
                                              58,
                                              59,
                                              60,
                                              62]

        }
        return name, groups

    @staticmethod
    def recodeBlackCombo():
        """
        recod black combo
        """
        name = CC.CENRACE_BLACKCOMBO
        groups = {"Black alone or in combination": [1,
                                                    6,
                                                    11,
                                                    12,
                                                    13,
                                                    14,
                                                    21,
                                                    22,
                                                    23,
                                                    24,
                                                    31,
                                                    32,
                                                    33,
                                                    34,
                                                    35,
                                                    36,
                                                    41,
                                                    42,
                                                    43,
                                                    44,
                                                    45,
                                                    46,
                                                    51,
                                                    52,
                                                    53,
                                                    54,
                                                    56,
                                                    57,
                                                    58,
                                                    59,
                                                    61,
                                                    62]

                  }
        return name, groups

    @staticmethod
    def recodeAianCombo():
        """
        recode asian combo
        """
        #function is misspelled
        name = CC.CENRACE_AIANCOMBO
        groups = {"Aian alone or in combination": [2,
                                                   7,
                                                   11,
                                                   15,
                                                   16,
                                                   17,
                                                   21,
                                                   25,
                                                   26,
                                                   27,
                                                   31,
                                                   32,
                                                   33,
                                                   37,
                                                   38,
                                                   39,
                                                   41,
                                                   42,
                                                   43,
                                                   47,
                                                   48,
                                                   49,
                                                   51,
                                                   52,
                                                   53,
                                                   55,
                                                   56,
                                                   57,
                                                   58,
                                                   60,
                                                   61,
                                                   62]
                  }
        return name, groups

    @staticmethod
    def recodeAsianCombo():
        """
        recode asian combo
        """
        name = CC.CENRACE_ASIANCOMBO
        groups = {"Asian alone or in combination": [3,
                                                    8,
                                                    12,
                                                    15,
                                                    18,
                                                    19,
                                                    22,
                                                    25,
                                                    28,
                                                    29,
                                                    31,
                                                    34,
                                                    35,
                                                    37,
                                                    38,
                                                    40,
                                                    41,
                                                    44,
                                                    45,
                                                    47,
                                                    48,
                                                    50,
                                                    51,
                                                    52,
                                                    54,
                                                    55,
                                                    56,
                                                    57,
                                                    59,
                                                    60,
                                                    61,
                                                    62]
                  }
        return name, groups

    @staticmethod
    def recodeNhopiCombo():
        """
        recode Native Hawaiian or Other Pacific Islander
        """
        name = CC.CENRACE_NHOPICOMBO
        groups = {"Nhopi alone or in combination": [4,
                                                    9,
                                                    13,
                                                    16,
                                                    18,
                                                    20,
                                                    23,
                                                    26,
                                                    28,
                                                    30,
                                                    32,
                                                    34,
                                                    36,
                                                    37,
                                                    39,
                                                    40,
                                                    42,
                                                    44,
                                                    46,
                                                    47,
                                                    49,
                                                    50,
                                                    51,
                                                    53,
                                                    54,
                                                    55,
                                                    56,
                                                    58,
                                                    59,
                                                    60,
                                                    61,
                                                    62]
                  }
        return name, groups

    @staticmethod
    def recodeSorCombo():
        """
        recode some race combo
        """
        name = CC.CENRACE_SORCOMBO
        groups = {"Some other race alone or in combination": [5,
                                                              10,
                                                              14,
                                                              17,
                                                              19,
                                                              20,
                                                              24,
                                                              27,
                                                              29,
                                                              30,
                                                              33,
                                                              35,
                                                              36,
                                                              38,
                                                              39,
                                                              40,
                                                              43,
                                                              45,
                                                              46,
                                                              48,
                                                              49,
                                                              50,
                                                              52,
                                                              53,
                                                              54,
                                                              55,
                                                              57,
                                                              58,
                                                              59,
                                                              60,
                                                              61,
                                                              62]
                  }
        return name, groups

    @staticmethod
    def recoderaceCombos():
        """
        recode all race combos into one query
        """
    # This implements all race combos into one query
        name = CC.CENRACE_RACECOMBOS
        groups = {
                "White alone or in combination": [0,6,7,8,9,10,21,22,23,24,25,26,27,28,29,30,
                                                 41,42,43,44,45,46,47,48,49,50,56,57,58,59,60,62],
                "Black alone or in combination": [1,6,11,12,13,14,21,22,23,24,31,32,33,34,35,
                                                  36,41,42,43,44,45,46,51,52,53,54,56,57,58,59,61,62],
                "Aian alone or in combination":  [2,7,11,15,16,17,21,25,26,27,31,32,33,37,38,
                                                  39,41,42,43,47,48,49,51,52,53,55,56,57,58,60,61,62],
                "Asian alone or in combination": [3,8,12,15,18,19,22,25,28,29,31,34,35,37,38,
                                                  40,41,44,45,47,48,50,51,52,54,55,56,57,59,60,61,62],
                "Nhopi alone or in combination": [4,9,13,16,18,20,23,26,28,30,32,34,36,37,39,
                                                  40,42,44,46,47,49,50,51,53,54,55,56,58,59,60,61,62],
                "Some other race alone or in combination":
                                                 [5,10,14,17,19,20,24,27,29,30,33,35,36,38,39,
                                                  40,43,45,46,48,49,50,52,53,54,55,57,58,59,60,61,62]
}
        return name, groups
