from constants import CC


def getTableDict_Household2010():
    tabledict = {
        ################################################
        # Household2010 Proposed Tables for 2020
        ################################################

        # Table P15 - Hispanic or Latino Origin of Householder by Race of Householder
        # Universe: Households
        "P15": ["total",
            "hisp", "hisp * hhrace"],

        # Table P18 - Household Type
        # Universe: Households
        "P18": ["total",
            "family",
                "marriedfamily",
                "otherfamily",
                "otherfamily * hhsex",
            "nonfamily",
                "alone",
                "notalone" ],

        # Table P19 - Household Size by Household Type by Presence of Own Children
        # Universe: Households
        "P19": ["isOccupied",
            "size1",
                "size1 * hhsex",
            "size2plus",
                "size2plus * family",
                    "size2plus * marriedfamily",
                        "size2plus * married_with_children_indicator",
                     "size2plus * otherfamily",
                        "size2plus * otherfamily * hhsex",
                            "size2plus * hhsex * other_with_children_indicator",
                "size2plus * nonfamily",
                    "size2plus * nonfamily * hhsex"],

        # Table 20 - Households by Type and Presence of Own Children Under 18
        # Universe: Households
        "P20": ["total",
            "marriedfamily",
                "married_with_children_indicator",
            "cohabiting",
                "cohabiting_with_children_indicator",
            "hhsex * no_spouse_or_partner",
                "hhsex * alone" ,
                    "hhsex * alone * age_65plus",
                "hhsex * no_spouse_or_partner_levels"],

        # Table P22 - Household Type by Age of Householder
        # Universe: Households
        "P22": ["total",

            "family",
                "hhage * family",
            "nonfamily",
                "hhage * nonfamily"],

        # Table P23 - Households by Presence of People 60 Years and Over by Household Type
        # Universe: Households
        "P23": ["total",

            "presence60",
                "family * presence60",
                    "presence60 * marriedfamily",
                    "presence60 * otherfamily",
                        "presence60 * otherfamily * hhsex",
                "presence60 * nonfamily"],

        # Table P24 - Households by Presence of People 60 Years and Over, Household Size, and Household Type
        # Universe: Households
        "P24": ["isOccupied",
            "isOccupied * presence60",
                "presence60 * size1",
                "presence60 * size2plus",
                    "presence60 * size2plus * family",
                    "presence60 * size2plus * nonfamily"],

        # Table P25 - Households by Presence of People 65 Years and Over, Household Size, and Household Type
        # Universe: Households
        "P25": ["isOccupied",
            "isOccupied * presence65",
                "presence65 * size1",
                "presence65 * size2plus",
                    "presence65 * size2plus * family",
                    "presence65 * size2plus * nonfamily"],

        # Table P26 - Households by Presence of People 75 Years and Over, Household Size, and Household Type
        # Universe: Households
        "P26": ["isOccupied",
            "isOccupied * presence75",
                "presence75 * size1",
                "presence75 * size2plus",
                    "presence75 * size2plus * family",
                    "presence75 * size2plus * nonfamily"],

        # Table P28 - Household Type by Household Size
        "P28": ["isOccupied",
            "isOccupied * family",
                "family * sizex01",
            "isOccupied * nonfamily",
                "nonfamily * sizex0"],

        # Table P38 - Family Type by Presence and Age of Own Children
        # Universe: Families
        "P38": ["family",

            "marriedfamily",
                "married_with_children_indicator",
                    "married_with_children_levels",
            "otherfamily",
                "hhsex * otherfamily",
                    "hhsex * other_with_children_indicator",
                        "hhsex * other_with_children_levels"],

        #Table P18A
        # Universe: Households with white alone householder
        "P18A": ["isWhiteAlone",
            "isWhiteAlone * family", "isWhiteAlone * marriedfamily", "isWhiteAlone * otherfamily", "isWhiteAlone * otherfamily * hhsex", "isWhiteAlone * nonfamily",
            "isWhiteAlone * alone", "isWhiteAlone * notalone"
        ],

        # Table P18B
        # Universe: Households with black alone householder
        "P18B": ["isBlackAlone",
                 "isBlackAlone * family", "isBlackAlone * marriedfamily", "isBlackAlone * otherfamily", "isBlackAlone * otherfamily * hhsex",
                 "isBlackAlone * nonfamily", "isBlackAlone * alone", "isBlackAlone * notalone"],

        # Table P18C
        # Universe: Households with AIAN alone householder
        "P18C": ["isAIANAlone",
                 "isAIANAlone * family", "isAIANAlone * marriedfamily", "isAIANAlone * otherfamily", "isAIANAlone * otherfamily * hhsex",
                 "isAIANAlone * nonfamily", "isAIANAlone * alone", "isAIANAlone * notalone"],

        # Table P18D
        # Universe: Households with Asian alone householder
        "P18D": ["isAsianAlone",
                 "isAsianAlone * family", "isAsianAlone * marriedfamily", "isAsianAlone * otherfamily", "isAsianAlone * otherfamily * hhsex",
                 "isAsianAlone * nonfamily", "isAsianAlone * alone", "isAsianAlone * notalone"],

        # Table P18E
        # Universe: Households with NHOPI alone householder
        "P18E": ["isNHOPIAlone",
                 "isNHOPIAlone * family", "isNHOPIAlone * marriedfamily", "isNHOPIAlone * otherfamily", "isNHOPIAlone * otherfamily * hhsex",
                 "isNHOPIAlone * nonfamily", "isNHOPIAlone * alone", "isNHOPIAlone * notalone"],

        # Table P18F
        # Universe: Households with SOR alone householder
        "P18F": ["isSORAlone",
                 "isSORAlone * family", "isSORAlone * marriedfamily", "isSORAlone * otherfamily", "isSORAlone * otherfamily * hhsex",
                 "isSORAlone * nonfamily", "isSORAlone * alone", "isSORAlone * notalone"],

        # Table P18G
        # Universe: Households with a householder of two or more races
        "P18G": ["isTMRaces",
                 "isTMRaces * family", "isTMRaces * marriedfamily", "isTMRaces * otherfamily", "isTMRaces * otherfamily * hhsex",
                 "isTMRaces * nonfamily", "isTMRaces * alone", "isTMRaces * notalone"],

        # Table P18H
        "P18H": ["isHisp",
            "isHisp * family",
                 "isHisp * marriedfamily",
                 "isHisp * otherfamily",
                    "isHisp * otherfamily * hhsex",
            "isHisp * nonfamily",
                "isHisp * alone",
                "isHisp * notalone"],

        # Table P18I
        "P18I": ["isWhiteAlone * isNotHisp",  # to get the totals for each of the white alone x hispanic categories

            "isWhiteAlone * isNotHisp * family",
                "isWhiteAlone * isNotHisp * marriedfamily",
                "isWhiteAlone * isNotHisp * otherfamily",
                    "isWhiteAlone * isNotHisp * otherfamily * hhsex",
            "isWhiteAlone * isNotHisp * nonfamily",
                "isWhiteAlone * isNotHisp * alone",
                "isWhiteAlone * isNotHisp * notalone"],

        #Table P28A
        #Universe: Households with white alone householder
        "P28A": ["isOccupied * isWhiteAlone",  # to get the totals for each race
                   "isOccupied * isWhiteAlone * family", "isWhiteAlone * family * sizex01", "isOccupied * isWhiteAlone * nonfamily", "isWhiteAlone * nonfamily * sizex0"],

        # Table P28B
        # Universe: Households with black alone householder
        "P28B": ["isOccupied * isBlackAlone",  # to get the totals for each race
                   "isOccupied * isBlackAlone * family", "isBlackAlone * family * sizex01", "isOccupied * isBlackAlone * nonfamily", "isBlackAlone * nonfamily * sizex0"],

        # Table P28C
        # Universe: Households with AIAN alone householder
        "P28C": ["isOccupied * isAIANAlone",  # to get the totals for each race
                   "isOccupied * isAIANAlone * family", "isAIANAlone * family * sizex01", "isOccupied * isAIANAlone * nonfamily", "isAIANAlone * nonfamily * sizex0"],

        # Table P28D
        # Universe: Households with Asian alone householder
        "P28D": ["isOccupied * isAsianAlone",  # to get the totals for each race
                   "isOccupied * isAsianAlone * family", "isAsianAlone * family * sizex01", "isOccupied * isAsianAlone * nonfamily", "isAsianAlone * nonfamily * sizex0"],

        # Table P28E
        # Universe: Households with NHOPI alone householder
        "P28E": ["isOccupied * isNHOPIAlone",  # to get the totals for each race
                   "isOccupied * isNHOPIAlone * family", "isNHOPIAlone * family * sizex01", "isOccupied * isNHOPIAlone * nonfamily", "isNHOPIAlone * nonfamily * sizex0"],

        # Table P28F
        # Universe: Households with SOR alone householder
        "P28F": ["isOccupied * isSORAlone",  # to get the totals for each race
                   "isOccupied * isSORAlone * family", "isSORAlone * family * sizex01", "isOccupied * isSORAlone * nonfamily", "isSORAlone * nonfamily * sizex0"],

        # Table P28G
        # Universe: Households with a householder of two or more races
        "P28G": ["isOccupied * isTMRaces",  # to get the totals for each race
                   "isOccupied * isTMRaces * family", "isTMRaces * family * sizex01", "isOccupied * isTMRaces * nonfamily", "isTMRaces * nonfamily * sizex0"],

        # Table P28H
        "P28H": ["isOccupied * isHisp",  # to get the totals for each of the hispanic categories
            "isOccupied * isHisp * family",
                 "isHisp * family * sizex01",
            "isOccupied * isHisp * nonfamily",
                 "isHisp * nonfamily * sizex0"],

        # Table P28I
        "P28I": ["isOccupied * isWhiteAlone * isNotHisp",  # to get the totals for each of the white alone x hispanic categories
            "isOccupied * isWhiteAlone * isNotHisp * family",
                 "isWhiteAlone * isNotHisp * family * sizex01",
            "isOccupied * isWhiteAlone * isNotHisp * nonfamily",
                "isWhiteAlone * isNotHisp * nonfamily * sizex0"],

        #Table P38A:
        #Universe: Families with white alone householder
        "P38A": ["isWhiteAlone * family",  # to get the totals for each race
            "isWhiteAlone * marriedfamily", "isWhiteAlone * married_with_children_indicator", "isWhiteAlone * married_with_children_levels", "isWhiteAlone * otherfamily",
            "isWhiteAlone * hhsex * otherfamily", "isWhiteAlone * hhsex * other_with_children_indicator", "isWhiteAlone * hhsex * other_with_children_levels"
        ],

        # Table P38B:
        # Universe: Families with black alone householder
        "P38B": ["isBlackAlone * family",  # to get the totals for each race
            "isBlackAlone * marriedfamily", "isBlackAlone * married_with_children_indicator", "isBlackAlone * married_with_children_levels", "isBlackAlone * otherfamily",
            "isBlackAlone * hhsex * otherfamily", "isBlackAlone * hhsex * other_with_children_indicator", "isBlackAlone * hhsex * other_with_children_levels"
        ],

        # Table P38C:
        # Universe: Families with AIAN alone householder
        "P38C": ["isAIANAlone * family",  # to get the totals for each race
            "isAIANAlone * marriedfamily", "isAIANAlone * married_with_children_indicator", "isAIANAlone * married_with_children_levels", "isAIANAlone * otherfamily",
            "isAIANAlone * hhsex * otherfamily", "isAIANAlone * hhsex * other_with_children_indicator", "isAIANAlone * hhsex * other_with_children_levels"
        ],

        # Table P38D:
        # Universe: Families with Asian alone householder
        "P38D": ["isAsianAlone * family",  # to get the totals for each race
            "isAsianAlone * marriedfamily", "isAsianAlone * married_with_children_indicator", "isAsianAlone * married_with_children_levels", "isAsianAlone * otherfamily",
            "isAsianAlone * hhsex * otherfamily", "isAsianAlone * hhsex * other_with_children_indicator", "isAsianAlone * hhsex * other_with_children_levels"
        ],

        # Table P38E:
        # Universe: Families with NHOPI alone householder
        "P38E": ["isNHOPIAlone * family",  # to get the totals for each race
            "isNHOPIAlone * marriedfamily", "isNHOPIAlone * married_with_children_indicator", "isNHOPIAlone * married_with_children_levels", "isNHOPIAlone * otherfamily",
            "isNHOPIAlone * hhsex * otherfamily", "isNHOPIAlone * hhsex * other_with_children_indicator", "isNHOPIAlone * hhsex * other_with_children_levels"
        ],

        # Table P38F:
        # Universe: Families with SOR alone householder
        "P38F": ["isSORAlone * family",  # to get the totals for each race
            "isSORAlone * marriedfamily", "isSORAlone * married_with_children_indicator", "isSORAlone * married_with_children_levels", "isSORAlone * otherfamily",
            "isSORAlone * hhsex * otherfamily", "isSORAlone * hhsex * other_with_children_indicator", "isSORAlone * hhsex * other_with_children_levels"
        ],

        # Table P38G:
        # Universe: Families with a householder of two or more races
        "P38G": ["isTMRaces * family",  # to get the totals for each race
            "isTMRaces * marriedfamily", "isTMRaces * married_with_children_indicator", "isTMRaces * married_with_children_levels", "isTMRaces * otherfamily",
            "isTMRaces * hhsex * otherfamily", "isTMRaces * hhsex * other_with_children_indicator", "isTMRaces * hhsex * other_with_children_levels"
        ],

        # Table P38H
        "P38H": [
            "isHisp * family",  # to get the totals
            "isHisp * marriedfamily",
                "isHisp * married_with_children_indicator",
                    "isHisp * married_with_children_levels",
            "isHisp * otherfamily",
                "isHisp * hhsex * otherfamily",
                    "isHisp * hhsex * other_with_children_indicator",
                        "isHisp * hhsex * other_with_children_levels"],

        # Table P38I
        "P38I": [
            "isWhiteAlone * isNotHisp * family",  # to get the totals for white alone x hispanic categories

            "isWhiteAlone * isNotHisp * marriedfamily",
                "isWhiteAlone * isNotHisp * married_with_children_indicator",
                    "isWhiteAlone * isNotHisp * married_with_children_levels",
            "isWhiteAlone * isNotHisp * otherfamily",
                "isWhiteAlone * isNotHisp * hhsex * otherfamily",
                    "isWhiteAlone * isNotHisp * hhsex * other_with_children_indicator",
                        "isWhiteAlone * isNotHisp * hhsex * other_with_children_levels"],

        # Table PCT14
        "PCT14": ["total",
            "multi"],

        # Table PCT15
        "PCT15": ["total",
            "couplelevels",
                "oppositelevels",
                "samelevels",
                    "hhsex * samelevels"],

        # Table PCT18
        "PCT18": ["nonfamily",
            "hhsex * nonfamily",
                "hhsex * alone",
                    "hhsex * alone * hhover65",
                "hhsex * notalone",
                    "hhsex * notalone * hhover65"],

        #Table PCT14A
        #Universe: Households with a householder who is white alone
        "PCT14A": ["isWhiteAlone", "isWhiteAlone * multi"],

        # Table PCT14B
        # Universe: Households with a householder who is black alone
        "PCT14B": ["isBlackAlone", "isBlackAlone * multi"],

        # Table PCT14C
        # Universe: Households with a householder who is AIAN alone
        "PCT14C": ["isAIANAlone", "isAIANAlone * multi"],

        # Table PCT14D
        # Universe: Households with a householder who is Asian alone
        "PCT14D": ["isAsianAlone", "isAsianAlone * multi"],

        # Table PCT14E
        # Universe: Households with a householder who is NHOPI alone
        "PCT14E": ["isNHOPIAlone", "isNHOPIAlone * multi"],

        # Table PCT14F
        # Universe: Households with a householder who is SOR alone
        "PCT14F": ["isSORAlone", "isSORAlone * multi"],

        # Table PCT14G
        # Universe: Households with a householder who is of two or races
        "PCT14G": ["isTMRaces", "isTMRaces * multi"],

        # Table PCT14H
        # Universe: Households with a householder who is hispanic or latino
        "PCT14H": ["isHisp", "isHisp * multi"],

        # Table PCT14I
        # Universe: Households with a householder who is white alone, non-hispanic
        "PCT14I": ["isWhiteAlone * isNotHisp", "isWhiteAlone * isNotHisp * multi"],

        # Table H1

        # Table H3

        # Table H6
        "H6": ["isOccupied",
            "isOccupied * hhrace"],

        # Table H7
        "H7": ["isOccupied",
            "isOccupied * hisp",
            "isOccupied * hisp * hhrace"],

        # Table H13
        "H13": ["isOccupied",
            "sizex0"],

    }

    return tabledict


def getTableDict_DHCP_HHGQ():
    tabledict = {
        
        "Geolevel_TVD": [
            "total",
            
            "relgq",
            "sex",
            "age",
            "hispanic",
            "cenrace",
            
            "cenrace * hispanic",
            "age * sex",
            "numraces",
            "numraces * hispanic",
            "votingage",
            
            # "votingage * ???",
            
            "detailed"
        ],
        
        ################################################
        # Hierarchical age category tables
        ################################################
        # Prefix Query
        "prefixquery": ['prefix_agecats', 'age'],

        # Range Query
        "rangequery": ['range_agecats', 'age'],

        # Prefix Query
        "binarysplitquery": ['binarysplit_agecats', 'age'],

        ################################################
        # SF1 Proposed Tables for 2020 (Person-focused)
        ################################################

        # Table P1 - Total Population
        # Universe: Total Population
        "P1": ["total"],

        # Table P3 - Race
        # Universe: Total Population
        "P3": ["total", "majorRaces"],

        # Table P4 - Hispanic or Latino
        # Universe: Total Population
        "P4": ["total", "hispanic"],

        # Table P5 - Hispanic or Latino by Race
        # Universe: Total Population
        "P5": ["total", "hispanic", "hispanic * majorRaces"],

        # Table P6 - Race (Total Races Tallied)
        # Universe: Total Races Tallied ???
        # What does the total races tallied query mean??? Do we use "total" or some
        # recode that totals the racecomb query answers?
        "P6": [# "racecombTotal",
            # "total",
            "racecomb"],

        # Table P7 - Hispanic or Latino by Race (Total Races Tallied)
        # Universe: Total Races Tallied ???
        # What does the total races tallied query mean??? Do we use "total" or some
        # recode that totals the racecomb query answers?
        "P7": [# "racecombTotal",
            # "total",
            # "hispanic * racecombTotal",
            # "hispanic",
            "racecomb", "hispanic * racecomb"],

        # Table P12 - Sex by Age
        # Universe: Total Population
        "P12": ["total", "sex", "sex * agecat"],

        # Table P13 - Median age by Sex (1 Expressed Decimal)

        # Table P14 - Sex by Age for the Population Under 20 Years
        # Universe: Population Under 20 Years
        "P14": ["under20yearsTotal", "under20yearsTotal * sex", "under20years * sex"],

        # Table P16 - Population in households by age
        "P16": ["hhTotal", "hhTotal * votingage"],

        # Table P43 - GQ Population by Sex by Age by Major GQ Type
        # Universe: Population in GQs
        "P43": ["gqTotal", "gqTotal * sex", "gqTotal * sex * agecat43",  "sex * agecat43 * institutionalized",
                "sex * agecat43 * majorGQs"],


        # Table P12A - Sex by Age (White alone)
        # Universe: People who are White alone
        "P12A": [
            "whiteAlone", "whiteAlone * sex", "whiteAlone * sex * agecat",
        ],

        # Table P12B - Sex by Age (Black or African American alone)
        # Universe: People who are Black or African American alone
        "P12B": [
            "blackAlone", "blackAlone * sex", "blackAlone * sex * agecat"
        ],

        # Table P12C - Sex by Age (American Indian and Alaska Native alone)
        # Universe: People who are American Indian and Alaska Native alone
        "P12C": [
            "aianAlone", "aianAlone * sex", "aianAlone * sex * agecat"
        ],

        # Table P12D - Sex by Age (Asian alone)
        # Universe: People who are Asian alone
        "P12D": [
            "asianAlone", "asianAlone * sex", "asianAlone * sex * agecat"
        ],

        # Table P12E - Sex by Age (Native Hawaiian and Other Pacific Islander alone)
        # Universe: People who are Native Hawaiian and Other Pacific Islander alone)
        "P12E": [
            "nhopiAlone",  "nhopiAlone * sex", "nhopiAlone * sex * agecat"
        ],

        # Table P12F - Sex by Age (Some Other Race alone)
        # Universe: People who are Some Other Race alone
        "P12F": [
            "sorAlone", "sorAlone * sex", "sorAlone * sex * agecat"
        ],

        # Table P12G - Sex by Age (Two or more races)
        # Universe: People who are two or more races
        "P12G": [
            "tomr", "tomr * sex", "tomr * sex * agecat"
        ],

        # Table P12H - Sex by Age (Hispanic or Latino)
        # Universe: People who are Hispanic or Latino
        "P12H": [
            "hispTotal", "hispTotal * sex", "hispTotal * sex * agecat"
        ],

        # Table P12I - Sex by Age (White alone, not Hispanic or Latino)
        "P12I": [
            "whiteAlone * notHispTotal", "whiteAlone * notHispTotal * sex", "whiteAlone * notHispTotal * sex * agecat"
        ],

        # Tables P13A-I Median age by sex

        # PCO1 - Group Quarters Population by Sex by Age
        # Universe: Population in group quarters
        "PCO1": ["gqTotal", "gqTotal * sex", "gqTotal * sex * agecatPCO1"],

        # PCO2 - Group Quarters Population in Institutional Facilities by Sex by Age
        # Universe: Institutionalized Population
        "PCO2": ["instTotal", "instTotal * sex", "instTotal * sex * agecatPCO1"],

        # PCO3 - GQ Pop in Correctional Facilities for Adults by Sex by Age
        # Universe: Pop in correctional facilities for adults
        "PCO3": ["gqCorrectionalTotal", "gqCorrectionalTotal * sex",  "gqCorrectionalTotal * sex * agecatPCO3"],

        # PCO4 - GQ Pop in Juvenile Facilities by Sex by Age
        # Universe: Pop in juvenile facilities
        "PCO4": ["gqJuvenileTotal", "gqJuvenileTotal * sex", "gqJuvenileTotal * sex * agecatPCO4"],

        # PCO5 - GQ Pop in Nursing Facilities / Skilled-Nursing Facilities by Sex by Age
        # Universe: Pop in nursing facilities/skilled-nursing facilities
        "PCO5": ["gqNursingTotal", "gqNursingTotal * sex", "gqNursingTotal * sex * agecatPCO5"],

        # PCO6 - GQ Pop in Other Institutional Facilities by Sex by Age
        # Universe: Pop in other institutional facilities
        "PCO6": ["gqOtherInstTotal", "gqOtherInstTotal * sex", "gqOtherInstTotal * sex * agecatPCO1"],

        # PCO7 - GQ Pop in Noninstitutional Facilities by Sex by Age
        # Universe: Pop in noninstitutional facilities
        "PCO7": ["noninstTotal", "noninstTotal * sex", "noninstTotal * sex * agecatPCO7"],

        # PCO8 - GQ Pop in College/University Student Housing by Sex by Age
        # Universe: Pop in college/university student housing
        "PCO8": ["gqCollegeTotal", "gqCollegeTotal * sex", "gqCollegeTotal * sex * agecatPCO8"],

        # PCO9 - GQ Pop in Military Quarters by Sex by Age
        # Universe: Pop in military quarters
        "PCO9": ["gqMilitaryTotal", "gqMilitaryTotal * sex",  "gqMilitaryTotal * sex * agecatPCO8"],

        # PCO10 - GQ Pop in Other Noninstitutional Facilities by Sex by Age
        # Universe: Pop in other noninstitutional facilities
        "PCO10": ["gqOtherNoninstTotal", "gqOtherNoninstTotal * sex", "gqOtherNoninstTotal * sex * agecatPCO7"],

        # PCO43A - GQ Pop by Sex by Age by Major GQ Type (White alone)
        # Universe: Pop in group quarters
         "PCO43A": [
             "whiteAlone * gqTotal",
             "whiteAlone * gqTotal * sex",
             "whiteAlone * gqTotal * sex * agecat43",
             "whiteAlone * institutionalized * sex * agecat43",
             "whiteAlone * majorGQs * sex * agecat43"
         ],

        # PCO43B - GQ Pop by Sex by Age by Major GQ Type (Black or African American alone)
        # Universe: Pop in group quarters
         "PCO43B": [
             "blackAlone * gqTotal",
             "blackAlone * gqTotal * sex",
             "blackAlone * gqTotal * sex * agecat43",
             "blackAlone * institutionalized * sex * agecat43",
             "blackAlone * majorGQs * sex * agecat43"
         ],

        # PCO43C - GQ Pop by Sex by Age by Major GQ Type (American Indian or Alaska Native alone)
        # Universe: Pop in group quarters
         "PCO43C": [
             "aianAlone * gqTotal",
             "aianAlone * gqTotal * sex",
             "aianAlone * gqTotal * sex * agecat43",
             "aianAlone * institutionalized * sex * agecat43",
             "aianAlone * majorGQs * sex * agecat43"
         ],

        # PCO43D - GQ Pop by Sex by Age by Major GQ Type (Asian alone)
        # Universe: Pop in group quarters
         "PCO43D": [
             "asianAlone * gqTotal",
             "asianAlone * gqTotal * sex",
             "asianAlone * gqTotal * sex * agecat43",
             "asianAlone * institutionalized * sex * agecat43",
             "asianAlone * majorGQs * sex * agecat43"
         ],

        # PCO43E - GQ Pop by Sex by Age by Major GQ Type (Native Hawaiian or Other Pacific Islander alone)
        # Universe: Pop in group quarters
         "PCO43E": [
             "nhopiAlone * gqTotal",
             "nhopiAlone * gqTotal * sex",
             "nhopiAlone * gqTotal * sex * agecat43",
             "nhopiAlone * institutionalized * sex * agecat43",
             "nhopiAlone * majorGQs * sex * agecat43"
         ],

        # PCO43F - GQ Pop by Sex by Age by Major GQ Type (Some Other Race alone)
        # Universe: Pop in group quarters
         "PCO43F": [
             "sorAlone * gqTotal",
             "sorAlone * gqTotal * sex",
             "sorAlone * gqTotal * sex * agecat43",
             "sorAlone * institutionalized * sex * agecat43",
             "sorAlone * majorGQs * sex * agecat43"
         ],

        # PCO43G - GQ Pop by Sex by Age by Major GQ Type (Two or More Races Alone)
        # Universe: Pop in group quarters
         "PCO43G": [
             "tomr * gqTotal",
             "tomr * gqTotal * sex",
             "tomr * gqTotal * sex * agecat43",
             "tomr * institutionalized * sex * agecat43",
             "tomr * majorGQs * sex * agecat43"
         ],

        # PCO43H - GQ Pop by Sex by Age by Major GQ Type (Hispanic or Latino)
        # Universe: Pop in group quarters
         "PCO43H": [
             "hispTotal * gqTotal",
             "hispTotal * gqTotal * sex",
             "hispTotal * gqTotal * sex * agecat43",
             "hispTotal * institutionalized * sex * agecat43",
             "hispTotal * majorGQs * sex * agecat43"
         ],

        # PCO43I - GQ Pop by Sex by Age by Major GQ Type (White Alone, Not Hispanic or Latino)
        # Universe: Pop in group quarters
        ### The [complement/partition of each race * not hispanic] of the universe might be useful ####
         "PCO43I": [
             "whiteAlone * notHispTotal * gqTotal",
             "whiteAlone * notHispTotal * gqTotal * sex",
             "whiteAlone * notHispTotal * gqTotal * sex * agecat43",
             "whiteAlone * notHispTotal * institutionalized * sex * agecat43",
             "whiteAlone * notHispTotal * majorGQs * sex * agecat43"
         ],

        # PCT12 - Sex by Age
        # Universe: Total Population
        "PCT12": ["total", "sex", "sex * agecatPCT12"],

        # PCT13
        # Universe: Population in households
        "PCT13": ["hhTotal", "hhTotal * sex", "hhTotal * sex * agecat"],

        # PCT22 - GQ Pop by Sex by Major GQ Type for Pop 18 Years and Over
        # Universe: Pop 18 years and over in group quarters
        "PCT22": ["over17yearsTotal * gqTotal", "over17yearsTotal * gqTotal * sex", "over17yearsTotal * sex * institutionalized",
                  "over17yearsTotal * sex * majorGQs"],

        # PCT13A
        # Universe: Population of White alone  in households
        "PCT13A": [
            "whiteAlone * hhTotal", "whiteAlone * hhTotal * sex", "whiteAlone * hhTotal * sex * agecat",
        ],

        # PCT13B
        # Universe: Population of Black alone in households
        "PCT13B": ["blackAlone * hhTotal", "blackAlone * hhTotal * sex", "blackAlone * hhTotal * sex * agecat",
        ],

        # PCT13C
        # Universe: Population of AIAN alone  in households
        "PCT13C": ["aianAlone * hhTotal", "aianAlone * hhTotal * sex", "aianAlone * hhTotal * sex * agecat",
        ],

        # PCT13D
        # Universe: Population of Asian alone  in households
        "PCT13D": ["asianAlone * hhTotal", "asianAlone * hhTotal * sex", "asianAlone * hhTotal * sex * agecat",
        ],

        # PCT13E
        # Universe: Population of Hawaiian/Pacific Islander alone  in households
        "PCT13E": ["nhopiAlone * hhTotal", "nhopiAlone * hhTotal * sex", "nhopiAlone * hhTotal * sex * agecat",
        ],

        # PCT13F
        # Universe: Population of Some other race alone  in households
        "PCT13F": ["sorAlone * hhTotal", "sorAlone * hhTotal * sex", "sorAlone * hhTotal * sex * agecat",
        ],

        # PCT13G
        # Universe: Population of Two or more races   in households
        "PCT13G": ["tomr * hhTotal", "tomr * hhTotal * sex", "tomr * hhTotal * sex * agecat",
        ],

        # PCT13H
        # Universe: Population of Hispanic or latino in households
        "PCT13H": ["hispTotal * hhTotal", "hispTotal * hhTotal * sex", "hispTotal * hhTotal * sex * agecat",
        ],

        # PCT13I
        # Universe: Population of White alone, non-hispanic in households
        "PCT13I": ["whiteAlone * notHispTotal * hhTotal", "whiteAlone * notHispTotal * hhTotal * sex",
            "whiteAlone * notHispTotal * hhTotal * sex * agecat"
        ],

    }

    return tabledict

def getTableDict_PL94_CVAP():
    tabledict = {
        ################################################
        # PL94 Tables
        ################################################
        
        # Table P1 - Race
        # Universe: Total Population
        "P1": [
            "total",
            "numraces",
            "cenrace"
        ],
        
        # Table P2 - Hispanic or Latino by Race
        # Universe: Total Population
        "P2": [
            "total",
            "hispanic",
            "numraces",
            "cenrace",
            "hispanic * numraces",
            "hispanic * cenrace"
        ],
        
        # Table P3 - Race for the Population 18 Years and over
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P3": [
            "votingage",
            "votingage * numraces",
            "votingage * cenrace"
        ],

        
        # Table P4 - Hispanic or Latino by Race for the Population 18 Years and over
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P4": [
            "votingage",
            "votingage * hispanic",
            "votingage * numraces",
            "votingage * cenrace",
            "votingage * hispanic * numraces",
            "votingage * hispanic * cenrace"
        ],
        
        # Table P42 - Group Quarters Population by GQ Type
        # Universe: Population in Group Quarters
        # Note: For the table, only 'gqTotal' is needed, given the universe,
        #       however, the complement of the universe implies that we
        #       might also want to know about households, so we'll use
        #       'household' to query BOTH the household total and gqTotal
        "P42": [
            "household",
            "institutionalized",
            "gqlevels"
        ],


        # Table P1_CVAP - Race by Citizenship
        # Universe: Total Population
        "P1_CVAP": [
            "total",
            "numraces",
            "cenrace",
            
            "citizen",
            "numraces * citizen",
            "cenrace * citizen"
        ],
        
        # Table P2_CVAP - Hispanic or Latino by Race by Citizenship
        # Universe: Total Population
        "P2_CVAP": [
            "total",
            "hispanic",
            "numraces",
            "cenrace",
            "hispanic * numraces",
            "hispanic * cenrace",

            "citizen",
            "hispanic * citizen",
            "numraces * citizen",
            "cenrace * citizen",
            "hispanic * numraces * citizen",
            "hispanic * cenrace * citizen"
        ],
        
        # Table P3_CVAP - Race for the Population 18 Years and over by Citizenship
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P3_CVAP": [
            "votingage",
            "votingage * numraces",
            "votingage * cenrace",

            "votingage * citizen",
            "votingage * numraces * citizen",
            "votingage * cenrace * citizen"
        ],

        
        # Table P4_CVAP - Hispanic or Latino by Race for the Population 18 Years and over by Citizenship
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P4_CVAP": [
            "votingage",
            "votingage * hispanic",
            "votingage * numraces",
            "votingage * cenrace",
            "votingage * hispanic * numraces",
            "votingage * hispanic * cenrace",
            
            "votingage * citizen",
            "votingage * hispanic * citizen",
            "votingage * numraces * citizen",
            "votingage * cenrace * citizen",
            "votingage * hispanic * numraces * citizen",
            "votingage * hispanic * cenrace * citizen"
        ],
    }
    
    return tabledict


def getTableDict_PL94_P12():
    tabledict = {
        ################################################
        # PL94 Tables
        ################################################
        
        "Geolevel_TVD": [
            "total",
            
            "hhgq",
            "sex",
            "agecat",
            "hispanic",
            "cenrace",
            
            "majorRaces",
            "numraces",
            "agecat * sex",
            "cenrace * hispanic",
            "numraces * hispanic",
            "votingage",
            
            "detailed"
        ],

        # Table P1 - Race
        # Universe: Total Population
        "P1": [
            "total",
            "numraces",
            "cenrace"
        ],
        
        # Table P2 - Hispanic or Latino by Race
        # Universe: Total Population
        "P2": [
            "total",
            "hispanic",
            "numraces",
            "cenrace",
            "hispanic * numraces",
            "hispanic * cenrace"
        ],
        
        # Table P3 - Race for the Population 18 Years and over
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P3": [
            "votingage",
            "votingage * numraces",
            "votingage * cenrace"
        ],

        
        # Table P4 - Hispanic or Latino by Race for the Population 18 Years and over
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P4": [
            "votingage",
            "votingage * hispanic",
            "votingage * numraces",
            "votingage * cenrace",
            "votingage * hispanic * numraces",
            "votingage * hispanic * cenrace"
        ],
        
        # Table P42 - Group Quarters Population by GQ Type
        # Universe: Population in Group Quarters
        # Note: For the table, only 'gqTotal' is needed, given the universe,
        #       however, the complement of the universe implies that we
        #       might also want to know about households, so we'll use
        #       'household' to query BOTH the household total and gqTotal
        "P42": [
            "household",
            "institutionalized",
            "gqlevels"
        ],
        
        # Need to add Table P12 and build a workload for PL94_P12
    }
    
    return tabledict

def getTableDict_PL94():
    tabledict = {
        ################################################
        # PL94 Tables
        ################################################

        "Geolevel_TVD": [
            "total",
            
            "hhgq",
            "votingage",
            "hispanic",
            "cenrace",
            
            "majorRaces",
            "numraces",
            "cenrace * hispanic",
            "numraces * hispanic",
            
            "detailed"
        ],
        
        # Table P1 - Race
        # Universe: Total Population
        "P1": [
            "total",
            "numraces",
            "cenrace"
        ],
        
        # Table P2 - Hispanic or Latino by Race
        # Universe: Total Population
        "P2": [
            "total",
            "hispanic",
            "numraces",
            "cenrace",
            "hispanic * numraces",
            "hispanic * cenrace"
        ],
        
        # Table P3 - Race for the Population 18 Years and over
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P3": [
            "votingage",
            "votingage * numraces",
            "votingage * cenrace"
        ],

        
        # Table P4 - Hispanic or Latino by Race for the Population 18 Years and over
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P4": [
            "votingage",
            "votingage * hispanic",
            "votingage * numraces",
            "votingage * cenrace",
            "votingage * hispanic * numraces",
            "votingage * hispanic * cenrace"
        ],
        
        # Table P42 - Group Quarters Population by GQ Type
        # Universe: Population in Group Quarters
        # Note: For the table, only 'gqTotal' is needed, given the universe,
        #       however, the complement of the universe implies that we
        #       might also want to know about households, so we'll use
        #       'household' to query BOTH the household total and gqTotal
        "P42": [
            "household",
            "institutionalized",
            "gqlevels"
        ],
    }
    
    return tabledict

def getTableDict(schema):
    assert schema in _tabledict, f"There are no tables listed for the schema '{schema}'."
    return _tabledict[schema]

_tabledict = {
    CC.SCHEMA_HOUSEHOLD2010: getTableDict_Household2010(),
    CC.SCHEMA_REDUCED_DHCP_HHGQ: getTableDict_DHCP_HHGQ(),
    CC.SCHEMA_PL94: getTableDict_PL94(),
    CC.SCHEMA_PL94_CVAP: getTableDict_PL94_CVAP(),
    CC.SCHEMA_PL94_P12: getTableDict_PL94_P12()
}

