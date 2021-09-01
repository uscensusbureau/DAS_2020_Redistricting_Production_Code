"""
Constraints for DHCP schema.
ConstraintCreator class is an actual DHCP constraint creator.

ConstraintCreator inherits from ConstraintsCreatorDHCGeneric the actual methods creating constraints,
but has its own __init__
"""
import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints

from constants import CC


class ConstraintsCreatorDHCGeneric(querybase_constraints.ConstraintsCreatorwHHGQ):
    """
        This class implements creation of constraints for DHCP2020 (official 2020 MDF DHC Persons file schema)
    """

    
    #######################################
    # New Structural Zeros
    #  Based on Tier 2 (Single relation) constraints 
    #  provided by William in DHC Full Specification (Single Pass) issue
    # TODO: Needs review
    #######################################
    def relgq0_lt15(self, name):
        """
            Structural zero: No <15 ages as Householder living alone
        """
        self.addToConstraintsDict(name, ("relgq0", CC.AGE_LT15_TOTAL), np.array(0), "=")

    def relgq1_lt15(self, name):
        """
            Structural zero: No <15 ages as Householder not living alone
        """
        self.addToConstraintsDict(name, ("relgq1", CC.AGE_LT15_TOTAL), np.array(0), "=")
        
    def relgq2_lt15(self, name):
        """
            Structural zero: No <15 ages as Opposite-sex husband/wife/spouse
        """
        self.addToConstraintsDict(name, ("relgq2", CC.AGE_LT15_TOTAL), np.array(0), "=")
        
    def relgq3_lt15(self, name):
        """
            Structural zero: No <15 ages as Opposite-sex unmarried partner
        """
        self.addToConstraintsDict(name, ("relgq3", CC.AGE_LT15_TOTAL), np.array(0), "=")

    def relgq4_lt15(self, name):
        """
            Structural zero: No <15 ages as Same-sex husband/wife/spouse
        """
        self.addToConstraintsDict(name, ("relgq4", CC.AGE_LT15_TOTAL), np.array(0), "=")

    def relgq5_lt15(self, name):
        """
            Structural zero: No <15 ages as Same-sex unmarried partner
        """
        self.addToConstraintsDict(name, ("relgq5", CC.AGE_LT15_TOTAL), np.array(0), "=")

    def relgq6_gt89(self, name):
        """
            Structural zero: No >89 ages as Biological son or daughter
        """
        self.addToConstraintsDict(name, ("relgq6", CC.AGE_GT89_TOTAL), np.array(0), "=")
        
    def relgq7_gt89(self, name):
        """
            Structural zero: No >89 ages as Adopted son or daughter
        """
        self.addToConstraintsDict(name, ("relgq7", CC.AGE_GT89_TOTAL), np.array(0), "=")
        
    def relgq8_gt89(self, name):
        """
            Structural zero: No >89 ages as Stepson / Stepdaughter
        """
        self.addToConstraintsDict(name, ("relgq8", CC.AGE_GT89_TOTAL), np.array(0), "=")
        
    ### Code 9 - Brother or Sister - spans the full age range - no constraint needed.

    ### Code 10 - Father or mother (first, second, third, ...?) - How do they differ and do we need them all as constraints?
    # WNS: for our purposes we only need to apply the bounds to the "father or mother" RELSHIP category which does not distinguish between first, second, etc. So this is handled correctly as-is.

    def relgq10_lt30(self, name):
        """
            Structural zero: No <30 ages as parents (father or mother)
        """
        self.addToConstraintsDict(name, ("relgq10", CC.AGE_LT30_TOTAL), np.array(0), "=")
    
    def relgq11_gt74(self, name):
        """
            Structural zero: No >74 ages as grandchildren
        """
        self.addToConstraintsDict(name, ("relgq11", CC.AGE_GT74_TOTAL), np.array(0), "=")
        
    def relgq12_lt30(self, name):
        """
            Structural zero: No <30 ages as parent-in-law (father or mother)
        """
        self.addToConstraintsDict(name, ("relgq12", CC.AGE_LT30_TOTAL), np.array(0), "=")
        
    def relgq13_lt15_gt89(self, name):
        """
            Structural zero: No <15 and >89 ages as Son-in-law / Daughter-in-law
        """
        self.addToConstraintsDict(name, ("relgq13", CC.AGE_LT15_GT89_TOTAL), np.array(0), "=")
        
    ### Code 14 - Other Relative - spans the full age range - no constraint needed.
    
    ### Code 15 - Housemate/Roommate - spans the full age range - no constraint needed.
    
    def relgq16_gt20(self, name):
        """
            Structural zero: No >20 ages as Foster Child
        """
        self.addToConstraintsDict(name, ("relgq16", CC.AGE_GT20_TOTAL), np.array(0), "=")
        
    ### Code 17 - Other Nonrelative - Are these the correct min/max values? Do we need a constraint if they span the full age range?
        
    def relgq18_lt15(self, name):
        """
            Structural zero: No <15 ages in GQ 101 Federal Detention Centers
        """
        self.addToConstraintsDict(name, ("relgq18", CC.AGE_LT15_TOTAL), np.array(0), "=")
        
    def relgq19_lt15(self, name):
        """
            Structural zero: No <15 ages in GQ 102 Federal Prisons
        """
        self.addToConstraintsDict(name, ("relgq19", CC.AGE_LT15_TOTAL), np.array(0), "=")
    
    def relgq20_lt15(self, name):
        """
            Structural zero: No <15 ages in GQ 103 State Prisons
        """
        self.addToConstraintsDict(name, ("relgq20", CC.AGE_LT15_TOTAL), np.array(0), "=")
        
    def relgq21_lt15(self, name):
        """
            Structural zero: No <15 ages in GQ 104 Local Jails and other Municipal Confinements
        """
        self.addToConstraintsDict(name, ("relgq21", CC.AGE_LT15_TOTAL), np.array(0), "=")
        
    def relgq22_lt15(self, name):
        """
            Structural zero: No <15 ages in GQ 105 Correctional Residential Facilities
        """
        self.addToConstraintsDict(name, ("relgq22", CC.AGE_LT15_TOTAL), np.array(0), "=")
        
    def relgq23_lt17_gt65(self, name):
        """
            Structural zero: No <17 and >65 ages in GQ 106 Military Disciplinary Barracks
        """
        self.addToConstraintsDict(name, ("relgq23", CC.AGE_LT17GT65_TOTAL), np.array(0), "=")
    
    def relgq24_gt25(self, name):
        """
            Structural zero: No >25 ages in GQ 201 Group Homes for Juveniles
        """
        self.addToConstraintsDict(name, ("relgq24", CC.AGE_GT25_TOTAL), np.array(0), "=")
        
    def relgq25_gt25(self, name):
        """
            Structural zero: No >25 ages in GQ 202 Residential Treatment Centers for Juveniles
        """
        self.addToConstraintsDict(name, ("relgq25", CC.AGE_GT25_TOTAL), np.array(0), "=")

    def relgq26_gt25(self, name):
        """
            Structural zero: No >25 ages in GQ 203 Correctional Facilities intended for Juveniles
        """
        self.addToConstraintsDict(name, ("relgq26", CC.AGE_GT25_TOTAL), np.array(0), "=")
        
    def relgq27_lt20(self, name):
        """
            Structural zero: No <20 ages in GQ 301 Nursing Facilites
        """
        self.addToConstraintsDict(name, ("relgq27", CC.AGE_LT20_TOTAL), np.array(0), "=")
        
    ### Code 28 - GQ 401 Mental Hospitals - Spans full age range, no constraints needed.
    
    ### Code 29 - GQ 402 Hospitals with patients who have no usual home elsewhere - Spans full age range, no constraints needed.

    ### Code 30 - GQ 403 In-patient Hospice Facilities - Spans full age range, no constraints needed.
    
    def relgq31_lt17_gt65(self, name):
        """
            Structural zero: No <17 and >65 ages in GQ 404 Military Treatment Facilities
        """
        self.addToConstraintsDict(name, ("relgq31", CC.AGE_LT17GT65_TOTAL), np.array(0), "=")
    
    def relgq32_lt3_gt30(self, name):
        """
            Structural zero: No <3 and >30 ages in GQ 405 Residential schools for people with disabilities
        """
        self.addToConstraintsDict(name, ("relgq32", CC.AGE_LT3_GT30_TOTAL), np.array(0), "=")
        
    def relgq33_lt16_gt65(self, name):
        """
            Structural zero: No <16 and >65 ages in College / University Student Housing
        """
        self.addToConstraintsDict(name, ("relgq33", CC.AGE_LT16GT65_TOTAL), np.array(0), "=")

    def relgq34_lt17_gt65(self, name):
        """
            Structural zero: No <17 and >65 ages in GQ 601 Military Quarters
        """
        self.addToConstraintsDict(name, ("relgq34", CC.AGE_LT17GT65_TOTAL), np.array(0), "=")

    def relgq35_lt17_gt65(self, name):
        """
            Structural zero: No <17 and >65 ages in GQ 602 Military Ships
        """
        self.addToConstraintsDict(name, ("relgq35", CC.AGE_LT17GT65_TOTAL), np.array(0), "=")
        
    ### Code 36 - GQ 701 Emergency and transitional shelters - Spans full age range, no constraints needed.
    
    def relgq37_lt16(self, name):
        """
            Structural zero: No <16 ages in GQ 801 Group homes intended for adults
        """
        self.addToConstraintsDict(name, ("relgq37", CC.AGE_LT16_TOTAL), np.array(0), "=")
        
    def relgq38_lt16(self, name):
        """
            Structural zero: No <16 ages in GQ 802 Residential Treatment Centers for Adults
        """
        self.addToConstraintsDict(name, ("relgq38", CC.AGE_LT16_TOTAL), np.array(0), "=")

    def relgq39_lt16_gt75(self, name):
        """
            Structural zero: No <16 and >75 ages in GQ 900 Maritime / Merchant Vessels
        """
        self.addToConstraintsDict(name, ("relgq39", CC.AGE_LT16_GT75_TOTAL), np.array(0), "=")

    def relgq40_lt16_gt75(self, name):
        """
            Structural zero: No <16 and >75 ages in GQ 901 Workers' group living quarters and job corps centers
        """
        self.addToConstraintsDict(name, ("relgq40", CC.AGE_LT16_GT75_TOTAL), np.array(0), "=")

    ### Code 41 - GQ 997 Other Noninstitutional (GQ Types 702, 704, 706, 903, 904) - Spans full age range; no constraint needed.    
    

    #######################################
    # End of New Structural Zeros
    #######################################
    
    # def hhgq1_lessthan15(self, name):
    #     """
    #     Structural zero: No <15 ages in adult correctional facilities
    #     """
    #     self.addToConstraintsDict(name, (CC.HHGQ_CORRECTIONAL_TOTAL, CC.AGE_LT15_TOTAL), np.array(0), "=")

    # def hhgq2_greaterthan25(self, name):
    #     """
    #     Structural zero: No >25 ages in juvenile facilities
    #     """
    #     self.addToConstraintsDict(name, (CC.HHGQ_JUVENILE_TOTAL, CC.AGE_GT25_TOTAL), np.array(0), "=")

    # def hhgq3_lessthan20(self, name):
    #     """
    #     Structural zero: No <20 ages in nursing facilities
    #     """
    #     self.addToConstraintsDict(name, (CC.HHGQ_NURSING_TOTAL, CC.AGE_LT20_TOTAL), np.array(0), "=")

    # #No age restrictions for other institutional facilities
    
    # def hhgq5_lt16gt65(self, name):
    #     """
    #     Structural zero: No <16 or >65 ages in college housing
    #     """
    #     self.addToConstraintsDict(name, (CC.HHGQ_COLLEGE_TOTAL, CC.AGE_LT16GT65_TOTAL), np.array(0), "=")

    # def hhgq6_lt17gt65(self, name):
    #     """
    #     Structural zero: No <17 or >65 ages in military housing
    #     """
    #     self.addToConstraintsDict(name, (CC.HHGQ_MILITARY_TOTAL, CC.AGE_LT17GT65_TOTAL), np.array(0), "=")

    # #No age restrictions for other non-institutional facilities

    def setHhgqDimsAndCaps(self):
        """
        Set dimension of hhgq axis and caps
        """
        #super().setHhgqDimsAndCaps()


        # look for the shape of the histogram for the recode that has all relationship-to-householder levels
        # collapsed into a single level, as well as all GQ types
        self.gqt_dim = self.schema.getQueryShape(CC.HHGQ_GQLEVELS)[0]
        self.hhgq_cap = np.repeat(99999, self.gqt_dim)

        # low bound
        # 1 person per building (TODO: we don't have vacant in the histogram, it's going to change)
        self.hhgq_lb = np.repeat(1, self.gqt_dim)

        # households include vacant housing units, so lower bound is 0
        self.hhgq_lb[0] = 0

    # def nurse_nva_0(self, name):
    #     """
    #     Structural zero: no minors in nursing facilities (GQ code 301)
    #     """
    #     self.addToConstraintsDict(name, (CC.HHGQ_NURSING_TOTAL, CC.NONVOTING_TOTAL), np.array(0), "=")

    def total(self, name):
        """
        Total population per geounit must remain invariant
        """
        self.checkInvariantPresence(name, ('tot',))
        self.addToConstraintsDict(name, "total", self.invariants["tot"].astype(int), "=")

    def hhgq_total_lb(self, name):
        """
        Lower bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=False)
        self.addToConstraintsDict(name, CC.HHGQ_GQLEVELS, rhs, "ge")

    def hhgq_total_ub(self, name):
        """
        Upper bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=True)
        self.addToConstraintsDict(name, CC.HHGQ_GQLEVELS, rhs, "le")

    def householder_ub(self, name):
        """
            Upper bound the number of householders by the total number of housing units.
        """
        self.checkInvariantPresence(name, ("gqhh_vect",))
        rhs = self.invariants["gqhh_vect"][0:1].astype(int) # should be total number of housing units.
        self.addToConstraintsDict(name, CC.RELGQ_HOUSEHOLDERS, rhs, "le")

    def spousesUnmarriedPartners_ub(self, name):
        """
            #Spouses_of_Householders + #Unmarried_Partners_of_Householders <= #Housing_Units
        """
        self.checkInvariantPresence(name, ("gqhh_vect",))
        rhs = self.invariants["gqhh_vect"][0:1].astype(int)
        self.addToConstraintsDict(name, CC.SPOUSES_UNMARRIED_PARTNERS, rhs, "le")

    # def partners_ub(self, name):
    #     """ Number of spouses and partners <= total number of housing units"""
    #     self.checkInvariantPresence(name, ("gqhh_vect",))
    #     rhs = self.invariants["gqhh_vect"][0:1].astype(int)
    #     self.addToConstraintsDict(name, CC.RELGQ_SPOUSE_OR_PARTNER, rhs, "le")

    def people100Plus_ub(self, name):
        """
            #People_in_Households_over_100_Years_Old <= 2 * #Housing_Units
        """
        self.checkInvariantPresence(name, ("gqhh_vect",))
        rhs = 2*self.invariants["gqhh_vect"][0:1].astype(int)
        self.addToConstraintsDict(name, (CC.PEOPLE_IN_HOUSEHOLDS, CC.AGE_ONLY_100_PLUS), rhs, "le")

    def parents_ub(self, name):
        """
            #Fathers_or_Mothers_of_the_Householders <= 2 * #Housing_Units
        """
        self.checkInvariantPresence(name, ("gqhh_vect",))
        rhs = 2*self.invariants["gqhh_vect"][0:1].astype(int)
        self.addToConstraintsDict(name, "relgq10", rhs, "le")

    def parentInLaws_ub(self, name):
        """
            #Parent_in_Laws_of_the_Householders <= 2 * #Housing_Units
        """
        self.checkInvariantPresence(name, ("gqhh_vect",))
        rhs = 2*self.invariants["gqhh_vect"][0:1].astype(int)
        self.addToConstraintsDict(name, "relgq12", rhs, "le")




class ConstraintsCreator(ConstraintsCreatorDHCGeneric):
    """
        This class is what is used to create constraint objects from what is listed in the config file, for DHCP2020 schema
        (official 2020 MDF Group 1 products schema)
        See base class for keyword arguments description.

        The variables in DHCP2020 are:

                        relgq sex age hispanic cenrace

        ( schema:  {'relgq': 0, 'sex': 1, 'age': 2, 'hispanic': 3, 'cenrace': 4} )

        With ranges: 0-41, 0-1, 0-115, 0-1, 0-62 respectively

        Thus, the histograms in use are 5-dimensional, with dimensions (42,2,116,2,63)
    """
    # Used only in unit tests
    implemented = (
        "total",
        "hhgq_total_lb",
        "hhgq_total_ub",
        "spousesUnmarriedPartners_ub",
        #"partners_ub",
        "people100Plus_ub",
        "parents_ub",
        "parentInLaws_ub",
        "householder_ub",


        ### New Structural zeros (for RELGQ variable)
        'relgq0_lt15',
        'relgq1_lt15',
        'relgq2_lt15',
        'relgq3_lt15',
        'relgq4_lt15',
        'relgq5_lt15',
        'relgq6_gt89',
        'relgq7_gt89',
        'relgq8_gt89',
        # 'relgq9_',
        'relgq10_lt30',
        'relgq11_gt74',
        'relgq12_lt30',
        'relgq13_lt15_gt89',
        # 'relgq14_',
        # 'relgq15_',
        'relgq16_gt20',
        # 'relgq17_',
        'relgq18_lt15',
        'relgq19_lt15',
        'relgq20_lt15',
        'relgq21_lt15',
        'relgq22_lt15',
        'relgq23_lt17_gt65',
        'relgq24_gt25',
        'relgq25_gt25',
        'relgq26_gt25',
        'relgq27_lt20',
        # 'relgq28_',
        # 'relgq29_',
        # 'relgq30_',
        'relgq31_lt17_gt65',
        'relgq32_lt3_gt30',
        'relgq33_lt16_gt65',
        'relgq34_lt17_gt65',
        'relgq35_lt17_gt65',
        # 'relgq36_',
        'relgq37_lt16',
        'relgq38_lt16',
        'relgq39_lt16_gt75',
        'relgq40_lt16_gt75',
        # 'relgq41_'

        # "nurse_nva_0",
        # "hhgq1_lessthan15",
        # "hhgq2_greaterthan25",
        # "hhgq3_lessthan20",
        # "hhgq5_lt16gt65",
        # "hhgq6_lt17gt65",
    )
    # attr_hhgq = CC.ATTR_HHGQ_UNIT_DHCP -- renamed it to CC.HHGQ, no longer needed
    schemaname = CC.SCHEMA_DHCP
