import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints

from constants import CC


class ConstraintsCreator(querybase_constraints.AbstractConstraintsCreator):
    """
       This class is what is used to create constraint objects from what is listed in the config file, for household schema
       see base class for keyword arguments description
       Schema: HH_SEX, HH_AGE, HH_HISPANIC, HH_RACE, HH_ELDERLY, HH_TENSHORT, HH_HHTYPE
       Schema dims: 2, 9, 2, 7, 4, 2, 522
    """

    # Used only in unit tests
    implemented = (
        "total",
        # TODO: implement structural zeros
        # "no_vacant",
        "living_alone",
        "size2",
        "size3",
        "size4",
        # "size2plus_notalone",
        # "not_multigen",
        "hh_elderly",
        "age_child",
        "tot_hh",
        "gq_vect",
        "owned",
    )

    schemaname = CC.SCHEMA_DHCH

    def total(self, name):
        """
        Total number of households is less than total number of housing units
        """
        self.checkInvariantPresence(name, ('tot_hu',))
        self.addToConstraintsDict(name, "total", self.invariants["tot_hu"].astype(int), "le")

    def tot_hh(self, name):
        """
        Total number of households is equal to total number of units in the unit table that are tenured
        (Implemented as the difference is equal to zero)
        """
        main_query = self.schema.getQuery("total")
        unit_query = self.unit_schema.getQuery(CC.HHGQ_HOUSEHOLD_TOTAL)
        self.addToConstraintsDictMultiHist(name, (main_query, unit_query), (1, -1), np.array(0), "=")

    def gq_vect(self, name):
        """
        Total number of housing units (tenured + vacant) in the units table is invariant, as are each gq
        """
        self.checkInvariantPresence(name, ('gqhh_vect',))
        self.addUnitConstraintsToDict(name, CC.HHGQ_UNIT_VECTOR, self.invariants["gqhh_vect"].astype(int), "=")

    def owned(self, name):
        main_query = self.schema.getQuery(CC.TEN_2LEV)
        unit_query = self.unit_schema.getQuery(CC.TEN_2LEV)
        self.addToConstraintsDictMultiHist(name, (main_query, unit_query), (1, -1), np.array([0,0]), "=")


    # TODO: Size related. Needs to be updated for new HHTYPE attribute
    # Structural zeros

    # def no_vacant(self, name):
    #     """ size=0 cannot happen """
    #     # Not needed, size is a part of HHTYPE and 0 is not allowedZZ
    #     self.addToConstraintsDict(name, "vacant", np.array(0), "=")

    def living_alone(self, name):
        """
        if size = 1, then elderly presence is defined by the householder age only.
        HH under 60 => can't be anyone over 60
        HH between 60 and 64 => cannot be elderly over 64 present
        HH between 65 and 74 => cannot be elderly below 65 or above 74 present
        HH 75+ => cannot be elderly below 75 present

        Also if there are children, that means the householder is a child and so there's limit on householder age
        """
        # Not needed, HHTYPE includes size now
        # self.addToConstraintsDict(f"{name}_gt1", (CC.HHSIZE_ONE, CC.HHTYPE_NOT_ALONE), np.array(0), "=")  # rules out all hhtype but living alone

        # Not needed, HHTYPE includes multi now
        # self.addToConstraintsDict(f"{name}_multi", (CC.HHSIZE_ONE, CC.HHMULTI_TRUE), np.array(0), "=") # rules out multigenerational household

        # Remove ALONE and MULTI from the cross
        self.addToConstraintsDict(f"{name}_eld0", (CC.HHSIZE_ONE, CC.HHAGE_UNDER60, CC.HHELDERLY_PRESENCE_OVER60), np.array(0), "=")     # rules out hh under 60 and presence of 60+
        self.addToConstraintsDict(f"{name}_eld1", (CC.HHSIZE_ONE, CC.HHAGE_60TO64, CC.HHELDERLY_NO1UNDER60ORSOME1OVER64), np.array(0), "=")  # rules out hh between 60 to 64 and (presence of 65+ or no presence of 60+)
        self.addToConstraintsDict(f"{name}_eld2", (CC.HHSIZE_ONE, CC.HHAGE_65TO74, CC.HHELDERLY_NO1UNDER65ORSOME1OVER74), np.array(0), "=")  # similar for 65 to 74
        self.addToConstraintsDict(f"{name}_eld3", (CC.HHSIZE_ONE, CC.HHAGE_75PLUS, CC.HHELDERLY_PRESENCE_NOT_75PLUS), np.array(0), "=")  # similar for 75+

        self.addToConstraintsDict(f"{name}_childalone", (CC.HHTYPE_CHILD_ALONE, CC.HHAGE_OVER24), np.array(0), "=")  # Householder is a child < 18, no  HHAGE > 24


    def size2(self, name):
        """
        if size=2, and the second person is a child, elderly completely deterimined by hh age,
        the same as for living_alone structural zeros above

        Also, exclude 50+ years difference between partners
        """
        # Not needed, HHTYPE includes SIZE now
        # self.addToConstraintsDict(f"{name}_gt2", (CC.HHSIZE_TWO, CC.HHTYPE_NOT_SIZE_TWO), np.array(0), "=") # rules out living alone and all types that imply at least 3 people
        # Not needed, HHTYPE includes MULTI now
        # self.addToConstraintsDict(f"{name}_multi", (CC.HHSIZE_TWO, CC.HHMULTI_TRUE), np.array(0), "=") # rules out multigenerational households

        # Remove SIZE2 since it's included in SIZE2WITHCHILD or HHTYPE_SIZE_TWO_COUPLE
        self.addToConstraintsDict(f"{name}_eld0", (CC.HHTYPE_SIZE_TWO_WITH_CHILD_EXCLUDING_HH, CC.HHAGE_UNDER60, CC.HHELDERLY_PRESENCE_OVER60), np.array(0), "=") # rules out hh under 60 and presence of 60+ and child under 18.
        self.addToConstraintsDict(f"{name}_eld1", (CC.HHTYPE_SIZE_TWO_WITH_CHILD, CC.HHAGE_60TO64, CC.HHELDERLY_NO1UNDER60ORSOME1OVER64), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld2", (CC.HHTYPE_SIZE_TWO_WITH_CHILD, CC.HHAGE_65TO74, CC.HHELDERLY_NO1UNDER65ORSOME1OVER74), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld3", (CC.HHTYPE_SIZE_TWO_WITH_CHILD, CC.HHAGE_75PLUS, CC.HHELDERLY_PRESENCE_NOT_75PLUS), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_le25_eld3", (CC.HHTYPE_SIZE_TWO_COUPLE, CC.HHAGE_15TO24, CC.HHELDERLY_PRESENCE_75PLUS), np.array(0), "=") # rules out hh under 25 and presence of 75+ and married/partner.

    def size3(self, name):
        """
        if size=3,
        the same as size=2, but two children (third person is a child)
        """
        #self.addToConstraintsDict(name, "size3 * yes_multi * type3_child_under_6 * ", np.array(0), "=") # multi gen plus child under 6 implies no grandchild. False not struct zero grandchild could be child of another child not living in house.
        # Remove SIZE3 since it's included in HHTYPE_SIZE_THREE_COUPLE_WITH_ONE_CHILD
        self.addToConstraintsDict(f"{name}_le25_eld3", (CC.HHTYPE_SIZE_THREE_COUPLE_WITH_ONE_CHILD, CC.HHAGE_15TO24, CC.HHELDERLY_PRESENCE_75PLUS), np.array(0), "=")

        # Not needed HHTYPE includes SIZE
        # self.addToConstraintsDict(f"{name}_gt3",   (CC.HHSIZE_THREE, CC.HHTYPE_NOT_SIZE_THREE), np.array(0), "=") # rules out living alone and all types that imply at least 4 people

        # Not needed HHTYPE includes SIZE and MULTI
        # self.addToConstraintsDict(f"{name}_multi", (CC.HHSIZE_THREE, CC.HHMULTI_TRUE, CC.HHTYPE_SIZE_THREE_NOT_MULTI), np.array(0), "=") # rules out multigenerational and householder has spouse/partner.

        # Remove SIZE3 since it's included in CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN
        # For the ones below, knowing that there are 2 children is only possible if they are in both ranges. There is only 1 HHTYPE for that, 436.
        # There are 8 more HHTYPEs for 1 parent with at least 1 child, but no variable to tell is the 3rd person is not elderly.
        self.addToConstraintsDict(f"{name}_eld0",  (CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN, CC.HHAGE_UNDER60, CC.HHELDERLY_PRESENCE_OVER60), np.array(0), "=") # rules out hh under 60 and presence of 60+ and 2 children under 18.
        self.addToConstraintsDict(f"{name}_eld1",  (CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN, CC.HHAGE_60TO64, CC.HHELDERLY_NO1UNDER60ORSOME1OVER64), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld2",  (CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN, CC.HHAGE_65TO74, CC.HHELDERLY_NO1UNDER65ORSOME1OVER74), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld3",  (CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN, CC.HHAGE_75PLUS, CC.HHELDERLY_PRESENCE_NOT_75PLUS), np.array(0), "=") # similar


    def size4(self, name):
        """
        If a couple with two children, can exclude 50+ years difference in the couple
        (same as size 3 and 2, but plus one child as the 4th person)
        """

        # Not needed, HHTYPE for ALONE have SIZE == 1
        # self.addToConstraintsDict(f"{name}_gt4", (CC.HHSIZE_FOUR, CC.HHTYPE_ALONE), np.array(0), "=")

        # Not needed, HHTYPE includes MULTI
        # self.addToConstraintsDict(f"{name}_multi", (CC.HHSIZE_FOUR, CC.HHTYPE_SIZE_FOUR_NOT_MULTI, CC.HHMULTI_TRUE), np.array(0), "=")

        # Removed SIZE4 since its in HHTYPE_SIZE_FOUR_COUPLE_WITH_TWO_CHILDREN
        self.addToConstraintsDict(f"{name}_le25_eld3", (CC.HHTYPE_SIZE_FOUR_COUPLE_WITH_TWO_CHILDREN, CC.HHAGE_15TO24, CC.HHELDERLY_PRESENCE_75PLUS), np.array(0), "=")



    # def size2plus_notalone(self, name):
    #     """
    #     Can't be living alone if more than two people
    #     """
    #     # Not needed, HHTYPE includes both ALONE and SIZE
    #     # self.addToConstraintsDict(f"{name}_gt2plus", (CC.HHSIZE_TWOPLUS, CC.HHTYPE_ALONE), np.array(0), "=")


    # def not_multigen(self, name):
    #     """
    #     Household is multigen if the householder has both a child and a parent/parent-in-law or both a child and a grandchild.
    #     Child doesn't have to be under 18 so not many restrictions here.
    #     if multi=1, then hhtype \notin {12,17,18,23}
    #     """
    #     # Not needed, MULTI is included in HHTYPE
    #     # self.addToConstraintsDict(f"{name}_1", (CC.HHMULTI_TRUE, CC.HHTYPE_NOT_MULTI), np.array(0), "=")


    def age_child(self, name):
        """
        Male householder can't have children more than 69 yrs younger.
        Female householder cant' have children more than 50 yrs younger.
        if sex=male and age>=75, cannot have own children under 6 yrs
        if sex=female and age>=75, cannot have own children under 18 yrs.
        if sex=female and age>=60, cannot have own children under 6yrs.
        """
        self.addToConstraintsDict(f"{name}_1", (CC.HHSEX_MALE,   CC.HHAGE_75PLUS,  CC.HHTYPE_OWNCHILD_UNDERSIX), np.array(0), "=")
        self.addToConstraintsDict(f"{name}_2", (CC.HHSEX_FEMALE, CC.HHAGE_75PLUS,  CC.HHTYPE_OWNCHILD_UNDER18),  np.array(0), "=")
        self.addToConstraintsDict(f"{name}_3", (CC.HHSEX_FEMALE, CC.HHAGE_60PLUS,  CC.HHTYPE_OWNCHILD_UNDERSIX), np.array(0), "=")
        pass

    def hh_elderly(self, name):
        """
        In general, householder age partially defines elderly.
        If householder is elderly, then the ELDERLY variable cannot be for age lower than HHAGE
        """
        self.addToConstraintsDict(f"{name}_eld3", (CC.HHAGE_75PLUS, CC.HHELDERLY_PRESENCE_NOT_75PLUS), np.array(0), "=")
        self.addToConstraintsDict(f"{name}_2", (CC.HHAGE_65TO74, CC.HHELDERLY_PRESENCE_NOT_65PLUS), np.array(0), "=")
        self.addToConstraintsDict(f"{name}_3", (CC.HHAGE_60TO64, CC.HHELDERLY_PRESENCE_NOT_60PLUS), np.array(0), "=")

    # def total_ub(self, name):
    #     """
    #     upper bound on total as derived from housing unit invariant
    #     """
    #     # Implemented under the name "total"
    #     pass


    # def population_ub(self, name):
    #     """
    #     sum of size s * number of size s <= pop where ever pop is an invariant
    #     """
    #     # TODO: Do we want this one? We don't ingest person tables for household runs now
    #     pass
    #
    # def population_lb(self, name):
    #     # TODO: Same as above + where to get the lb
    #     pass