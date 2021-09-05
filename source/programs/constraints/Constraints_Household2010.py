import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints

from constants import CC


class ConstraintsCreatorHousehold2010Generic(querybase_constraints.AbstractConstraintsCreator):

    # Used only in unit tests
    implemented = (
        "total",
        "no_vacant",
        "living_alone",
        "size2",
        "size3",
        "size4",
        "size2plus_notalone",
        "not_multigen",
        "hh_elderly",
        "age_child",
        "tot_hh",
        "gq_vect",
        "owned",
    )


    def total(self, name):
        """
        Total number of households is less than total number of housing units
        """
        self.checkInvariantPresence(name, ('tot_hu',))
        self.addToConstraintsDict(name, "total", self.invariants["tot_hu"].astype(int), "le")


    # Structural zeros

    # size related.
    def no_vacant(self, name):
        """ size=0 cannot happen """
        self.addToConstraintsDict(name, "vacant", np.array(0), "=")

    def living_alone(self, name):
        """
        if size = 1, hhtype=18 and multi=0
            age<5 iff elderly=0
            age=5 iff elderly=1
            age=6 iff elderly=2
            age>6 iff elderly=3z
        """
        self.addToConstraintsDict(f"{name}_gt1", (CC.HHSIZE_ONE, CC.HHTYPE_NOT_ALONE), np.array(0), "=") # rules out all hhtype but living alone
        self.addToConstraintsDict(f"{name}_multi", (CC.HHSIZE_ONE, CC.HHMULTI_TRUE), np.array(0), "=") # rules out multigenerational household
        self.addToConstraintsDict(f"{name}_eld0", (CC.HHSIZE_ONE, CC.HHTYPE_ALONE, CC.HHMULTI_FALSE, CC.HHAGE_UNDER60, CC.HHELDERLY_PRESENCE_OVER60), np.array(0), "=") # rules out hh under 60 and presence of 60+
        self.addToConstraintsDict(f"{name}_eld1", (CC.HHSIZE_ONE, CC.HHTYPE_ALONE, CC.HHMULTI_FALSE, CC.HHAGE_60TO64, CC.HHELDERLY_NO1UNDER60ORSOME1OVER64), np.array(0), "=")  # rules out hh between 60 to 64 and (presence of 65+ or no presence of 60+)
        self.addToConstraintsDict(f"{name}_eld2", (CC.HHSIZE_ONE, CC.HHTYPE_ALONE, CC.HHMULTI_FALSE, CC.HHAGE_65TO74, CC.HHELDERLY_NO1UNDER65ORSOME1OVER74), np.array(0), "=")  # similar for 65 to 74
        self.addToConstraintsDict(f"{name}_eld3", (CC.HHSIZE_ONE, CC.HHTYPE_ALONE, CC.HHMULTI_FALSE, CC.HHAGE_75PLUS, CC.HHELDERLY_PRESENCE_NOT_75PLUS), np.array(0), "=")  # similar for 75+


    def size2(self, name):
        r"""
        if size=2, hhtype \in {3,7,12,17,19,20,22,23} (other types imply at least 3 people in hhd) and multi=0
            if age<5 and elderly>0, hhtype is not 19 or 20
            if age=0 and elderly=3, hhtype must be 22 or 23
            if the second person is a child, elderly completely deterimined by hh age
        """
        self.addToConstraintsDict(f"{name}_gt2", (CC.HHSIZE_TWO, CC.HHTYPE_NOT_SIZE_TWO), np.array(0), "=") # rules out living alone and all types that imply at least 3 people
        self.addToConstraintsDict(f"{name}_multi", (CC.HHSIZE_TWO, CC.HHMULTI_TRUE), np.array(0), "=") # rules out multigenerational households
        self.addToConstraintsDict(f"{name}_eld0", (CC.HHSIZE_TWO, CC.HHTYPE_SIZE_TWO_WITH_CHILD, CC.HHAGE_UNDER60, CC.HHELDERLY_PRESENCE_OVER60), np.array(0), "=") # rules out hh under 60 and presence of 60+ and child under 18.
        self.addToConstraintsDict(f"{name}_eld1", (CC.HHSIZE_TWO, CC.HHTYPE_SIZE_TWO_WITH_CHILD, CC.HHAGE_60TO64, CC.HHELDERLY_NO1UNDER60ORSOME1OVER64), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld2", (CC.HHSIZE_TWO, CC.HHTYPE_SIZE_TWO_WITH_CHILD, CC.HHAGE_65TO74, CC.HHELDERLY_NO1UNDER65ORSOME1OVER74), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld3", (CC.HHSIZE_TWO, CC.HHTYPE_SIZE_TWO_WITH_CHILD, CC.HHAGE_75PLUS, CC.HHELDERLY_PRESENCE_NOT_75PLUS), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_le25_eld3", (CC.HHSIZE_TWO, CC.HHAGE_15TO24, CC.HHELDERLY_PRESENCE_75PLUS, CC.HHTYPE_SIZE_TWO_COUPLE), np.array(0), "=") # rules out hh under 25 and presence of 75+ and married/partner.

    def size3(self, name):
        r"""
        if size=3, hhtype \notin {2,6,10,15,18}
           if hhtype=21, must be householder with 2 children. multi=0, elderly completely detemined by hh age.
           if multi=1, must be hhtype \in {19, 20, 22}
        """
        #self.addToConstraintsDict(name, "size3 * yes_multi * type3_child_under_6 * ", np.array(0), "=") # multi gen plus child under 6 implies no grandchild. False not struct zero grandchild could be child of another child not living in house.
        self.addToConstraintsDict(f"{name}_le25_eld3", (CC.HHSIZE_THREE, CC.HHTYPE_SIZE_THREE_COUPLE_WITH_ONE_CHILD, CC.HHAGE_15TO24, CC.HHELDERLY_PRESENCE_75PLUS), np.array(0), "=")
        self.addToConstraintsDict(f"{name}_gt3", (CC.HHSIZE_THREE, CC.HHTYPE_NOT_SIZE_THREE), np.array(0), "=") # rules out living alone and all types that imply at least 4 people
        self.addToConstraintsDict(f"{name}_multi", (CC.HHSIZE_THREE, CC.HHMULTI_TRUE, CC.HHTYPE_SIZE_THREE_NOT_MULTI), np.array(0), "=") # rules out multigenerational and householder has spouse/partner.
        self.addToConstraintsDict(f"{name}_eld0", (CC.HHSIZE_THREE, CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN, CC.HHAGE_UNDER60, CC.HHELDERLY_PRESENCE_OVER60), np.array(0), "=") # rules out hh under 60 and presence of 60+ and 2 children under 18.
        self.addToConstraintsDict(f"{name}_eld1", (CC.HHSIZE_THREE, CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN, CC.HHAGE_60TO64, CC.HHELDERLY_NO1UNDER60ORSOME1OVER64), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld2", (CC.HHSIZE_THREE, CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN, CC.HHAGE_65TO74, CC.HHELDERLY_NO1UNDER65ORSOME1OVER74), np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld3", (CC.HHSIZE_THREE, CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN, CC.HHAGE_75PLUS, CC.HHELDERLY_PRESENCE_NOT_75PLUS), np.array(0), "=") # similar


    def size4(self, name):
        """
        Is there anything special? oh yes.
        if hhtype=2, must be spouse, 2 children so multi=0.
        """
        self.addToConstraintsDict(f"{name}_gt4", (CC.HHSIZE_FOUR, CC.HHTYPE_ALONE), np.array(0), "=")
        self.addToConstraintsDict(f"{name}_multi", (CC.HHSIZE_FOUR, CC.HHTYPE_SIZE_FOUR_NOT_MULTI, CC.HHMULTI_TRUE), np.array(0), "=")
        self.addToConstraintsDict(f"{name}_le25_eld3", (CC.HHSIZE_FOUR, CC.HHTYPE_SIZE_FOUR_COUPLE_WITH_TWO_CHILDREN, CC.HHAGE_15TO24, CC.HHELDERLY_PRESENCE_75PLUS), np.array(0), "=")



    def size2plus_notalone(self, name):
        """
        Can't be living alone if more than two people
        """
        self.addToConstraintsDict(f"{name}_gt2plus", (CC.HHSIZE_TWOPLUS, CC.HHTYPE_ALONE), np.array(0), "=")


    def not_multigen(self, name):
        """
        Household is multigen if the householder has both a child and a parent/parent-in-law or both a child and a grandchild.
        Child doesn't have to be under 18 so not many restrictions here.
        if multi=1, then hhtype \notin {12,17,18,23}
        """
        self.addToConstraintsDict(f"{name}_1", (CC.HHMULTI_TRUE, CC.HHTYPE_NOT_MULTI), np.array(0), "=")


    def age_child(self, name):
        """
        Male householder can't have children more than 69 yrs younger.
        Female householder cant' have children more than 50 yrs younger.
        if sex=male and age>=75, cannot have own children under 6 yrs
        if sex=female and age>=75, cannot have own children under 18 yrs.
        if sex=female and age>=60, cannot have own children under 6yrs.
        """
        # self.addToConstraintsDict(f"{name}_1", (CC.HHSEX_MALE, CC.HHAGE_75PLUS, CC.HHTYPE_OWNCHILD_UNDERSIX), np.array(0), "=")
        # self.addToConstraintsDict(f"{name}_2", (CC.HHSEX_FEMALE, CC.HHAGE_75PLUS, CC.HHTYPE_OWNCHILD_UNDER18), np.array(0), "=")
        # self.addToConstraintsDict(f"{name}_3", (CC.HHSEX_FEMALE, CC.HHAGE_60PLUS, CC.HHTYPE_OWNCHILD_UNDERSIX), np.array(0), "=")
        pass

    def hh_elderly(self, name):
        """
        In general, householder age partially defines elderly.
        """
        self.addToConstraintsDict(f"{name}_eld3", (CC.HHAGE_75PLUS, CC.HHELDERLY_PRESENCE_NOT_75PLUS), np.array(0), "=")
        self.addToConstraintsDict(f"{name}_2", (CC.HHAGE_65TO74, CC.HHELDERLY_PRESENCE_NOT_65PLUS), np.array(0), "=")
        self.addToConstraintsDict(f"{name}_3", (CC.HHAGE_60TO64, CC.HHELDERLY_PRESENCE_NOT_60PLUS), np.array(0), "=")

    def total_ub(self, name):
        """
        upper bound on total as derived from housing unit invariant
        """
        pass

    def population_ub(self, name):
        """
        sum of size s * number of size s <= pop where ever pop is an invariant
        """
        pass

    def population_lb(self, name):
        pass

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


class ConstraintsCreator(ConstraintsCreatorHousehold2010Generic):
    """
       This class is what is used to create constraint objects from what is listed in the config file, for household schema
       see base class for keyword arguments description
       Schema: sex, age, hisp, race, size, hhtype, elderly, multi
       Schema dims: 2, 9, 2, 7, 8, 24, 4, 2
       """

    schemaname = CC.SCHEMA_HOUSEHOLD2010

    # Used only in unit tests
    implemented = (
        "total",
        "no_vacant",
        "living_alone",
        "size2",
        "size3",
        "size4",
        "size2plus_notalone",
        "not_multigen",
        "hh_elderly",
        "age_child",
    )
