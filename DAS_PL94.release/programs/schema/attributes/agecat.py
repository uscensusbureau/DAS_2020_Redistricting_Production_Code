from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class AgecatAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_AGECAT

    @staticmethod
    def getLevels():
        return  {
            'Under 5 years'    : [0],
            '5 to 9 years'     : [1],
            '10 to 14 years'   : [2],
            '15 to 17 years'   : [3],
            '18 to 19 years'   : [4],
            '20 to 21 years'   : [5],
            '22 to 24 years'   : [6],
            '25 to 29 years'   : [7],
            '30 to 34 years'   : [8],
            '35 to 39 years'   : [9],
            '40 to 44 years'   : [10],
            '45 to 49 years'   : [11],
            '50 to 54 years'   : [12],
            '55 to 59 years'   : [13],
            '60 to 61 years'   : [14],
            '62 to 64 years'   : [15],
            '65 years'         : [16],
            '66 years'         : [17],
            '67 to 69 years'   : [18],
            '70 to 74 years'   : [19],
            '75 to 79 years'   : [20],
            '80 to 84 years'   : [21],
            '85 years and over': [22]
        }


    @staticmethod
    def recodeVotingage():
        """
        Group into "Voting age" and "Non-voting age"
        """
        name = CC.AGECAT_VOTINGAGE
        groups = {
            "Non-voting age": list(range(0,4)),
            "Voting age"    : list(range(4,23)),
        }
        return name, groups

    @staticmethod
    def recodeVoting():
        """  
        Subset for only "Voting age"
        """
        name = CC.VOTING_TOTAL
        groups = {
            "Voting age": list(range(4,23))
        }
        return name, groups

    @staticmethod
    def recodeNonvoting():
        """
        Subset for only "Non-voting age"
        """
        name = CC.NONVOTING_TOTAL
        groups = {
            "Non-voting age": list(range(0,4))
        }
        return name, groups

