from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class VotingAgeAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_VOTING_AGE

    @staticmethod
    def getLevels():
        return {
                '17 and under': [0],
                '18 and over' : [1]
            }

    @staticmethod
    def recodeVoting():
        """
        Subset of the votingage variable to include only the "18 and over" category
        """
        name = CC.VOTING_TOTAL
        groups = {
                "18 and over": [1]
        }
        return name, groups

    @staticmethod
    def recodeNonvoting():
        """
        Subset of the votingage variable to include only the "17 and under" category
        """
        name = CC.NONVOTING_TOTAL
        groups = {
            "17 and under": [0]
        }
        return name, groups
