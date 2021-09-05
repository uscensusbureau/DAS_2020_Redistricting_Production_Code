from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC

class HHElderlyAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHELDERLY

    @staticmethod
    def getLevels():
        return {
            'None'                                        : [0],
            'At least one 60+ year old person, no one 65+': [1],
            'At least one 65+ year old person, no one 75+': [2],
            'At least one 75+ year old person'            : [3]
        }


    @staticmethod
    def recodePresenceOver60():
        name = CC.HHELDERLY_PRESENCE_OVER60
        groupings = {
            "At least one person over 60 present" : [1, 2, 3]
        }
        return name, groupings

    @staticmethod
    def recodePresenceNot60to64():
        """
        This recode is needed for structural zero constraints.
        If we know that the elderly is the householder (e.g. size=1, or size=2 and there's a child, or size=3 and there are 2 children),
        then they have a particular ELDERLY variable.
        Here, if householder is between 60 and 64, then ELDERLY = 1, as there's no one over 64, but there's someone over 60,
        and hence ELDERLY can't be 0, 2 or 3.
        Or, "if there are elderly, someone among them is over 65"
        """
        name = CC.HHELDERLY_NO1UNDER60ORSOME1OVER64
        groupings = {
            "No one below 60 present or someone over 64 present" : [0, 2, 3]
        }
        return name, groupings

    @staticmethod
    def recodePresenceNot65to74():
        """
        This recode is needed for structural zero constraints.
        If we know that the elderly is the householder (e.g. size=1, or size=2 and there's a child, or size=3 and there are 2 children),
        then they have a particular ELDERLY variable.
        Here, if householder is between 65 and 74, then ELDERLY = 2, as there's no one over 64, but there's someone over 60,
        and hence ELDERLY can't be 0, 1 or 3.
        Or, "if there are elderly, they are either all below 65, or someone among them is over 75"
        """
        name = CC.HHELDERLY_NO1UNDER65ORSOME1OVER74
        groupings = {
            "No one below 65 present or someone over 74 present" : [0, 1, 3]
        }
        return name, groupings

    @staticmethod
    def recodePresenceNot75Plus():
        name = CC.HHELDERLY_PRESENCE_NOT_75PLUS
        groupings = {
            "No one 75+ present" : [0, 1, 2]
        }
        return name, groupings

    @staticmethod
    def recodePresenceNot65Plus():
        name = CC.HHELDERLY_PRESENCE_NOT_65PLUS
        groupings = {
            "No one 65+ present": [0, 1]
        }
        return name, groupings

    @staticmethod
    def recodePresenceNot60Plus():
        name = CC.HHELDERLY_PRESENCE_NOT_60PLUS
        groupings = {
            "No one 60 or older": [0]
        }
        return name, groupings

    @staticmethod
    def recodePresence75Plus():
        name = CC.HHELDERLY_PRESENCE_75PLUS
        groupings = {
            "Someone 75+ present" : [3]
        }
        return name, groupings


    @staticmethod
    def recodePresence60():
        name = CC.HHELDERLY_PRESENCE_60
        groupings = {
            "No one over 60": [0],
            "At least one over 60": [1,2,3]
        }
        return name, groupings

    @staticmethod
    def recodePresence65():
        name = CC.HHELDERLY_PRESENCE_65
        groupings = {
            "No one over 65 years": [0, 1],
            "At least one person over 65 years": [2, 3]
        }
        return name, groupings

    @staticmethod
    def recodePresence75():
        name = CC.HHELDERLY_PRESENCE_75
        groupings = {
            "No one over 75 years": [0, 1, 2],
            "At least one person over 75 years": [3]
        }
        return name, groupings
