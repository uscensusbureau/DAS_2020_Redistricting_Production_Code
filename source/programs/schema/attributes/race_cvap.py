from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class CVAPRace(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_CVAPRACE

    @staticmethod
    def getLevels():
        return {
             "Hispanic or Latino": [0],
             "White alone": [1],
             "Black or African American alone": [2],
             "American Indian and Alaska Native alone": [3],
             "Asian alone": [4],
             "Native Hawaiian and Other Pacific Islander alone": [5],
             "Some Other Race alone": [6],
             "Black or African American and White": [7],
             "American Indian and Alaska Native and White": [8],
             "Asian and White": [9],
             "American Indian and Alaska Native and Black or African American": [10],
             "Remainder of Two or More Race Responses": [11],
        }

    # In reader, both CVAP file and PL94 files are recoded to match these levels

    # @staticmethod
    # def recodeAllLevels():
    #     name = CC.ATTR_CVAPRACE
    #     groups = CVAPRace.getLevels()
    #     return name, groups