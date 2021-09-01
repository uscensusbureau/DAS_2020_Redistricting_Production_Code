import pandas as pd
from typing import Dict, List

from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC
from programs.schema.attributes.dhch.hhtype_map import mapping

# DataFrame of HHType
_HHType_df = pd.DataFrame(data=[v.values() for v in mapping.values()],
                          index=[k for k in mapping.keys()],
                          columns=[v for v in list(mapping.values())[0]])


class HHTypeDHCHAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHTYPE_DHCH

    @staticmethod
    def get_index(in_attr: Dict) -> int:
        """
        Get the HHType index for the attributes.
        This is the inverse of the get_attributes method.

        :param in_attr: Dictionary of the attributes.
        :return: Index.
        """
        expr = ""
        first = True

        for c in _HHType_df.columns:
            if first:
                first = False
            else:
                expr = expr + ' & '
            attr = in_attr.get(c)
            if attr is None:
                raise ValueError(f"Attribute not found in query for get_index: {c}. All attributes are required! The input attribute dictionary is: {in_attr}")
            expr = expr + f'{c} == {attr}'
        indices = HHTypeDHCHAttr.get_grouping(expr=expr)
        if len(indices) != 1:
            raise ValueError(f"Attributes do not correspond to a valid index. {in_attr}")
        # Convert to int as Panda's use of int64 is not usable in Spark DataFrames
        return int(indices[0])

    @staticmethod
    def get_attributes(index: int) -> Dict:
        """
        Get the attributes of the HHType for the index.
        This is the inverse of get_index method.

        :param index: Index value of the HHType.
        :return: Dictionary of the attributes.
        """
        return _HHType_df.iloc[index]

    @staticmethod
    def get_description(attrs: Dict) -> str:
        """
        Get the description from the attributes.  The description is the key for the getLevels method.

        :param attrs: Dictionary of the attributes.
        :return: Text
        """
        size = attrs.get('SIZE', 0)
        if size == 1:
            if attrs.get('CHILD_UNDER_18', 0):
                return "Size 1, not multig, no spouse or partner, living alone, c"
            else:
                return "Size 1, not multig, no spouse or partner, living alone, no c"
        size_desc = f"Size {size if size < 7 else '7+'}"
        multig_desc = f"{'multig' if attrs.get('MULTIG', 0) else 'not multig'}"
        if attrs.get('MARRIED', 0):
            married_desc = f"married {'opposite-sex' if attrs.get('MARRIED_OPPOSITE_SEX') else 'same-sex'}"
        elif attrs.get('COHABITING', 0):
            married_desc = f"cohabiting {'opposite-sex' if attrs.get('COHABITING_OPPOSITE_SEX') else 'same-sex'}"
        elif attrs.get('NO_SPOUSE_OR_PARTNER', 0):
            married_desc = f"no spouse or partner"
        else:
            married_desc = ""

        if attrs.get('NO_OWN_CHILD_UNDER_18', 0):
            if not attrs.get('MARRIED', 0):
                if attrs.get('NO_RELATIVES', 0):
                    married_desc = married_desc + f" with no relatives,"
                elif attrs.get('WITH_RELATIVES', 0):
                    married_desc = married_desc + f" with relatives,"
                child_desc = "no oc"
            else:
                child_desc = "with no oc"
            if attrs.get('NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0):
                child_desc = child_desc + ", no cxhhsup"
                if attrs.get("CHILD_UNDER_18", 0):
                    child_desc = child_desc + ", but has c"
                else:
                    child_desc = child_desc + ", no c"
            elif attrs.get('CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0):
                if not attrs.get('MARRIED', 0):
                    cxhhsup_desc = f"cxhhsup u6 only" if attrs.get('CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0) else \
                        f"cxhhsup 6to17" if attrs.get('CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER', 0) else \
                        f"cxhhsup both" if attrs.get('CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER', 0) else ""
                else:
                    cxhhsup_desc = f"cxhhsup u6 only" if attrs.get('CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0) else \
                        f"cxhhsup 6to17 only" if attrs.get('CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER', 0) else \
                        f"cxhhsup both" if attrs.get('CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER', 0) else ""
                child_desc = child_desc + ", but has " + cxhhsup_desc
        elif attrs.get('OWN_CHILD_UNDER_18', 0):
            child_desc = f"with oc u6 only" if attrs.get('OWN_CHILD_UNDER_6_ONLY', 0) else \
                f"with oc 6to17 only" if attrs.get('OWN_CHILD_BETWEEN_6_AND_17', 0) else \
                f"with oc both" if attrs.get('OWN_CHILD_IN_BOTH_RANGES', 0) else ""
            if attrs.get('NO_SPOUSE_OR_PARTNER', 0):
                married_desc = married_desc + ","
            if attrs.get('CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0):
                if attrs.get('OWN_CHILD_UNDER_6_ONLY', 0) and \
                        attrs.get('CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0):
                    child_desc = child_desc + ", no cxhhsup 6to17"
                elif attrs.get('OWN_CHILD_UNDER_6_ONLY', 0) and \
                        attrs.get('CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER', 0):
                    child_desc = child_desc + ", cxhhsup both"
                elif attrs.get('OWN_CHILD_BETWEEN_6_AND_17', 0) and \
                        attrs.get('CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER', 0):
                    child_desc = child_desc + ", no cxhhsup u6"
                elif attrs.get('OWN_CHILD_BETWEEN_6_AND_17', 0) and \
                        attrs.get('CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER', 0):
                    child_desc = child_desc + ", cxhhsup both"
        else:
            child_desc = ""
        return f"{size_desc}, {multig_desc}, {married_desc} {child_desc}"

    @staticmethod
    def getLevels():
        levels = {}
        for index, attrs in mapping.items():
            description: str = HHTypeDHCHAttr.get_description(attrs)
            levels[description] = [index]

        return levels

    @staticmethod
    def get_grouping(expr: str) -> List[int]:
        """
        Get the List of HHType indices from the query of the data using the expression.

        :param expr: Expression for the query
        :return: List of valid indices.
        """
        return _HHType_df.query(expr=expr, inplace=False).index

    @staticmethod
    def recodeSizeOnePerson():
        name = CC.HHSIZE_ONE
        groupings = {
            "1-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 1')
        }
        return name, groupings

    @staticmethod
    def recodeSizeTwoPersons():
        name = CC.HHSIZE_TWO
        groupings = {
            "2-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 2')
        }
        return name, groupings

    @staticmethod
    def recodeFamily():
        """Recode Family as married or has own child or with relatives."""
        name = CC.HHTYPE_FAMILY
        groupings = {
            "Family": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 or OWN_CHILD_UNDER_18 != 0 or WITH_RELATIVES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeNonfamily():
        """Recode non-family as not married and no own children and no relatives."""
        name = CC.HHTYPE_NONFAMILY
        groupings = {
            "Non-Family": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_18 == 0 and WITH_RELATIVES == 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyMarried():
        """Recode family married as just married."""
        name = CC.HHTYPE_FAMILY_MARRIED
        groupings = {
            "Married Family": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyMarriedWithChildrenIndicator():
        """Recode Family married with children indicator as grouping of married with own child and
        married without own child."""
        name = CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR
        groupings = {
            "Married with own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_UNDER_18 != 0'),
            "Married without own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_UNDER_18 == 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyMarriedWithChildrenLevels():
        """Recode Family married with children levels as grouping of married with only own children under 6,
        married with own children only between 6 and 17 and married with own children in both groups."""
        name = CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS
        groupings = {
            "Married with children under 6 only": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_UNDER_6_ONLY != 0'),
            "Married with children 6 to 17 years only": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_BETWEEN_6_AND_17 != 0'),
            "Married with children under 6 years and 6 to 17 years": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_IN_BOTH_RANGES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyOther():
        """Recode family other as not married and either with cwn child or relatives."""
        name = CC.HHTYPE_FAMILY_OTHER
        groupings = {
            "Other Family": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and (OWN_CHILD_UNDER_18 != 0 or WITH_RELATIVES != 0)')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyOtherWithChildrenIndicator():
        """Recode Family other with children indicator as grouping of not married with own child and
        not married without own child."""
        name = CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR
        groupings = {
            "Other with own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_18 != 0'),
            "Other without own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_18 == 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyOtherWithChildrenLevels():
        """Recode Family other with children levels as grouping of not married with only own children under 6,
        not married with own children only between 6 and 17 and not married with own children in both groups."""
        name = CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS
        groupings = {
            "Other with children under 6 only": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_6_ONLY != 0'),
            "Other with children 6 to 17 years only": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_BETWEEN_6_AND_17 != 0',),
            "Other with children under 6 years and 6 to 17 years": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_IN_BOTH_RANGES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeAlone():
        """Recode Along as not not alone."""
        name = CC.HHTYPE_ALONE
        groupings = {
            "Alone": HHTypeDHCHAttr.get_grouping(expr=f'NOT_ALONE == 0')
        }
        return name, groupings

    @staticmethod
    def recodeNotAlone():
        """Recode Not Alone as not along."""
        name = CC.HHTYPE_NOT_ALONE
        groupings = {
            "Not alone": HHTypeDHCHAttr.get_grouping(expr=f'NOT_ALONE != 0')
        }
        return name, groupings

    @staticmethod
    def recodeNonfamilyNotAlone():
        """Recode non-family not alone as not not alone and not married and no own children and no relatives."""
        name = CC.HHTYPE_NONFAMILY_NOT_ALONE
        groupings = {
            "Non-Family not alone": HHTypeDHCHAttr.get_grouping(expr=f'NOT_ALONE != 0 and MARRIED == 0 and OWN_CHILD_UNDER_18 == 0 and WITH_RELATIVES == 0')
        }
        return name, groupings

    @staticmethod
    def recodeNotSizeTwo():
        """Recode not size two as size not equal two."""
        name = CC.HHTYPE_NOT_SIZE_TWO
        groupings = {
            "Not size two": HHTypeDHCHAttr.get_grouping(expr=f'SIZE != 2')
        }
        return name, groupings

    @staticmethod
    def recodeChildAlone():
        name = CC.HHTYPE_CHILD_ALONE
        groupings = {
            "Child alone": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 1 and CHILD_UNDER_18 == 1')
        }
        return name, groupings

    @staticmethod
    def recodeSizeTwoWithChild():
        """Recode Size two with child as size equals two and either has own child or co-habitat has child or child. This includes a child householder and other person not a child"""
        name = CC.HHTYPE_SIZE_TWO_WITH_CHILD
        groupings = {
            "Size two with child": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 2 and (OWN_CHILD_UNDER_18 != 0 or CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER != 0 or CHILD_UNDER_18 != 0)')
        }
        return name, groupings

    @staticmethod
    def recodeSizeTwoWithChildExclHH():
        """Recode Size two with child as size equals two and has a child excluding householder"""
        name = CC.HHTYPE_SIZE_TWO_WITH_CHILD_EXCLUDING_HH
        groupings = {
            "Size two with child": HHTypeDHCHAttr.get_grouping(
                expr=f'SIZE == 2 and (OWN_CHILD_UNDER_18 != 0 or CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER != 0)')
        }
        return name, groupings

    @staticmethod
    def recodeSizeTwoCouple():
        name = CC.HHTYPE_SIZE_TWO_COUPLE
        groupings = {
            "Size 2, householder with spouse/unmarried partner": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 2 and (MARRIED != 0 or COHABITING != 0)')
        }
        return name, groupings

    @staticmethod
    def recodeOwnChildrenUnderSix():
        name = CC.HHTYPE_OWNCHILD_UNDERSIX
        groupings = {
            "Householder has own child under 6": HHTypeDHCHAttr.get_grouping(expr=f'OWN_CHILD_UNDER_6_ONLY != 0 or OWN_CHILD_IN_BOTH_RANGES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeOwnChildUnder18():
        name = CC.HHTYPE_OWNCHILD_UNDER18
        groupings = {
            "Householder has own child under 18": HHTypeDHCHAttr.get_grouping(expr=f'OWN_CHILD_UNDER_18 != 0')
        }
        return name, groupings

    @staticmethod
    def recodeSizeThreeWithTwoChildren():
        """ Two children (can be known for sure only if in both ranges) and no spouse or partner, size=3. Only one HHTYPE, 436."""
        name = CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN
        groupings = {
            "Size 3, two children": HHTypeDHCHAttr.get_grouping(expr=f'SIZE==3 and OWN_CHILD_IN_BOTH_RANGES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeSizeThreeCoupleWithOneChild():
        name = CC.HHTYPE_SIZE_THREE_COUPLE_WITH_ONE_CHILD
        groupings = {
            # "Size 3, couple with one child": [0, 17, 86, 103, 172, 189, 282, 299]  # 8 HHTYPES, like with the old HHTYPE attribute
            "Size 3, couple with one child": HHTypeDHCHAttr.get_grouping(expr=f'NO_SPOUSE_OR_PARTNER == 0 and SIZE == 3 and OWN_CHILD_UNDER_18 != 0')
        }
        return name, groupings

    @staticmethod
    def recodeSizeFourCoupleWithTwoChildren():
        name = CC.HHTYPE_SIZE_FOUR_COUPLE_WITH_TWO_CHILDREN
        groupings = {
            "Size 4, couple with two children": HHTypeDHCHAttr.get_grouping(expr=f'SIZE==4 and OWN_CHILD_IN_BOTH_RANGES != 0 and NO_SPOUSE_OR_PARTNER == 0')
        }
        # Like in the old HHTYPE only 4 types: [34, 120, 206, 316]
        return name, groupings


