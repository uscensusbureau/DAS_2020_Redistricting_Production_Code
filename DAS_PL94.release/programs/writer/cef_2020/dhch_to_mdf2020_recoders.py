from pyspark.sql import Row

from programs.writer.mdf2020_recoder import MDF2020Recoder
from programs.schema.attributes.dhch.hhtype_map import mapping
# Schema / Node columns

from constants import CC

class DHCHToMDF2020HouseholdRecoder(MDF2020Recoder):

    schema_name = CC.SCHEMA_DHCH

    def recode(self, row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row_dict: dict = row
        try:
            hhsex: int = int(row[self.getMangledName(CC.ATTR_SEX)])
            hhage: int = int(row[self.getMangledName(CC.ATTR_HHAGE)])
            hhhisp: int = int(row[self.getMangledName(CC.ATTR_HHHISP)])
            hhrace: int = int(row[self.getMangledName(CC.ATTR_RACE)])
            hhtype: int = int(row[self.getMangledName(CC.ATTR_HHTYPE_DHCH)])
            hhelderly: int = int(row[self.getMangledName(CC.ATTR_HHELDERLY)])
            hhtenshort: int = int(row[self.getMangledName(CC.ATTR_HHTENSHORT)])

            geocode:str = row['geocode']

            rtype: int = 2 #RTYPE  is 2 for household universe

            # ================================
            hhtype_dict = mapping[hhtype]

            hhsize = hhtype_dict[CC.ATTR_HHSIZE.upper()]
            child_under_18 = hhtype_dict[CC.ATTR_CHILD_UNDER_18.upper()]
            own_child_under_18 = hhtype_dict[CC.ATTR_OWN_CHILD_UNDER_18.upper()]
            own_child_under_6_only = hhtype_dict[CC.ATTR_OWN_CHILD_UNDER_6_ONLY.upper()]
            own_child_between_6_and_17 = hhtype_dict[CC.ATTR_OWN_CHILD_BETWEEN_6_AND_17.upper()]
            own_child_in_both_ranges = hhtype_dict[CC.ATTR_OWN_CHILD_IN_BOTH_RANGES.upper()]
            child_under_6_only_excluding_householder_spouse_partner = hhtype_dict[CC.ATTR_CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER.upper()]
            child_between_6_and_17_excluding_householder_spouse_partner  = hhtype_dict[CC.ATTR_CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER.upper()]
            child_in_both_ranges_excluding_householder_spouse_partner = hhtype_dict[CC.ATTR_CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER.upper()]
            multg = hhtype_dict[CC.ATTR_MULTIG.upper()]
            married = hhtype_dict[CC.ATTR_MARRIED.upper()]
            married_same_sex = hhtype_dict[CC.ATTR_MARRIED_SAME_SEX.upper()]
            married_opposite_sex = hhtype_dict[CC.ATTR_MARRIED_OPPOSITE_SEX.upper()]
            cohabiting = hhtype_dict[CC.ATTR_COHABITING.upper()]
            cohabiting_same_sex = hhtype_dict[CC.ATTR_COHABITING_SAME_SEX.upper()]
            cohabiting_opposite_sex = hhtype_dict[CC.ATTR_COHABITING_OPPOSITE_SEX.upper()]
            with_relatives = hhtype_dict[CC.ATTR_WITH_RELATIVES.upper()]
            not_alone = hhtype_dict[CC.ATTR_NOT_ALONE.upper()]

            # These are additional variables that can be obtained from the HHTYPE variable, but are not used in recodes
            # They are not used because other variables account for the same functionality
            # They could potentially be used for additional validation/verification
            # child_under_18_excluding_householder_spouse_partner = hhtype_dict[CC.ATTR_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER.upper()]
            # no_spouse_or_partner = hhtype_dict[CC.ATTR_NO_SPOUSE_OR_PARTNER.upper()]
            # no_own_children_under_18 = hhtype_dict[CC.ATTR_NO_OWN_CHILDREN_UNDER_18.upper()]
            # no_child_under_18_excluding_householder_spouse_partner = hhtype_dict[CC.ATTR_NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER.upper()]
            # no_relatives = hhtype_dict[CC.ATTR_NO_RELATIVES.upper()]
            # ================================

            output_dict = {}
            output_dict[CC.ATTR_MDF_SCHEMA_TYPE_CODE] = schema_type_code_recoder()
            output_dict[CC.ATTR_MDF_SCHEMA_BUILD_ID] = schema_build_id_recoder()

            output_dict[CC.ATTR_MDF_TABBLKST] = self.format_geocode(geocode, 0, CC.GEOLEVEL_STATE)
            output_dict[CC.ATTR_MDF_TABBLKCOU] = self.format_geocode(geocode, CC.GEOLEVEL_STATE, CC.GEOLEVEL_COUNTY)
            output_dict[CC.ATTR_MDF_TABTRACTCE] = self.format_geocode(geocode, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_TRACT)
            output_dict[CC.ATTR_MDF_TABBLKGRPCE] = self.format_geocode(geocode, CC.GEOLEVEL_TRACT, 'Block_Group')
            output_dict[CC.ATTR_MDF_TABBLK] = self.format_geocode(geocode, 'Block_Group', CC.GEOLEVEL_BLOCK)

            output_dict[CC.ATTR_MDF_RTYPE] = "2" # 2 is for households
            output_dict[CC.ATTR_MDF_GQTYPE] = "000"
            output_dict[CC.ATTR_MDF_VACS] = "0"
            output_dict[CC.ATTR_MDF_HHSIZE] = str(hhsize)
            output_dict[CC.ATTR_MDF_HHT] = hht_recode(married, with_relatives, hhsex, not_alone)
            output_dict[CC.ATTR_MDF_HHT2] = hht2_recode(married, cohabiting, own_child_under_18, with_relatives, hhsex, not_alone)

            # Set tenure to either owned or rented (coarse).
            # Will be reset to greedy detailed tenure after recoding has completed.
            # See dhch_to_mdf2020_writer.py's greedyTenureRecode method
            output_dict[CC.ATTR_MDF_TEN] = "1" if hhtenshort == 0 else "3"

            output_dict[CC.ATTR_MDF_CPLT] = cplt_recode(married_same_sex, married_opposite_sex, cohabiting_same_sex, cohabiting_opposite_sex, rtype, hhsize)
            output_dict[CC.ATTR_MDF_UPART] = upart_recode(hhsex, cohabiting_same_sex, cohabiting_opposite_sex, rtype, hhsize)
            output_dict[CC.ATTR_MDF_MULTG] = multg_recode(multg, rtype, hhsize)
            output_dict[CC.ATTR_MDF_HHLDRAGE] = hhldrage_recode(hhage, rtype, hhsize)
            output_dict[CC.ATTR_MDF_HHSPAN] = hhspan_recode(hhhisp, rtype, hhsize)
            output_dict[CC.ATTR_MDF_HHRACE] = hhrace_recode(hhrace, rtype, hhsize)

            output_dict[CC.ATTR_MDF_P18] = p18_recode(child_under_18)
            output_dict[CC.ATTR_MDF_P60] = p60_recode(hhelderly)
            output_dict[CC.ATTR_MDF_P65] = p65_recode(hhelderly)
            output_dict[CC.ATTR_MDF_P75] = p75_recode(hhelderly)

            output_dict[CC.ATTR_MDF_PAOC] = paoc_recode(own_child_under_6_only, own_child_between_6_and_17, own_child_in_both_ranges, rtype, hhsize)
            output_dict[CC.ATTR_MDF_PAC] = pac_recode(child_under_6_only_excluding_householder_spouse_partner, child_between_6_and_17_excluding_householder_spouse_partner, child_in_both_ranges_excluding_householder_spouse_partner, rtype, hhsize)

            output_dict[CC.ATTR_MDF_HHSEX] = hhsex_recode(hhsex)

            return output_dict
        except Exception:
            raise Exception(f"Unable to recode row: {str(row_dict)}")


class DHCHToMDF2020UnitRecoder(MDF2020Recoder):

    schema_name = CC.SCHEMA_DHCH

    def recode(self, row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        print(f'recoding row: {row}')
        row_dict: dict = row
        try:
            tenvacgq: int = int(row[CC.ATTR_TENVACGQ])

            assert 4 <= tenvacgq <= 34, f'For the Unit recoder, TENVACGQ must be between 4 and 34. Found {tenvacgq} instead'

            rtype = "4"
            if tenvacgq <= 10: # Indicates this is a vacant household
                rtype = "2"

            geocode:str = row['geocode']

            output_dict = {}
            output_dict[CC.ATTR_MDF_SCHEMA_TYPE_CODE] = schema_type_code_recoder()
            output_dict[CC.ATTR_MDF_SCHEMA_BUILD_ID] = schema_build_id_recoder()

            output_dict[CC.ATTR_MDF_TABBLKST] = self.format_geocode(geocode, 0, CC.GEOLEVEL_STATE)
            output_dict[CC.ATTR_MDF_TABBLKCOU] = self.format_geocode(geocode, CC.GEOLEVEL_STATE, CC.GEOLEVEL_COUNTY)
            output_dict[CC.ATTR_MDF_TABTRACTCE] = self.format_geocode(geocode, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_TRACT)
            output_dict[CC.ATTR_MDF_TABBLKGRPCE] = self.format_geocode(geocode, CC.GEOLEVEL_TRACT, 'Block_Group')
            output_dict[CC.ATTR_MDF_TABBLK] = self.format_geocode(geocode, 'Block_Group', CC.GEOLEVEL_BLOCK)

            output_dict[CC.ATTR_MDF_GQTYPE] = gqtype_recode(tenvacgq)
            output_dict[CC.ATTR_MDF_RTYPE] = rtype
            output_dict[CC.ATTR_MDF_TEN] = "0"
            output_dict[CC.ATTR_MDF_VACS] = vacs_recode(tenvacgq)
            output_dict[CC.ATTR_MDF_HHSIZE] = "0"
            output_dict[CC.ATTR_MDF_HHT] = "0"
            output_dict[CC.ATTR_MDF_HHT2] = "00"

            output_dict[CC.ATTR_MDF_CPLT] = "0"
            output_dict[CC.ATTR_MDF_UPART] = "0"
            output_dict[CC.ATTR_MDF_MULTG] = "0"
            output_dict[CC.ATTR_MDF_HHLDRAGE] = "0"
            output_dict[CC.ATTR_MDF_HHSPAN] = "0"
            output_dict[CC.ATTR_MDF_HHRACE] = "00"
            output_dict[CC.ATTR_MDF_PAOC] = "0"

            output_dict[CC.ATTR_MDF_P18] = "0"
            output_dict[CC.ATTR_MDF_P60] = "0"
            output_dict[CC.ATTR_MDF_P65] = "0"
            output_dict[CC.ATTR_MDF_P75] = "0"
            output_dict[CC.ATTR_MDF_PAC] = "0"

            output_dict[CC.ATTR_MDF_HHSEX] = "0"

            return output_dict
        except Exception:
            raise Exception(f"Unable to recode row: {str(row_dict)}")


def zeropad(x, width):
    return '0'*(width-len(str(x)))+str(x)

def rtype_recoder(relgq):
    _HOUSING_UNIT = "3"
    return _HOUSING_UNIT

def schema_type_code_recoder():
    """
    adds the SCHEMA_TYPE_CODE column to the row
    CHAR(3)
    row: dict
    """
    return "MUD"

def schema_build_id_recoder():
    """
    adds the SCHEMA_BUILD_ID column to the row
    CHAR(5)
    row: dict
    """
    return "1.1.0"


def gqtype_recode(tenvacgq: int) -> str:
    if tenvacgq <= 10: #Household and Vacant are just 000 (NIU) for GQ
        return "000"

    gq_map: dict = {
        11: "101",
        12: "102",
        13: "103",
        14: "104",
        15: "105",
        16: "106",
        17: "201",
        18: "202",
        19: "203",
        20: "301",
        21: "401",
        22: "402",
        23: "403",
        24: "404",
        25: "405",
        26: "501",
        27: "601",
        28: "602",
        29: "701",
        30: "801",
        31: "802",
        32: "900",
        33: "901",
        34: "997"
    }

    assert tenvacgq in gq_map, f"Entered TENVACGQ value not found: {tenvacgq}"

    return gq_map[tenvacgq]


def vacs_recode(tenvacgq: int) -> str:
    if 0 <= tenvacgq <= 3: # Households are not vacant
        return "0"
    if 11 <= tenvacgq <= 34: # Group quarters are not vacant
        return "0"

    vacs_map: dict = {
        4:  "1",
        5:  "2",
        6:  "3",
        7:  "4",
        8:  "5",
        9:  "6",
        10: "7"
    }

    assert tenvacgq in vacs_map, f"Entered TENVACGQ value could not be converted to VACS: {tenvacgq}"

    return vacs_map[tenvacgq]

def hht_recode(married: int, with_relatives: int, hhsex: int, not_alone: int) -> str:
    if married == 1:
        return "1" # 1 == Married Couple household

    if with_relatives == 1: # Indicates "Other family household"
        if hhsex == 0:
            return "2"  # 2 == Other family household: Male householder
        if hhsex == 1:
            return "3" # 3 == Other family household: Female householder
        raise Exception(f'Invalid set of attributes for hht_recode: married: {married}, with_relatives: {with_relatives}, hhsex: {hhsex}, not_alone: {not_alone}')

    # Must be Nonfamiy household
    if hhsex == 0:
        if not_alone == 0:
            return "4"  # 4 == Nonfamily household: Male householder, living alone
        if not_alone == 1:
            return "5"  # 5 == Nonfamily household: Male householder, not living alone
        raise Exception(f'Invalid set of attributes for hht_recode: married: {married}, with_relatives: {with_relatives}, hhsex: {hhsex}, not_alone: {not_alone}')

    if hhsex == 1:
        if not_alone == 0:
            return "6"  # 6 == Nonfamily household: Female householder, living alone
        if not_alone == 1:
            return "7"  # 7 == Nonfamily household: Female householder, not living alone
        raise Exception(f'Invalid set of attributes for hht_recode: married: {married}, with_relatives: {with_relatives}, hhsex: {hhsex}, not_alone: {not_alone}')
    raise Exception(f'Invalid set of attributes for hht_recode: married: {married}, with_relatives: {with_relatives}, hhsex: {hhsex}, not_alone: {not_alone}')


def hht2_recode(married: int, cohabiting: int, own_child_under_18: int, with_relatives: int, hhsex: int, not_alone: int):
    if married == 1:
        if own_child_under_18 == 1:
            return "01" # 01 = Married couple household: With own children < 18
        if own_child_under_18 == 0:
            return "02" # 02 = Married couple household: No own children < 18
        raise Exception(f'Invalid set of attributes for hht_recode: married: {married}, cohabiting: {cohabiting}, own_child_under_18; {own_child_under_18}, with_relatives: {with_relatives}, hhsex: {hhsex}, not_alone: {not_alone}')

    if cohabiting == 1:
        if own_child_under_18 == 1:
            return "03" # 03 = Cohabiting couple household: With own children < 18
        if own_child_under_18 == 0:
            return "04" # 04 = Cohabiting couple household: No own children < 18
        raise Exception(f'Invalid set of attributes for hht_recode: married: {married}, cohabiting: {cohabiting}, own_child_under_18; {own_child_under_18}, with_relatives: {with_relatives}, hhsex: {hhsex}, not_alone: {not_alone}')

    # Must be no_spouse_or_partner. TODO: validate this?
    if hhsex == 1:
        if not_alone == 0:
            return "05" # 05 = Female householder, no spouse/partner present: Living alone
        if with_relatives == 1:
            if own_child_under_18 == 1:
                return "06" # 06 = Female householder, no spouse/partner present: With own children < 18
            return "07" # 07 = Female householder, no spouse/partner present: With relatives, no own children < 18
        return "08" # 08 = Female householder, no spouse/partner present: Only nonrelatives present

    if hhsex == 0:
        if not_alone == 0:
            return "09" # 09 = Male householder, no spouse/partner present: Living alone
        if with_relatives == 1:
            if own_child_under_18 == 1:
                return "10" # 10 = Male householder, no spouse/partner present: With own children < 18
            return "11" # 11 = Male householder, no spouse/partner present: With relatives, no own children < 18
        return "12" # 12 = Male householder, no spouse/partner present: Only nonrelatives present

    raise Exception(f'Invalid set of attributes for hht_recode: married: {married}, cohabiting: {cohabiting}, own_child_under_18; {own_child_under_18}, with_relatives: {with_relatives}, hhsex: {hhsex}, not_alone: {not_alone}')


def cplt_recode(married_same_sex: int, married_opposite_sex: int, cohabiting_same_sex: int,
                cohabiting_opposite_sex: int, rtype: int, hhsize: int):
    if (rtype == 2 and hhsize <= 1) or rtype == 4:
        return "0"
    if married_opposite_sex == 1:
        return "1"
    elif married_same_sex == 1:
        return "2"
    elif cohabiting_opposite_sex == 1:
        return "3"
    elif cohabiting_same_sex == 1:
        return "4"
    return "5"


def upart_recode(hhsex: int, cohabiting_same_sex: int,
                 cohabiting_opposite_sex: int, rtype: int, hhsize: int):
    # All households of size < 2 have a UPART of 0=NIU
    if (rtype == 2 and hhsize <= 1) or rtype == 4:
        return "0"
    # All unmarried (cohabiting) households with size >= 2 have a detailed UPART
    if cohabiting_same_sex == 1:
        if hhsex == 0: # Indicates Male householder
            return "1" # 1 = Male householder and male partner
        return "3" # 3 = Female householder and female partner
    if cohabiting_opposite_sex == 1:
        if hhsex == 0: # Indicates Male householder
            return "2"  # 2 = Male householder and female partner
        return "4"  # 4 = Female householder and male partner
    # All married households with size >= 2 have a UPART of 5 = All other households
    return "5"


def multg_recode(multig: int, rtype: int, hhsize: int):
    if (rtype == 2 and hhsize <= 2) or rtype == 4:
        return "0"
    if multig == 0:
        return "1"
    if multig == 1:
        return "2"
    return "0"


def hhldrage_recode(hhage: int, rtype: int, hhsize: int):
    if (rtype == 2 and hhsize == 0) or rtype == 4 or hhage < 0 or hhage > 8:
        return "0"
    return str(hhage + 1)


def hhspan_recode(hhhisp: int, rtype: int, hhsize: int):
    if (rtype == 2 and hhsize == 0) or rtype == 4:
        return "0"
    return str(hhhisp + 1)


def hhrace_recode(hhrace: int, rtype: int, hhsize: int):
    if (rtype == 2 and hhsize == 0) or rtype == 4:
        return "00"
    return "0" + str(hhrace + 1)


def paoc_recode(own_child_under_6_only: int, own_child_between_6_and_17: int, own_child_in_both_ranges: int, rtype: int, hhsize: int):
    if (rtype == 2 and hhsize == 0) or rtype == 4:
        return "0"
    if own_child_under_6_only == 1:
        return "1"
    if own_child_between_6_and_17 == 1:
        return "2"
    if own_child_in_both_ranges == 1:
        return "3"
    return "4"  # no own children


def p18_recode(child_under_18: int):
    if child_under_18 is 1:
        return "1"
    return "0"


def verify_hhelderly(hhelderly: int):
    assert 0 <= hhelderly <= 3, f'HHELDERLY must be between 0 and 3, inclusive. Found {hhelderly} instead.'



def p60_recode(hhelderly: int):
    verify_hhelderly(hhelderly)
    if hhelderly >= 1:
        return "1"
    return "0"


def p65_recode(hhelderly: int):
    verify_hhelderly(hhelderly)
    if hhelderly >= 2:
        return "1"
    return "0"


def p75_recode(hhelderly: int):
    verify_hhelderly(hhelderly)
    if hhelderly >= 3:
        return "1"
    return "0"


def pac_recode(cxhhsup_child_under_6_only: int, cxhhsup_child_between_6_and_17: int, cxhhsup_child_in_both_ranges: int, rtype: int, hhsize: int):
    if (rtype == 2 and hhsize == 0) or rtype == 4:
        return "0"
    if cxhhsup_child_under_6_only == 1:
        return "1"
    if cxhhsup_child_between_6_and_17 == 1:
        return "2"
    if cxhhsup_child_in_both_ranges == 1:
        return "3"
    return "4"  # no children


def hhsex_recode(hhsex: int):
    """
    This is recoding the household histogram variable sex, which can only be 0 or 1, to the MDF HHSEX variable.
    The MDF specifies HHSEX variable as:
    0 = NIU
    1 = Male householder
    2 = Female householder
    """
    assert 0 <= hhsex <= 1, f'SEX must be between 0 and 1, inclusive. Found {hhsex} instead.'
    if hhsex == 0:
        return "1"
    return "2"
