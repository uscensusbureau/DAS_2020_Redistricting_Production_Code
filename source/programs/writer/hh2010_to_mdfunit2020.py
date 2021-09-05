from programs.writer.mdf2020_recoder import MDF2020Recoder

from constants import CC


# Schema / Node columns

HHSEX = CC.ATTR_HHSEX
HHAGE = CC.ATTR_HHAGE
HISP = CC.ATTR_HHHISP
RACE = CC.ATTR_HHRACE
SIZE = CC.ATTR_HHSIZE
HHTYPE = CC.ATTR_HHTYPE
ELDERLY = CC.ATTR_HHELDERLY
MULTI = CC.ATTR_HHMULTI


class Household2010ToMDFUnit2020Recoder(MDF2020Recoder):

    schema_name = CC.DAS_Household2010

    def recode(self, row, nullfill=False):
        """
        mapper for Row objects in spark dataframe
        recodes variables from hh2010 histogram spec
        to the requested mdf 2020 unit spec
        Inputs:
            row: dict - a dictionary representation of the Row object
        """
        uncond_recodes = [
            self.schema_type_code_recoder,
            self.schema_build_id_recoder,
            self.tabblkst_recoder,
            self.tabblkcou_recoder,
            self.tabtractce_recoder,
            self.tabblkgrpce_recoder,
            self.tabblk_recoder,
        ]

        niu_fill_recodes = [

            self.rtype_hu_recoder,
            self.gqtype_niu_recoder,
            self.ten_occupied_recoder,
            self.vacs_niu_recoder,
            self.hhsize_recoder,
            self.hht_recoder,
            self.hht2_recoder,
            self.cplt_recoder,
            self.upart_recoder,
            self.multg_recoder,
            self.hhldrage_recoder,
            self.hhspan_recoder,
            self.hhrace_recoder,
            self.paoc_recoder,
            self.p60_recoder,
            self.p65_recoder,
            self.p75_recoder,
            self.hhsex_recoder,
            # self.ac_recoder,
            # self.oc_recoder,

        ]

        not_supported_fill_recodes = [
            self.p18_recoder,
            self.pac_recoder,
        ]

        for recode in uncond_recodes:
            row = recode(row)

        if nullfill:
            for recode in niu_fill_recodes:
                row = recode(row, nullfiller=self.niuFiller)

            for recode in not_supported_fill_recodes:
                row = recode(row, nullfiller=self.notSupprortedFiller)
        else:
            for recode in niu_fill_recodes + not_supported_fill_recodes:
                row = recode(row, nullfiller=None)

        for varname in [self.geocode] + self.mangled_dimnames:
            row.pop(varname, None)

        return row

    @staticmethod
    def schema_type_code_recoder(row):
        """
        adds the SCHEMA_TYPE_CODE column to the row
        CHAR(3)
        row: dict
        """
        row['SCHEMA_TYPE_CODE'] = "MUD"
        return row

    def rtype_hu_recoder(self, row, nullfiller):
        """
        adds the RTYPE column to the row
        refers to the Record Type
        CHAR(1)
        row: dict
        Notes:
            Given the Household2010 Schema, all histogram counts
            refer to Housing units. As such, this function outputs
            the value "2" for all items coming from the Household2010
            histogram data.
        """
        _HOUSING_UNIT = "2"
        row['RTYPE'] = _HOUSING_UNIT if nullfiller is None else nullfiller(1, 'str')
        return row

    def gqtype_niu_recoder(self, row, nullfiller):
        """
        adds the GQTYPE column to the row
        refers to the Group Quarters Type
        CHAR(3)
        row: dict
        Notes:
            Given the Household2010 Schema, all histogram counts
            refer to the Housing units. As such, this function outputs
            the value "000" (indicating NIU (not in universe)) for all
            items coming from the Household2010 histogram data.
        """
        _NIU = "000"
        row['GQTYPE'] = _NIU if nullfiller is None else nullfiller(3, 'str')
        return row

    def ten_occupied_recoder(self, row, nullfiller):
        """
        adds the TEN column to the row
        refers to the Tenure
        CHAR(1)
        row: dict
        Notes:
            Since the Household2010 histogram consists of occupied housing units,
            the TEN variable will contain "9", indicating Occupied.
        """
        _OCCUPIED = "9"
        row['TEN'] = _OCCUPIED if nullfiller is None else nullfiller(1, 'str')
        return row

    def vacs_niu_recoder(self, row, nullfiller):
        """
        adds the VACS column to the row
        refers to the Vacancy Status
        CHAR(1)
        row: dict
        Notes:
            Since the Household2010 histogram consists of occupied housing units,
            the VACS variable will contain "0", indicating that they are NIU.
        """
        _NIU = "0"
        row['VACS'] = _NIU if nullfiller is None else nullfiller(1, 'str')
        return row

    def hhsize_recoder(self, row, nullfiller):
        """
        adds the HHSIZE column to the row
        refers to the Population Count
        INT(5)
        row: dict
        Notes:
            The same as the hhtype column, but as an integer
        """
        row['HHSIZE'] = int(row[self.getMangledName(SIZE)]) if nullfiller is None else nullfiller(5, 'int')
        return row

    def hhsex_recoder(self, row, nullfiller):
        """
        adds the HHSEX column to the row
        refers to the Householder Sex
        CHAR(1)
        row: dict
        Notes:
            This is NOT in the official MDF Spec v3.0, but appears to be either difficult (or perhaps impossible)
            to extract from combinations of other variables/columns/fields.
        """
        # add 1 to offset for Male = "1", Female = "2"
        row['HHSEX'] = str(int(row[self.getMangledName(HHSEX)]) + 1) if nullfiller is None else nullfiller(1, 'str')
        return row

    def hht_recoder(self, row, nullfiller):
        """
        adds the HHT column to the row
        refers to the Household/Family Type
        CHAR(1)
        row: dict
        """
        _MALE = 0
        # _FEMALE = 1
        sex = int(row[self.getMangledName(HHSEX)])
        hhtype = int(row[self.getMangledName(HHTYPE)])
        hhsize = int(row[self.getMangledName(SIZE)])
        living_alone = True if hhsize == 1 else False
        married = [0, 1, 2, 3, 4, 5, 6, 7]
        other_family = [8, 9, 10, 11, 13, 14, 15, 16, 19, 20, 21, 22]
        non_family = [12, 17, 18, 23]
        if hhtype in married:
            hht = "1"
        elif hhtype in other_family:
            if sex == _MALE:
                hht = "2"
            else:  # _FEMALE
                hht = "3"
        elif hhtype in non_family:
            if sex == _MALE:
                if living_alone:
                    hht = "4"
                else:  # not living alone
                    hht = "5"
            else:  # _FEMALE
                if living_alone:
                    hht = "6"
                else:  # not living alone
                    hht = "7"
        else:  # NIU - The other categories span the levels of the hhtype variable, so this should never be invoked
            hht = "0"
        row['HHT'] = str(hht) if nullfiller is None else nullfiller(1, 'str')
        return row

    def hht2_recoder(self, row, nullfiller):
        """
        adds the HHT2 column to the row
        refers to the Household/Family Type (Includes Cohabiting)
        CHAR(2)
        row: dict
        """
        # _MALE = 0
        _FEMALE = 1
        sex = int(row[self.getMangledName(HHSEX)])
        hhtype = int(row[self.getMangledName(HHTYPE)])
        married_with_own_children_under18 = [0, 1, 2, 4, 5, 6]      # 01
        married_no_own_children_under18 = [3, 7]                    # 02
        cohab_with_own_children_under18 = [8, 9, 10, 13, 14, 15]    # 03
        cohab_no_own_children_under18 = [11, 12, 16, 17]            # 04
        no_spouse_or_partner_alone = [18]                           # 05 F; 09 M
        no_spouse_or_partner_with_own_children_under18 = [19, 20, 21]  # 06 F; 10 M
        no_spouse_or_partner_relatives_no_own_children_under18 = [22]  # 07 F; 11 M
        no_spouse_or_partner_only_nonrelatives = [23]                  # 08 F; 12 M
        if hhtype in married_with_own_children_under18:
            hht2 = "01"
        elif hhtype in married_no_own_children_under18:
            hht2 = "02"
        elif hhtype in cohab_with_own_children_under18:
            hht2 = "03"
        elif hhtype in cohab_no_own_children_under18:
            hht2 = "04"
        elif hhtype in no_spouse_or_partner_alone:
            if sex == _FEMALE:
                hht2 = "05"
            else:  # _MALE
                hht2 = "09"
        elif hhtype in no_spouse_or_partner_with_own_children_under18:
            if sex == _FEMALE:
                hht2 = "06"
            else:  # _MALE
                hht2 = "10"
        elif hhtype in no_spouse_or_partner_relatives_no_own_children_under18:
            if sex == _FEMALE:
                hht2 = "07"
            else:  # _MALE
                hht2 = "11"
        elif hhtype in no_spouse_or_partner_only_nonrelatives:
            if sex == _FEMALE:
                hht2 = "08"
            else:  # _MALE
                hht2 = "12"
        else:  # NIU - The other categories span the levels of the hhtype variable, so this should never be invoked
            hht2 = "00"
        row['HHT2'] = str(hht2) if nullfiller is None else nullfiller(2, 'str')
        return row

    def cplt_recoder(self, row, nullfiller):
        """
        adds the CPLT colun to the row
        refers to the Couple Type
        CHAR(1)
        row: dict
        """
        hhtype = int(row[self.getMangledName(HHTYPE)])
        married_opposite_sex = [0, 1, 2, 3]
        married_same_sex = [4, 5, 6, 7]
        unmarried_opposite_sex = [8, 9, 10, 11, 12]
        unmarried_same_sex = [13, 14, 15, 16, 17]
        if hhtype in married_opposite_sex:
            cplt = "1"
        elif hhtype in married_same_sex:
            cplt = "2"
        elif hhtype in unmarried_opposite_sex:
            cplt = "3"
        elif hhtype in unmarried_same_sex:
            cplt = "4"
        else:  # NIU
            cplt = "0"
        row['CPLT'] = str(cplt) if nullfiller is None else nullfiller(1, 'str')
        return row

    def upart_recoder(self, row, nullfiller):
        """
        adds the UPART column to the row
        refers to the Presence and Type of Unmarried Partner Household
        CHAR(1)
        row: dict
        """
        _MALE = 0
        # _FEMALE = 1
        sex = int(row[self.getMangledName(HHSEX)])
        hhtype = int(row[self.getMangledName(HHTYPE)])
        unmarried_same_sex = [13, 14, 15, 16, 17]
        unmarried_opposite_sex = [8, 9, 10, 11, 12]
        if hhtype in unmarried_same_sex:
            if sex == _MALE:
                upart = "1"
            else:  # _FEMALE
                upart = "3"
        elif hhtype in unmarried_opposite_sex:
            if sex == _MALE:
                upart = "2"
            else:  # _FEMALE
                upart = "4"
        else:  # all other households
            upart = "5"
        row['UPART'] = str(upart) if nullfiller is None else nullfiller(1, 'str')
        return row

    def multg_recoder(self, row, nullfiller):
        """
        adds the MULTG column to the row
        refers to Multigenerational Households
        CHAR(1)
        row: dict
        Notes:
            Given the Household2010 Schema, all histogram counts
            refer to the Housing units. As such, this function will NOT
            output the value "0" (indicating NIU (not in universe)).
        """
        _NIU = "0"
        hhsize = int(row[self.getMangledName(SIZE)])
        if hhsize < 3: # TODO: CHECK IF CORRECT!
            # If the size of the household is 0, 1, or 2, then it's considered NIU
            multg = _NIU
        else:
            multg = str(int(row[self.getMangledName(MULTI)]) + 1)
        row['MULTG'] = str(multg) if nullfiller is None else nullfiller(1, 'str')
        return row

    def hhldrage_recoder(self, row, nullfiller):
        """
        adds the HHLDRAGE column to the row
        refers to the Age of Householder
        INT(3)
        row: dict
        """
        # add 1 to offset for the NIU category = 0
        row['HHLDRAGE'] = int(row[self.getMangledName(HHAGE)]) + 1 if nullfiller is None else nullfiller(3, 'int')
        return row

    def hhspan_recoder(self, row, nullfiller):
        """
        adds the HHSPAN column to the row
        refers to Hispanic Householders
        CHAR(1)
        row: dict
        """
        # add 1 to offset for the NIU category = "0"
        row['HHSPAN'] = str(int(row[self.getMangledName(HISP)]) + 1) if nullfiller is None else nullfiller(1, 'str')
        return row

    def hhrace_recoder(self, row, nullfiller):
        """
        adds the HHRACE column to the row
        refers to the Race of Householder
        CHAR(2)
        row: dict
        """
        # add 1 to offset for the NIU category = "00"
        # pad with a leading zero character
        hhrace = int(row[self.getMangledName(RACE)]) + 1
        row['HHRACE'] = f"0{hhrace}" if nullfiller is None else nullfiller(2, 'str')
        return row

    def paoc_recoder(self, row, nullfiller):
        """
        adds the PAOC column to the row
        refers to the Presence and Age of Own Children Under 18
        CHAR(1)
        row: dict
        """
        hhtype = int(row[self.getMangledName(HHTYPE)])
        own_child_under6years = [0, 4, 8, 13, 19]
        own_child_6to17years = [1, 5, 9, 14, 20]
        own_child_both_ranges = [2, 6, 10, 15, 21]
        if hhtype in own_child_under6years:
            paoc = "1"
        elif hhtype in own_child_6to17years:
            paoc = "2"
        elif hhtype in own_child_both_ranges:
            paoc = "3"
        else:  # No own children
            paoc = "4"
        row['PAOC'] = str(paoc) if nullfiller is None else nullfiller(1, 'str')
        return row

    def p18_recoder(self, row, nullfiller):
        """
        adds the P18 column to the row
        refers to the Presence of People Under 18 Years in Household
        INT(1)
        row: dict
        Notes:
            Based on the Household2010 schema, this cannot be calculated.
            9 refers to the _NOT_REPORTED value.
        """
        _NOT_REPORTED = 9
        row['P18'] = _NOT_REPORTED if nullfiller is None else nullfiller(1, 'int')
        return row

    def p60_recoder(self, row, nullfiller):
        """
        adds the P60 column to the row
        refers to the Number of People 60 Years and Over in Household
        INT(2)
        row: dict
        """
        # assuming the "None" category refers to the "NIU" level of this recoded variable
        over_60 = [1, 2, 3]
        elderly = int(row[self.getMangledName(ELDERLY)])
        if elderly in over_60:
            # With one or more people 60 years and over in household
            p60 = 1
        else:
            # NIU
            p60 = 0
        row['P60'] = p60 if nullfiller is None else nullfiller(2, 'int')
        return row

    def p65_recoder(self, row, nullfiller):
        """
        adds the P65 column to the row
        refers to the Number of People 65 Years and Over in Household
        INT(2)
        row: dict
        """
        # assuming the "None" category refers to the "NIU" level of this recoded variable
        over_65 = [2, 3]
        elderly = int(row[self.getMangledName(ELDERLY)])
        if elderly in over_65:
            # With one or more people 65 years and over in household
            p65 = 1
        else:
            # NIU
            p65 = 0
        row['P65'] = p65 if nullfiller is None else nullfiller(2, 'int')
        return row

    def p75_recoder(self, row, nullfiller):
        """
        adds the P75 column to the row
        refers to the Number of People 75 Years and Over in Household
        INT(2)
        row: dict
        """
        # assuming the "None" category refers to the "NIU" level of this recoded variable
        over_75 = [3]
        elderly = int(row[self.getMangledName(ELDERLY)])
        if elderly in over_75:
            # With one or more people 75 years and over in household
            p75 = 1
        else:
            # NIU
            p75 = 0
        row['P75'] = p75 if nullfiller is None else nullfiller(2, 'int')
        return row

    # DEPRECATED - REMOVED FROM MDF SPEC IN V3
    # def ac_recoder(self, row, nullfiller):
    #     """
    #     adds the AC column to the row
    #     refers to the Presence of Child Indicator
    #     CHAR(1)
    #     row: dict
    #     Notes:
    #         The Household2010 histogram only contains data on "own child"
    #         rather than on "child", so this recode will return "9" to
    #         indicate that this data "Does not exist".
    #     """
    #     _NOT_REPORTED = "9"
    #     row['AC'] = _NOT_REPORTED if nullfiller is None else nullfiller(1, 'str')
    #     return row

    # DEPRECATED - REMOVED FROM MDF SPEC IN V3
    # def oc_recoder(self, row, nullfiller):
    #     """
    #     #####################
    #     Double-check
    #     Do the levels of hhtype that don't state anything about own/not own children count as No or Yes for OC variable?
    #     Current assumption is: They count as "No"
    #     #####################
    #     adds the OC column the row
    #     refers to the Own Child Indicator
    #     CHAR(1)
    #     row: dict
    #     """
    #     hhtype = int(row[self.getMangledName(HHTYPE)])
    #     own_child = [
    #         0, 1, 2,
    #         4, 5, 6,
    #         8, 9, 10,
    #         13, 14, 15,
    #         19, 20, 21
    #     ]
    #     if hhtype in own_child:
    #         # Yes
    #         oc = "1"
    #     else:
    #         # No
    #         oc = "0"
    #     row['OC'] = str(oc) if nullfiller is None else nullfiller(1, 'str')
    #     return row

    def pac_recoder(self, row, nullfiller):
        """
        adds the PAC column to the row
        refers to the Presence and Age of Children Under 18
        CHAR(1)
        row: dict
        Notes:
            The Household2010 histogram only contains data on "own child"
            rather than on "child", so this recode will return "9" to
            indicate that this data "Does not exist".
        """
        _NOT_REPORTED = "9"
        row['PAC'] = _NOT_REPORTED if nullfiller is None else nullfiller(1, 'str')
        return row


class AnyToMDFHousehold2020Recoder(Household2010ToMDFUnit2020Recoder):

    def recode(self, row, nullfill=False):
        """
        mapper for Row objects in spark dataframe
        recodes variables from hh2010 histogram spec
        to the requested mdf 2020 unit spec
        Inputs:
            row: dict - a dictionary representation of the Row object
        """
        uncond_recodes = [
            self.schema_type_code_recoder,
            self.schema_build_id_recoder,
            self.tabblkst_recoder,
            self.tabblkcou_recoder,
            self.tabtractce_recoder,
            self.tabblkgrpce_recoder,
            self.tabblk_recoder,
        ]

        niu_fill_recodes = [

            self.rtype_hu_recoder,
            self.gqtype_niu_recoder,
            self.ten_occupied_recoder,
            self.vacs_niu_recoder,

        ]

        not_supported_fill_recodes = [
            self.p18_recoder,
            self.pac_recoder,
        ]

        empty_var_list =   [

            "RTYPE",
            "HHSIZE",
            "HHT",
            "HHT2",
            "CPLT",
            "UPART",
            "MULTG",
            "HHLDRAGE",
            "HHSPAN",
            "HHRACE",
            "PAOC",
            "P60",
            "P65",
            "P75",
            "HHSEX",

        ]

        for recode in uncond_recodes:
            row = recode(row)

        if nullfill:
            for recode in niu_fill_recodes:
                row = recode(row, nullfiller=self.niuFiller)

            for recode in not_supported_fill_recodes:
                row = recode(row, nullfiller=self.notSupprortedFiller)
        else:
            for recode in niu_fill_recodes + not_supported_fill_recodes:
                row = recode(row, nullfiller=None)

        for var in empty_var_list:
            row[var] = "9"

        for varname in [self.geocode] + self.mangled_dimnames:
            row.pop(varname, None)

        return row


class H12020MDFHousehold2020Recoder(Household2010ToMDFUnit2020Recoder):

    schema_name = CC.SCHEMA_H1_2020

    def recode(self, row, nullfill=False):
        """
        mapper for Row objects in spark dataframe
        recodes variables from hh2010 histogram spec
        to the requested mdf 2020 unit spec
        Inputs:
            row: dict - a dictionary representation of the Row object
        """
        uncond_recodes = [
            self.schema_type_code_recoder,
            self.schema_build_id_recoder,
            self.tabblkst_recoder,
            self.tabblkcou_recoder,
            self.tabtractce_recoder,
            self.tabblkgrpce_recoder,
            self.tabblk_recoder,
        ]

        niu_fill_recodes = [
            self.rtype_hu_recoder,
            self.hh_status_recoder,
        ]

        for recode in uncond_recodes:
            row = recode(row)

        if nullfill:
            for recode in niu_fill_recodes:
                row = recode(row, nullfiller=self.niuFiller)
        else:
            for recode in niu_fill_recodes:
                row = recode(row, nullfiller=None)

        for varname in [self.geocode] + self.mangled_dimnames:
            row.pop(varname, None)

        return row

    def hh_status_recoder(self, row, nullfiller):
        """
        adds the HH_STATUS column to the row
        refers to the Occupancy Status
        CHAR(1)
        row: dict
        """
        h1 = int(row[self.getMangledName(CC.ATTR_H1)])
        assert h1 in [0, 1]

        # h1 = 0 means vacant, HH_STATUS for vacant = 2
        # h1 = 1 means occupied, HH_STATUS for occupied = 1
        # HH_STATUS for NIU is 0 (not included in this recoder, only households will be passed in)
        hh_status = -1
        if h1 == 0: hh_status = '2'
        elif h1 == 1: hh_status = '1'

        row['HH_STATUS'] = hh_status
        return row
