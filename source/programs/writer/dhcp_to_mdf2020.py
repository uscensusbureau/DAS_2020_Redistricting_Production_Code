# import constants as con
from programs.writer.mdf2020_recoder import MDF2020Recoder

# Schema / Node columns

from constants import CC

RELGQ = CC.ATTR_RELGQ
SEX = CC.ATTR_SEX
AGE = CC.ATTR_AGE
HISP = CC.ATTR_HISP
CENRACE = CC.ATTR_CENRACE


class DHCPToMDF2020Recoder(MDF2020Recoder):

    schema_name = CC.SCHEMA_DHCP

    def recode(self, row, nullfill=False):
        """
        mapper for Row objects in spark dataframe
        recodes variables from dhcp histogram spec
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

            self.rtype_recoder,
            self.gqtype_recoder,
            self.relship_recoder,
            self.qsex_recoder,
            self.qage_recoder,
            self.cenhisp_recoder,
            self.cenrace_recoder,
            self.live_alone_recoder
        ]

        niu_fill_recodes = [
        ]

        not_supported_fill_recodes = [
        ]

        for recode in uncond_recodes:
            row = recode(row)

        if nullfill:
            for recode in niu_fill_recodes:
                row = recode(row, nullfiller=self.niuFiller)

            for recode in not_supported_fill_recodes:
                row = recode(row, nullfiller=self.notSupportedFiller)
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
        row['SCHEMA_TYPE_CODE'] = "MPD"
        return row


    def rtype_recoder(self, row):
        """
        adds the RTYPE column to the row
        refers to the Record Type
        CHAR(1)
        row: dict
            Notes:
                - In the histogram, relgq levels [0, 17] (or, in python, relgq[0:18] since the right size is exclusive),
                  refers to the Housing Unit.

                - In the histogram, relgq levels [18, 41] (or, in python, relgq[18:42] since the right size is exclusive),
                  refers to the Group Quarters types.
        """
        relgq = int(row[self.getMangledName(RELGQ)])
        _HOUSING_UNIT = "3"
        _GROUP_QUARTERS = "5"

        # relgq levels 0 through 17
        housing_unit_levels = list(range(0,18))

        # relgq levels 18 through 41
        gq_levels = list(range(18,42))

        if relgq in housing_unit_levels:
            rtype = _HOUSING_UNIT
        elif relgq in gq_levels:
            rtype = _GROUP_QUARTERS

        else: # for any values of relgq outside of [0,41], throw an error
            raise ValueError(f"{RELGQ} = '{relgq}' is not a legal value")
        row['RTYPE'] = str(rtype)
        return row


    def gqtype_recoder(self, row):
        """
        adds the GQTYPE column to the row
        refers to the Group Quarters Type
        CHAR(3)
        row: dict
        Notes:

        """
        relgq = int(row[self.getMangledName(RELGQ)])
        # relgq levels 0 through 17
        housing_unit_levels = list(range(0,18))

        # Housing unit
        if relgq in housing_unit_levels: gqtype = "000"     # NIU (i.e. household)

        # GQ 101-106
        elif relgq in [18]: gqtype = "101"     # Federal Detention centers
        elif relgq in [19]: gqtype = "102"     # Federal prisons
        elif relgq in [20]: gqtype = "103"     # State prisons
        elif relgq in [21]: gqtype = "104"     # Local jails
        elif relgq in [22]: gqtype = "105"     # Correctional residential facilities
        elif relgq in [23]: gqtype = "106"     # Military disciplinary barracks and jails

        # GQ 201-203
        elif relgq in [24]: gqtype = "201"     # Group homes for juveniles (non-correctional)
        elif relgq in [25]: gqtype = "202"     # Residential treatment centers for juveniles (non-correctional)
        elif relgq in [26]: gqtype = "203"     # Correctional facilities intended for juveniles

        # GQ 301
        elif relgq in [27]: gqtype = "301"     # Nursing facilities

        # GQ 401-405
        elif relgq in [28]: gqtype = "401"     # Mental hospitals
        elif relgq in [29]: gqtype = "402"     # Hospitals with patients who have no usual home elsewhere
        elif relgq in [30]: gqtype = "403"     # In-patient hospice facilities
        elif relgq in [31]: gqtype = "404"     # Military treatment facilities
        elif relgq in [32]: gqtype = "405"     # Residential schools for people with disabilities

        # GQ 501
        elif relgq in [33]: gqtype = "501"     # College / University student housing

        # GQ 601-602
        elif relgq in [34]: gqtype = "601"     # Military quarters
        elif relgq in [35]: gqtype = "602"     # Military ships

        # GQ 701
        elif relgq in [36]: gqtype = "701"     # Emergency and transitional shelters for people experiencing homelessness

        # GQ 801-802
        elif relgq in [37]: gqtype = "801"     # Group homes intended for adults
        elif relgq in [38]: gqtype = "802"     # Residential treatment centers for adults

        # GQ 900, 901, and 997
        elif relgq in [39]: gqtype = "900"     # Maritime / Merchant vessels
        elif relgq in [40]: gqtype = "901"     # Workers' group living quarters and job corps centers
        elif relgq in [41]: gqtype = "997"     # Other noninstitutional facilities (GQs 702, 704, 706, 903, 904)

        else: # for any values of relgq outside of [0,41], throw an error
            raise ValueError(f"{RELGQ} = '{relgq}' is not a legal value")

        row['GQTYPE'] = str(gqtype)
        return row


    def relship_recoder(self, row):
        """
        adds the RELSHIP column to the row
        refers to the Edited Relationship
        CHAR(2)
        row: dict
        Notes:
            In the histogram, the relgq levels that correspond to the relatiionship to householder variable
            are [0,17] (or relgq[0:18] in python)

            https://www.census.gov/prod/cen2010/doc/sf1.pdf
            In the histogram, institutional gq persons are [18,32] (relgq[18:33] in python)
            and noninstitutional gq persons are [33,41] (relgq[33:42] in python)
        """
        relgq = int(row[self.getMangledName(RELGQ)])
        inst_gq_levels = list(range(18, 33))
        noninst_gq_levels = list(range(33, 42))

        # relationship to householder
        if relgq in [0,1]: relship = "20"        # Householder (living alone and not living alone)
        elif relgq in [2]: relship = "21"        # Opposite sex spouse
        elif relgq in [3]: relship = "22"        # Opposite sex unmarried partner
        elif relgq in [4]: relship = "23"        # Same sex spouse
        elif relgq in [5]: relship = "24"        # Same sex unmarried partner
        elif relgq in [6]: relship = "25"        # Biological son or daughter
        elif relgq in [7]: relship = "26"        # Adopted son or daughter
        elif relgq in [8]: relship = "27"        # Stepson or stepdaughter
        elif relgq in [9]: relship = "28"        # Brother or sister
        elif relgq in [10]: relship = "29"       # Father or mother
        elif relgq in [11]: relship = "30"       # Grandchild
        elif relgq in [12]: relship = "31"       # Parent-in-law
        elif relgq in [13]: relship = "32"       # Son-in-law or daughter-in-law
        elif relgq in [14]: relship = "33"       # Other relative
        elif relgq in [15]: relship = "34"       # Roommate or housemate
        elif relgq in [16]: relship = "35"       # Foster child
        elif relgq in [17]: relship = "36"       # Other nonrelative

        # Institutional GQs
        # 101-106
        # 201-203
        # 301
        # 401-405
        elif relgq in inst_gq_levels: relship = "37"  # Institutional GQ person

        # Noninstitutional GQs
        # 501
        # 601-602
        # 701, 702, 704, 706 (Note that 702, 704, and 706 are combined into 997)
        # 801-802
        # 900, 901, 903, 904 (Note that 903 and 904 are combined into 997)
        elif relgq in noninst_gq_levels: relship = "38"  # Non-institutional GQ person

        else: # for any values of relgq outside of [0,41], throw an error
            raise ValueError(f"{RELGQ} = '{relgq}' is not a legal value")

        row['RELSHIP'] = str(relship)
        return row


    def qsex_recoder(self, row):
        """
        adds the QSEX column to the row
        refers to the Edited Sex
        CHAR(1)
        row: dict
        """
        sex = int(row[self.getMangledName(SEX)])
        _MALE = "1"
        _FEMALE = "2"
        if   sex in [0]: qsex = _MALE
        elif sex in [1]: qsex = _FEMALE

        else: # for any values of sex outside of [0,1], throw an error
            raise ValueError(f"{SEX} = '{sex}' is not a legal value")

        row['QSEX'] = str(qsex)
        return row


    def qage_recoder(self, row):
        """
        adds the QAGE column to the row
        refers to the Edited Age
        INT(3)
        row: dict
        """
        age = int(row[self.getMangledName(AGE)])
        row['QAGE'] = int(age)
        return row


    def cenhisp_recoder(self, row):
        """
        adds the CENHISP column to the row
        refers to the Hispanic Origin
        CHAR(1)
        row: dict
        """
        hispanic = int(row[self.getMangledName(HISP)])
        _NOT_HISPANIC = "1"
        _HISPANIC = "2"
        if   hispanic in [0]: cenhisp = _NOT_HISPANIC
        elif hispanic in [1]: cenhisp = _HISPANIC

        else: # for any values of hisp outside of [0,1], throw an error
            raise ValueError(f"{HISP} = '{hispanic}' is not a legal value")

        row['CENHISP'] = str(cenhisp)
        return row


    def cenrace_recoder(self, row):
        """
        adds the CENRACE column to the row
        refers to the CensusRace
        CHAR(2)
        row: dict
        Notes:
            Offset from the histogram encoding. (e.g. "01" = White alone = cenrace[0])
        """
        cenrace = int(row[self.getMangledName(CENRACE)])
        # add offset
        cenrace = cenrace + 1
        # make 2-pos with a leading zero
        cenrace = f"{cenrace:02d}"
        row['CENRACE'] = cenrace
        return row


    def live_alone_recoder(self, row):
        """
        adds the LIVE_ALONE column to the row
        refers to the Person Living Alone
        CHAR(1)
        row: dict
        """
        relgq = int(row[self.getMangledName(RELGQ)])
        relgq_nonGQ_nonHH_levels = list(range(2, 18))
        gq_levels = list(range(18, 42))
        if   relgq in [0]: live_alone = "1"   # Person is living alone
        elif relgq in [1]: live_alone = "2"   # Person is not living alone

        # What do we do with people in levels [2-17] (i.e. relationship to householder)?
        # I assume we will code them as not living alone since they are not the householder,
        # but have a relationship to the householder, indicating there is the householder and
        # this person, at a minimum.
        elif relgq in relgq_nonGQ_nonHH_levels: live_alone = "2"    # Person is not living alone

        # for GQs [18,41] (relgq[18:42] in python), people are considered "NIU" for the LIVE_ALONE variable
        elif relgq in gq_levels: live_alone = "0"    # NIU

        else: # for any values of relgq outside of [0,41], throw an error
            raise ValueError(f"{RELGQ} = '{relgq}' is not a legal value")

        row['LIVE_ALONE'] = str(live_alone)
        return row


class AnyToMDFPersons2020Recoder(DHCPToMDF2020Recoder):

    def recode(self, row, nullfill=False):
        """
        mapper for Row objects in spark dataframe
        recodes variables from dhcp histogram spec
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
            self.live_alone_recoder
        ]

        empty_var_list = [
            "RTYPE",
            "GQTYPE",
            "RELSHIP",
            "QSEX",
            "QAGE",
            "CENHISP",
            "CENRACE",
        ]

        for recode in uncond_recodes:
            row = recode(row)

        for var in empty_var_list:
            row[var] = "9"

        for varname in [self.geocode] + self.mangled_dimnames:
            row.pop(varname, None)

        return row


HHGQ = CC.ATTR_HHGQ
VOTING_AGE = CC.ATTR_VOTING_AGE


class PL94ToMDFPersons2020Recoder(DHCPToMDF2020Recoder):

    schema_name = CC.SCHEMA_PL94_2020

    def recode(self, row, nullfill=False):
        """
        mapper for Row objects in spark dataframe
        recodes variables from dhcp histogram spec
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
            self.tabblk_recoder,
            self.cenrace_recoder,
            self.cenhisp_recoder,
            self.rtype_recoder,
            self.gqtype_recoder,
            self.votingage_recoder,
        ]

        empty_var_list = [
            "RELSHIP",
            "QSEX",
            "QAGE",
            "LIVE_ALONE",
        ]

        for recode in uncond_recodes:
            row = recode(row)

        for var in empty_var_list:
            row[var] = "9"

        for varname in [self.geocode] + self.mangled_dimnames:
            row.pop(varname, None)

        return row

    def rtype_recoder(self, row):
        """
        adds the RTYPE column to the row
        refers to the Record Type
        CHAR(1)
        row: dict
            Notes: In the histogram, hhgq[0] refers to housing unit; hhgq[1:8] refers to group quarter types
        """
        hhgq = int(row[self.getMangledName(HHGQ)])
        _HOUSING_UNIT = "3"
        _GROUP_QUARTERS = "5"
        if hhgq in [0]:
            rtype = _HOUSING_UNIT
        else:
            rtype = _GROUP_QUARTERS
        row['RTYPE'] = str(rtype)
        return row

    def gqtype_recoder(self, row):
        """
        adds the GQTYPE column to the row
        refers to the Group Quarters Type
        CHAR(3)
        row: dict
        Notes:
            Since the HHGQ variable only has major group quarters information,
            the recodes group detailed categories into major GQ bands.
        """
        hhgq = int(row[self.getMangledName(HHGQ)])
        if hhgq in [0]:
            gqtype = "0"     # NIU (i.e. household)
        elif hhgq in [1]:
            gqtype = "1"     # 101 - 106
        elif hhgq in [2]:
            gqtype = "2"     # 201 - 203
        elif hhgq in [3]:
            gqtype = "3"     # 301
        elif hhgq in [4]:
            gqtype = "4"     # 401 - 405
        elif hhgq in [5]:
            gqtype = "5"     # 501 - 502
        elif hhgq in [6]:
            gqtype = "6"     # 601 - 602
        else:
            gqtype = "7"     # 701 - 706; 801 - 802; 900 - 904
        row['GQTYPE_PL'] = str(gqtype)
        return row

    def votingage_recoder(self, row):
        """
        adds the VOTING_AGE column to the row
        CHAR(1)
        row: dict
        """
        va = int(row[self.getMangledName(VOTING_AGE)])
        _NVA = "1"
        _VA = "2"
        if va in [0]:
            voting_age = _NVA
        elif va in [1]:
            voting_age = _VA

        else:  # for any values of sex outside of [0,1], throw an error
            raise ValueError(f"{VOTING_AGE} = '{va}' is not a legal value")

        row['VOTING_AGE'] = str(voting_age)
        return row


class CVAP2020Recoder(DHCPToMDF2020Recoder):

    schema_name = CC.SCHEMA_CVAP

    def recode(self, row, nullfill=False):
        """
        mapper for Row objects in spark dataframe
        recodes variables from dhcp histogram spec
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
            self.tabblk_recoder,
            self.cvaprace_recoder,
        ]

        empty_var_list = [

        ]

        for recode in uncond_recodes:
            row = recode(row)

        for var in empty_var_list:
            row[var] = "9"

        # for varname in [self.geocode] + self.mangled_dimnames:
        #     row.pop(varname, None)

        return row

    @staticmethod
    def schema_type_code_recoder(row):
        """
        adds the SCHEMA_TYPE_CODE column to the row
        CHAR(3)
        row: dict
        """
        row['SCHEMA_TYPE_CODE'] = "CVA"
        return row

    @staticmethod
    def schema_build_id_recoder(row):
        """
        adds the SCHEMA_BUILD_ID column to the row
        CHAR(5)
        row: dict
        """
        row['SCHEMA_BUILD_ID'] = "1.0.0"
        return row

    def cvaprace_recoder(self, row):
        """
        adds the CVAPRACE column to the row
        refers to the 12 level race attribute for CVAP
        CHAR(1)
        row: dict
        Notes:
            Offset from the histogram encoding. (e.g. "02" = White alone = cenrace[1])
        """
        cvaprace = int(row[self.getMangledName(CC.ATTR_CVAPRACE)])
        # add offset
        cvaprace = cvaprace + 1
        # make 2-pos with a leading zero
        row['CVAPRACE'] = cvaprace
        return row
