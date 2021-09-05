import os
from programs.reader.fwf_reader import FixedWidthFormatTable
from programs.reader.cef_2020.cef_2020_grfc import load_grfc
from typing import Tuple
from constants import CC

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    from pyspark.sql.types import StringType, IntegerType
    from pyspark.sql.functions import concat, col
except ImportError as e:
    pass


class CEF2020PersonsTable(FixedWidthFormatTable):
    def load(self, filter=None):
        self.annotate("In Persons load")

        # Store the unit table as a local variable instead of class variable to avoid an issue
        # during serialization when calling the for an RDD"s map function
        # TODO: Better factoring of table_reader"s load method to include a postprocessing step would remove this requirement
        uwc = self.unit_with_geocode
        self.unit_with_geocode = None

        persons_df = super().load()

        final_df = persons_df.join(uwc, on=["MAFID"], how="inner")

        self.annotate(f"Finished Persons load")
        return final_df

class CEF2020DHCPUnitTable(FixedWidthFormatTable):
    def load(self, filter=None):
        self.annotate("In Unit load")
        # Load the GRF-C to obtain a DataFrame with oidtb and geocode
        grfc_path = os.path.expandvars(self.getconfig("grfc_path", section=CC.READER))
        geo_data = load_grfc(grfc_path)

        # Load the Unit table
        unit_data = super().load(filter)
        # Join with geo_data on oidtb, which will add the geocode to the Unit DataFrame
        unit_with_geocode = unit_data.join(geo_data, on=["oidtb"], how="left")

        # Make the unit_with_geocode DataFrame available
        units_without_geocode = unit_with_geocode.where(col('geocode').isNull())
        assert units_without_geocode.count() == 0, f'Some units could not match with geocode! {units_without_geocode.show()}'

        self.reader.tables["Person"].unit_with_geocode = unit_with_geocode
        self.annotate("Finished Unit load")
        return unit_with_geocode



class DHCP_recoder:
    """
        This is the input recoder for the 2020 CEF of the Decennial Census.
        It creates the relgq, sex, age, and hispanic variables
    """

    def __init__(
        self,
        relgq_varname_list: Tuple[str],
        sex_varname_list: Tuple[str],
        age_varname_list: Tuple[str],
        hispanic_varname_list: Tuple[str],
        cenrace_das_varname_list: Tuple[str],
    ):
        self.relgq_varname_list: Tuple[str] = relgq_varname_list
        self.sex_varname_list: Tuple[str] = sex_varname_list
        self.age_varname_list: Tuple[str] = age_varname_list
        self.hispanic_varname_list: Tuple[str] = hispanic_varname_list
        self.cenrace_das_varname_list: Tuple[str] = cenrace_das_varname_list

    def recode(self, row: Row) -> Row:
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row_dict: dict = row.asDict()
        try:
            relship: int = int(row[self.relgq_varname_list[0]])
            qgqtyp: str = str(row[self.relgq_varname_list[1]])
            final_pop: int = int(row["final_pop"])

            qsex: str = str(row[self.sex_varname_list[0]])
            qage: int = int(row[self.age_varname_list[0]])
            cenhisp: str = str(row[self.hispanic_varname_list[0]])
            cenrace: str = str(row[self.cenrace_das_varname_list[0]])

            row_dict[CC.ATTR_RELGQ] = relgq_recode(relship, qgqtyp, final_pop)
            row_dict[CC.ATTR_SEX] = sex_recode(qsex)
            row_dict[CC.ATTR_AGE] = age_recode(qage)
            row_dict[CC.ATTR_HISP] = hispanic_recode(cenhisp)
            row_dict[CC.ATTR_CENRACE_DAS] = cenrace_recode(cenrace)

            return Row(**row_dict)
        except Exception:
            raise Exception(f"Unable to recode row: {str(row_dict)}")


def cenrace_recode(cenrace):
    return int(cenrace) - 1


def relgq_recode(relship: int, qgqtyp: str, final_pop: int) -> int:
    """
    This function returns the recoded value for relgq based on the values of relship, gqgtyp, and final_pop.

    :param relship: The value of the relship variable
    :param qgqtyp: The value of the qgqtyp variable
    :param final_pop: The value of the final_pop variable

    :return: An int representing the recoded value of relgq based on the relship, qgqtyp, and final_pop
    """
    assert (
        relship >= 20 and relship <= 38
    ), f"Only values between 20 and 38 are valid for relship, received: {relship}"

    qgqtyp_mapping = {
        "101": 18,
        "102": 19,
        "103": 20,
        "104": 21,
        "105": 22,
        "106": 23,
        "201": 24,
        "202": 25,
        "203": 26,
        "301": 27,
        "401": 28,
        "402": 29,
        "403": 30,
        "404": 31,
        "405": 32,
        "501": 33,
        "601": 34,
        "602": 35,
        "701": 36,
        "801": 37,
        "802": 38,
        "900": 39,
        "901": 40,
        # Other noninstitutional. Does 997 exist?
        "702": 41,
        "704": 41,
        "706": 41,
        "903": 41,
        "904": 41,
    }

    assert (
        qgqtyp == "   " or qgqtyp in qgqtyp_mapping.keys()
    ), f"Invalid CEF QGQTYPE value found: {qgqtyp}"

    relgq = -1

    relship_mapping = {
        21: 2,
        22: 3,
        23: 4,
        24: 5,
        25: 6,
        26: 7,
        27: 8,
        28: 9,
        29: 10,
        30: 11,
        31: 12,
        32: 13,
        33: 14,
        34: 15,
        35: 16,
        36: 17,
    }

    """
    0: row.RELSHIP == 20 && (JoinWithUnit.FINAL_POP == 1)
    1: row.RELSHIP == 20 && (JoinWithUnit.FINAL_POP > 1)
    """
    if relship == 20:
        assert (
            final_pop >= 0
        ), f"relship is 20 (householder), final_pop variable should exist and be greater than or equal to 1, but is {final_pop}"
        if final_pop == 1:
            relgq = 0
        elif final_pop > 1:
            relgq = 1
    elif relship in relship_mapping:
        relgq = relship_mapping[relship]
    elif qgqtyp in qgqtyp_mapping:
        relgq = qgqtyp_mapping[qgqtyp]

    assert (
        relgq > -1
    ), f"Relgq could not be recoded, for relship: {relship}, qgqtyp: {qgqtyp}, final_pop: {final_pop}"

    return relgq


def sex_recode(qsex: str):
    assert qsex in ["1", "2"], f"CEF QSEX variable must be either 1 or 2, found {qsex}"
    return int(qsex) - 1


def age_recode(qage: int):
    assert (
        0 <= qage <= 115
    ), f"CEF QAGE variable must be between 0 and 115 inclusive, found {qage}"
    return qage


def votingage_recode(qage: int):
    assert (
        0 <= qage <= 115
    ), f"CEF QAGE variable must be between 0 and 115 inclusive, found {qage}"
    return int(qage >= 18)

def hhgq_recode(qgqtyp: str):
    qgqtyp_mapping = {
        "   ": 0,
        "101": 1,
        "102": 1,
        "103": 1,
        "104": 1,
        "105": 1,
        "106": 1,
        "201": 2,
        "202": 2,
        "203": 2,
        "301": 3,
        "401": 4,
        "402": 4,
        "403": 4,
        "404": 4,
        "405": 4,
        "501": 5,
        "601": 6,
        "602": 6,
        "701": 7,
        "801": 7,
        "802": 7,
        "900": 7,
        "901": 7,
        # Other noninstitutional. Does 997 exist?
        "702": 7,
        "704": 7,
        "706": 7,
        "903": 7,
        "904": 7,
    }

    assert (
            qgqtyp == "   " or qgqtyp in qgqtyp_mapping.keys()
    ), f"Invalid CEF QGQTYPE value found: {qgqtyp}"

    hhgq = qgqtyp_mapping[qgqtyp]

    hhgq1dig = min(int(qgqtyp) // 100, 7) if qgqtyp != "   " else 0
    assert hhgq == hhgq1dig, f"Explicit mapping didn't correspond to taking 1st digit: {hhgq} != {hhgq1dig}"
    return hhgq


def hispanic_recode(cenhisp: str):
    """
    Calculate the valid hispanic variable value from a row and the variable names for cenhisp

    :param row: A Row object containing the cenhisp variable
    :param hispanic_varname_list:
    :return:
    """
    assert cenhisp in [
        "1",
        "2",
    ], f"CEF CENHISP variable must be either 1 or 2, found {cenhisp}"
    return int(cenhisp) - 1


class PL94_2020_recoder:
    """
    This recoder takes the same data as intended for DHCP 2020, but produces only variables needed for PL94 histogram
    """
    def __init__(
        self,
        hhgq_varname_list: Tuple[str],
        votingage_varname_list: Tuple[str],
        hispanic_varname_list: Tuple[str],
        cenrace_das_varname_list: Tuple[str],
    ):
        self.hhgq_varname_list: Tuple[str] = hhgq_varname_list
        self.votingage_varname_list: Tuple[str] = votingage_varname_list
        self.hispanic_varname_list: Tuple[str] = hispanic_varname_list
        self.cenrace_das_varname_list: Tuple[str] = cenrace_das_varname_list

    def recode(self, row: Row) -> Row:
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row_dict: dict = row.asDict()
        try:

            qgqtyp: str = str(row[self.hhgq_varname_list[0]])
            qage: int = int(row[self.votingage_varname_list[0]])
            cenhisp: str = str(row[self.hispanic_varname_list[0]])
            cenrace: str = str(row[self.cenrace_das_varname_list[0]])

            row_dict[CC.ATTR_HHGQ] = hhgq_recode(qgqtyp)
            row_dict[CC.ATTR_VOTING_AGE] = votingage_recode(qage)
            row_dict[CC.ATTR_HISP] = hispanic_recode(cenhisp)
            row_dict[CC.ATTR_CENRACE_DAS] = cenrace_recode(cenrace)

            return Row(**row_dict)
        except Exception:
            raise Exception(f"Unable to recode row: {str(row_dict)}")


class PL94_2020_Unit_recoder:
    """
    This recoder takes the same data as intended for DHCP 2020, but produces only variables needed for unit histogram
    to go with PL94 Persons histogram. That is, a 1D histogram with 8-level HHGQ attribute
    """
    def __init__(self, qgqtyp_varname_list):
        self.qgqtyp_list = qgqtyp_varname_list

    def recode(self, row):
        """
            Input:
                row: original dataframe Row
            Output:
                dataframe Row with recode variables added
        """
        row_dict: dict = row.asDict()
        try:
            qgqtyp: str = str(row[self.qgqtyp_list[0]])
            row_dict[CC.ATTR_HHGQ_UNIT_SIMPLE_RECODED] = hhgq_recode(qgqtyp)

            return Row(**row_dict)

        except Exception:
            raise Exception(f"Unable to recode row: {str(row)}")


# class DHCP_Unit_recoder:
#     """
#     #TODO: Incomplete. Minimal/stub implementation to allow Persons to complete
#     This is the recoder for table1 of the Decennial Census.
#     It creates the hhgq_unit_dhcp, hhsex, hhage, hhhisp, tenure, and hhtype variables.
#     """
#     def __init__(self, qgqtyp_varname_list):
#         self.qgqtyp_list = qgqtyp_varname_list
#
#     def recode(self, row):
#         """
#             Input:
#                 row: original dataframe Row
#             Output:
#                 dataframe Row with recode variables added
#         """
#         try:
#             # relship: int = int(row[self.qgqtyp_list[0]])
#             # gqtyp: int = int(row[self.qgqtyp_list[1]])
#             #
#             # row = hhgq_unit_dhcp_recode(row, self.qgqtyp_list)
#
#             row = Row(**row.asDict())
#
#             return row
#         except Exception:
#             raise Exception(f"Unable to recode row: {str(row)}")
#
#
# def hhgq_unit_dhcp_recode(row, var_list):
#
#     hhgq_unit_dhcp = 0
#
#
#
#
#
#     return Row(**row.asDict(), hhgq_unit_dhcp=hhgq_unit_dhcp)

