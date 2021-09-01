"""
mdf2020writer.py:  Writes out the MDF in the 2020 format.
"""
# Pavel Zhuravlev
# 6/4/19

import logging
from typing import Union, Callable, List
import xml.etree.ElementTree as ET

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

from programs.writer.writer import DASDecennialWriter
from programs.writer.rowtools import makeHistRowsFromMultiSparse
from programs.writer.hh2010_to_mdfunit2020 import Household2010ToMDFUnit2020Recoder, H12020MDFHousehold2020Recoder # Household 2010 Recoder (Demonstration Products)
from programs.writer.dhcp_hhgq_to_mdfpersons2020 import DHCPHHGQToMDFPersons2020Recoder # DHCP HHGQ Recoder (Demonstration Products)
from programs.writer.dhcp_to_mdf2020 import DHCPToMDF2020Recoder, PL94ToMDFPersons2020Recoder, CVAP2020Recoder # DHCP Recoder
from programs.nodes.nodes import GeounitNode, SYN, INVAR, GEOCODE, UNIT_SYN, getNodeAttr
from programs.schema import schema as sk
from programs.schema.attributes.hhgq_unit_demoproduct import HHGQUnitDemoProductAttr

from das_framework.ctools.s3 import s3open

from constants import CC


class MDF2020Writer(DASDecennialWriter):

    var_list: List[str]   # List of MDF columns to write

    def saveHeader(self, *, path):
        """Saves header to the requested S3 location. This header will then be combined with the contents by the s3cat command"""
        self.annotate(f"writing header to {path}")
        with s3open(path, "w", fsync=True) as f:
            f.write("|".join(self.var_list))
            f.write("\n")

    def saveRDD(self, path, df: DataFrame):
        """
        Saves the DataFrame.
        Coalesces it to a 50 partitions (with shuffle, so .repartition()) if self.s3cat is on.
        50 is based on a ballpark estimation for "optimal" partition size. Too many can lead to problems with bash command calling s3cat,
        too few may lead to slow performance (but even 1 was ok, empirically)
        Called from writer.py:saveRunData(), which reports that the file is being written, writes the file,
        then optionally writes the metadata and runs s3cat.
        s3cat concatenates metadata and header to the single partition CSV.
        """
        if self.s3cat:
            df = df.repartition(CC.MDF_PARTITIONS_FOR_S3CAT)
        df = self.sort(df)
        ET.SubElement(self.das.dfxml_writer.doc, CC.DAS_OUTPUT).text = path
        df.write.csv(path, sep="|")

        if self.setup.dvs_enabled:
            self.annotate(f"{__file__}: Computing DVS annotations")
            from  programs.python_dvs.dvs import DVS_Singleton
            ds = DVS_Singleton()
            try:
                ds.add_s3_paths_or_prefixes(ds.COMMIT_AFTER, [path + "/"], threads=CC.DVS_THREADS)
            except OSError as e:
                logging.warning("DVS OSError %s", str(e))
                logging.warning("Cannot add_s3_paths_or_prefixes for %s", path)

        # to_geocode_pair = lambda node: ((node.geocode,), node.syn)
        #
        # by_state_dict = defaultdict()
        # if self.getboolean(SPLIT_BY_STATE, default=False):
        #     for state in self.gettuple(STATE_CODES, sep=" ", default=tuple(map(lambda x: f"{x:02d}", range(50)))):
        #         by_state_dict[state] = df.filter(lambda node: node.geocode.startswith(state)).map(to_geocode_pair)
        # else:
        #     by_state_dict["ALL"] = df.map(to_geocode_pair)
        #
        # for key in by_state_dict:
        #     by_state_dict[key] = by_state_dict[key].flatMapValues(self.to_list_from_sparse).flatMap(self.expand).map(self.to_line_func)
        #     data_path = f"{path}/{key}"
        #     logging.debug(f"Saving data to directory: {data_path}")
        #     by_state_dict[key].saveAsTextFile(data_path)

    def sort(self, df):
        return df

class MDF2020PersonWriter(MDF2020Writer):
    """
    Applies recodes and saves file for the DHCP_HHGQ Demonstration product
    """
    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "TABBLKST",
        "TABBLKCOU",
        "TABTRACTCE",
        "TABBLKGRPCE",
        "TABBLK",
        "EPNUM",
        "RTYPE",
        "GQTYPE",
        "RELSHIP",
        "QSEX",
        "QAGE",
        "CENHISP",
        "CENRACE",
        "CITIZEN",
        "LIVE_ALONE"
    ]

    row_recoder = DHCPHHGQToMDFPersons2020Recoder

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((SYN, INVAR, GEOCODE))
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict)
            return persons

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.var_list)

        return df


class DHCP_MDF2020_Writer(MDF2020Writer):
    """
    Applies recodes and saves file for the DHC-P product in the 2020 format as requested
    by the MDF Specification for the Person Table.

    Includes the EPNUM variable, which is added via zipWithIndex and a mapper rather than
    through a recode function.
    """
    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "TABBLKST",
        "TABBLKCOU",
        "TABTRACTCE",
        "TABBLKGRPCE",
        "TABBLK",
        "EPNUM",
        "RTYPE",
        "GQTYPE",
        "RELSHIP",
        "QSEX",
        "QAGE",
        "CENHISP",
        "CENRACE",
        "LIVE_ALONE"
    ]

    row_recoder = DHCPToMDF2020Recoder

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        #TODO: Put this in a common location
        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((SYN, INVAR, GEOCODE))
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict, microdata_field=None)
            return persons

        def addIndexAsEPNUM(row_index):
            row, index = row_index
            rowdict = row.asDict()
            rowdict['EPNUM'] = index
            # 'priv' means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
            ordered_cols = self.var_list + ['priv']
            return Row(*ordered_cols)(*[rowdict[col] for col in ordered_cols])

        rdd = rdd.repartition(min(rdd.count(), 20000))
        rdd = rdd.flatMap(node2SparkRows)
        rdd = rdd.flatMap(lambda r: [r] * r['priv'])
        rdd = rdd.repartition(20000)

        # df = rdd.zipWithIndex().map(addIndexAsEPNUM).toDF()
        schema = StructType([StructField(col, (StringType() if col != 'EPNUM' else LongType())) for col in self.var_list + ['priv']])
        rdd = rdd.zipWithIndex().map(addIndexAsEPNUM)
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rdd, schema=schema)
        df = df.select(self.var_list)

        return df

    def sort(self, df):
        df = df.orderBy(df.TABBLKST.asc(), df.TABBLKCOU.asc(), df.TABTRACTCE.asc(), df.TABBLK.asc(), df.EPNUM.asc())
        return df


class MDF2020HouseholdWriter(MDF2020Writer):
    """
    Write out the household universe MDF. Applies recodes and adds rows for vacant housing units and groupquarters
    """

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "TABBLKST",
        "TABBLKCOU",
        "TABTRACTCE",
        "TABBLKGRPCE",
        "TABBLK",
        "RTYPE",
        "GQTYPE",
        "TEN",
        "VACS",
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
        "P18",
        "P60",
        "P65",
        "P75",
        "PAC",
        "HHSEX",
    ]

    row_recoder = Household2010ToMDFUnit2020Recoder

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        #TODO: Put this in a common location
        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((SYN, INVAR, GEOCODE))
            households = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            units = addEmptyAndGQ(nodedict, schema, households, row_recoder=self.row_recoder, gqtype_recoder=HHGQUnitDemoProductAttr.das2mdf, geocode_dict=inverted_geodict)
            return units

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.var_list)
        return df


def addEmptyAndGQ(node: Union[GeounitNode, dict],
                  schema: sk,
                  rows,
                  row_recoder: Callable,
                  gqtype_recoder: Callable = None,
                  geocode_dict = None):

    # Invariant that keeps number of total housing units (index 0) and numbers of each GQ type in the other cells
    hu_gqtype = getNodeAttr(node, INVAR)['gqhh_vect']

    # Number of vacant housing units is the total minus number of households (which are occupied units)
    vacant = hu_gqtype[0] - len(rows)

    # Template dict for empty housing unit or GQ entry
    unit_rowdict = {k: '0' for k in map(lambda s: f"{s}_{schema.name}", schema.dimnames)}
    unit_rowdict[GEOCODE] = getNodeAttr(node, GEOCODE)

    # Generate a row for with everything filled with NIU or NOT_PRODUCED
    # Generate a row for with everything filled with NIU or NOT_PRODUCED
    if geocode_dict is not None:
        recoder = row_recoder(geocode_dict=geocode_dict)
    else:
        # To ensure backwards compatibility with old writers
        recoder = row_recoder()

    unit_row = recoder.recode(unit_rowdict, nullfill=True)

    # Add empty housing units
    # Fill values appropriate to vacant units
    unit_row['VACS'] = '9'
    unit_row['GQTYPE'] = '000'
    unit_row['RTYPE'] = '2'
    # Add number of rows equal to number of vacant housing units
    rows.extend([Row(**unit_rowdict)] * vacant)

    # Add GQ
    # Fill values appropriate to GQ
    unit_row['VACS'] = '0'
    unit_row['RTYPE'] = '4'
    # Add GQ for each GQTYPE
    for gqtype in range(1, len(hu_gqtype)):
        unit_row['GQTYPE'] = gqtype_recoder(gqtype) if gqtype_recoder is not None else lambda gq_type: f"{gq_type}00"
        # Add number of rows equal to number of GQ of that type
        rows.extend([Row(**unit_rowdict)] * hu_gqtype[gqtype])

    return rows



class MDF2020PersonPL94HistogramWriter(DHCP_MDF2020_Writer):

    row_recoder = PL94ToMDFPersons2020Recoder

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "TABBLKST",
        "TABBLKCOU",
        "TABTRACTCE",
        "TABBLKGRPCE",
        "TABBLK",
        "EPNUM",
        "RTYPE",
        "GQTYPE_PL",
        "VOTING_AGE",
        "CENHISP",
        "CENRACE"
    ]


class MDF2020H1HistogramWriter(MDF2020PersonWriter):
    # Using inheritance from person writer rather than household, because the main histogram contains all units
    # If GQTYPE and RTYPE are desired to be in the output, it may be more convenient to switch to inheritance from MDF2020HouseholdWriter

    row_recoder = H12020MDFHousehold2020Recoder

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "TABBLKST",
        "TABBLKCOU",
        "TABTRACTCE",
        "TABBLKGRPCE",
        "TABBLK",
        "RTYPE",
        "HH_STATUS",
    ]

    #var_list = ["geocode", "vacant_count", "occupied_count"]

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((SYN, INVAR, GEOCODE))
            households = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict, microdata_field=None)
            units = addGroupQuarters(nodedict, schema, households, row_recoder=self.row_recoder, geocode_dict=inverted_geodict, to_microdata=False)
            # 'priv' means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
            ordered_cols = self.var_list + ['priv']
            return [Row(*ordered_cols)(*[unit[col] for col in ordered_cols]) for unit in units]

        rdd = rdd.repartition(min(rdd.count(), 20000))
        rdd = rdd.flatMap(node2SparkRows)
        rdd = rdd.flatMap(lambda r: [r] * r['priv'])
        rdd = rdd.repartition(20000)

        # df = rdd.toDF()
        schema = StructType([StructField(col, StringType()) for col in self.var_list + ['priv']])
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rdd, schema=schema)
        df = df.select(self.var_list)

        return df


def addGroupQuarters(node: Union[GeounitNode, dict],
                      schema: sk,
                      rows,
                      row_recoder: Callable,
                      gqtype_recoder: Callable = None,
                      geocode_dict = None,
                      to_microdata = True,
                     ):
    # Invariant that keeps number of total units (households, vacant, and GQ)
    unit_total = getNodeAttr(node, INVAR)['gqhh_tot']

    # Number of group quarters is the total minus the number of households
    group_quarters = unit_total - sum([row['priv'] for row in rows])

    # Template dict for empty housing unit or GQ entry
    unit_rowdict = {k: '0' for k in map(lambda s: f"{s}_{schema.name}", schema.dimnames)}
    unit_rowdict[GEOCODE] = getNodeAttr(node, GEOCODE)

    # Generate a row for with everything filled with NIU or NOT_PRODUCED
    if geocode_dict is not None:
        recoder = row_recoder(geocode_dict=geocode_dict)
    else:
        # To ensure backwards compatibility with old writers
        recoder = row_recoder()

    unit_row = recoder.recode(unit_rowdict, nullfill=True)

    # Add empty housing units
    # Fill values appropriate to vacant units
    unit_row['RTYPE'] = '4'
    unit_row['HH_STATUS'] = '0'
    # Add number of rows equal to number of vacant housing units
    if to_microdata:
        rows.extend([Row(**unit_rowdict)] * group_quarters)
    else:
        # 'priv' means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
        unit_rowdict['priv'] = int(group_quarters)
        rows.extend([Row(**unit_rowdict)])

    return rows


class CVAPWriter(MDF2020Writer):

    row_recoder = CVAP2020Recoder

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "TABBLKST",
        "TABBLKCOU",
        "TABTRACTCE",
        "TABBLKGRPCE",
        "TABBLK",
        "CVAPRACE",
#        "EPNUM"
    ]

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        import numpy as np
        from programs.sparse import multiSparse

        schema = self.setup.schema_obj

        def conform2PL94(node: GeounitNode):
            DP_counts = node.getDenseSyn()
            PL94_counts = node.invar['pl94counts']
            node.syn = multiSparse(np.where(DP_counts > PL94_counts, PL94_counts, DP_counts))
            return node

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((SYN, INVAR, GEOCODE))
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            return persons

        # def addIndexAsEPNUM(row_index):
        #     row, index = row_index
        #     rowdict = row.asDict()
        #     rowdict['EPNUM'] = index
        #     return Row(**rowdict)

        df: DataFrame = rdd.map(conform2PL94).flatMap(node2SparkRows).toDF()  # zipWithIndex().map(addIndexAsEPNUM).toDF()

        df = df.select(self.var_list)

        return df

    def sort(self, df):
        df = df.orderBy(df.TABBLKST.asc(), df.TABBLKCOU.asc(), df.TABTRACTCE.asc(), df.TABBLK.asc(), df.CVAPRACE.asc())
        return df


## These classes produce MDF2020 specifications Unit and Person Files from nodes with any histogram schemas, filling with "0". For testing purposes
from programs.writer.dhcp_hhgq_to_mdfpersons2020 import AnyToMDFPersons2020Recoder
from programs.writer.hh2010_to_mdfunit2020 import AnyToMDFHousehold2020Recoder


class MDF2020PersonAnyHistogramWriter(MDF2020PersonWriter):

    row_recoder = AnyToMDFPersons2020Recoder


class MDF2020HouseholdAnyHistogramWriter(MDF2020HouseholdWriter):

    row_recoder = AnyToMDFHousehold2020Recoder
