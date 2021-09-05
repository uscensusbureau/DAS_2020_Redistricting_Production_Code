# Table.py
# William Sexton
# Last Modified: 4/5/19
"""
    This is the table module for the DAS-2018 instance of the DAS-framework.
    It contains a table classes that inherits from the AbstractDASReader.
    The classes must contain a method called process.
"""

from operator import add
import numpy as np
from programs.reader.table_reader import AbstractTable

from exceptions import DASConfigError

from constants import CC


class DenseHistogramTable(AbstractTable):
    """
        This class stores the microdata and metadata for the CEF person records.

        This is an OUTDATED class and should not really be used. Use SQLSparTable (or, if you need for some reason, SparTable)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_shape = None
        tv = set(map(lambda v: v.name, self.variables))
        vv = set(map(lambda v: v.name, self.recode_variables)) if self.recode_variables else set()
        tv_and_vv = tv.union(vv)
        for varname in self.histogram_variables:
            if varname not in tv_and_vv:
                msg = f"Histogram variable {varname} for {self.name} is neither in table variables nor in recode variables, check your config file"
                raise DASConfigError(msg, f"{self.name}.{CC.HISTOGRAM}/{self.name}.{CC.GEOGRAPHY}", CC.READER)

    def printNumPartsAndSchema(self, data):
        print(f"{self.__class__.__name__} received microdata dataframe with {data.rdd.getNumPartitions()} partitions and the following schema")
        data.printSchema()

    def process(self, data):
        """
            args:
                a Spark dataframe containing CEF person (or household, or unit) records

            This function performs the following process:
                (1) Convert the data to a RDD.
                (2) row -> (geo_histogram,1)) (Map the row to a geo_histogram, a tuple of the
                    geography variables plus the histogram variables.)
                (3) Reduce by key.
                (4) (geo_histogram,cnt) -> (geo, (histogram, cnt))
                (5) groupbykey: creates (geo, list of (histogram, cnt))
                (6) (geo, list of (histogram, cnt)) -> (geo, ndarray)

            returns: an rdd of (geo,numpy ndarray) pairs.
        """

        self.printNumPartsAndSchema(data)
        return (data.rdd.map(self.create_key_value_pair)
                        .reduceByKey(add)
                        .map(lambda key_value:
                             (key_value[0][:len(self.geography_variables)],
                              (key_value[0][len(self.geography_variables):],
                               key_value[1])))
                        .groupByKey()
                        .mapValues(self.to_ndarray).repartition(self.reader.num_reader_partitions))

    def to_ndarray(self, prehist_list):
        """
            Input:
                list of (idx, cnt) pairs, where idx consists of histogram variables,
                (e.g (qrel, qage, qsex, cenrace,) for some person table
                     or (gqtype, ) for a unit table)

            This function performs the following process:
                (1) Initialize the ndarray.
                (2) Iterate through the list assigning cnt to
                    idx in ndarray except for error idx.

            Output:
                a ndarray, which is the histogram of detail counts
        """
        hist = np.zeros(self.data_shape).astype(int)
        for idx, cnt in prehist_list:
            hist[idx] = cnt
        return hist

    def create_key_value_pair(self, row):
        """
            This creates a key value pair for geography and histogram variables.
            This function needs to check the validity of the row record.

            Input:
                a row of data from the CEF

            Output:
                (geo_histogram_idx, 1), where geo_histogram is a tuple
                    of the geography variable values and
                    the histogram variable values.
        """
        blk_idx = (tuple([row[var] for var in self.geography_variables]) + tuple([int(row[var]) for var in self.histogram_variables]))

        return blk_idx, 1


class UnitFromPersonTable(DenseHistogramTable):
    """ This is an outdated class and should not really be used. Use SQLSparTable (or, if you need for some reason, SparTable) """
    COUNT = "count"  # This is the name used by Spark SQL for the field produced by count() operation

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uniqueID = self.gettuplewsec(CC.UNIQUE)
        self.data_shape = None

    def process(self, data):
        """
            Input:
                 data: a Spark dataframe (df)

            Output:
                a RDD with block by block counts of housing units and gqs by type
        """
        self.printNumPartsAndSchema(data)
        all_vars = list(self.geography_variables) + list(self.histogram_variables)
        return (data.select(*(all_vars + list(self.uniqueID)))
                    .distinct()
                    .groupBy(*all_vars)
                    .count() # Up to this point it's an SQL query, and violates partitioning, yielding 200. Repartition here, if you need # This is an outdated class and should not really be used. Use SQLSparTable (or, if you need for some reason, SparTable)
                    .rdd
                    .map(lambda row: (tuple([row[var] for var in self.geography_variables]), (tuple([row[var] for var in self.histogram_variables]), row[self.COUNT])))
                    .groupByKey()
                    .mapValues(self.to_ndarray).repartition(self.reader.num_reader_partitions))
