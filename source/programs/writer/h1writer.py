"""
h1writer.py:  Writes out simple dataframe of geocode | vacant_count | occupied count
for H1 only run.

"""

from programs.writer.mdf2020writer import MDF2020Writer
from pyspark.sql import Row

__version__="0.2.0"


class H1Writer(MDF2020Writer):
    """Write out H1 only file."""

    var_list = ["geocode", "vacant_count", "occupied_count"]

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        # ## Normally, for MDF we convert counts into individual rows, the same number as the count
        # ## and apply recodes to MDF format. Won't do it for H1 here and just output geocode, vacant_count and occupied count

        # schema = self.setup.schema_obj

        # def node2SparkRows(node: GeounitNode):
        #     nodedict = node.toDict((SYN, INVAR, GEOCODE))
        #     persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
        #     return persons

        df = rdd.map(lambda node:
                     Row(
                         geocode=node.geocode,
                         vacant_count=int(node.syn.toDense()[0]),
                         occupied_count=int(node.syn.toDense()[1])
                     )).toDF()

        df = df.select(self.var_list)

        return df
