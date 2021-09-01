from programs.writer.mdf2020writer import MDF2020Writer
from pyspark.sql import Row
import numpy as np


class CVAP_Writer(MDF2020Writer):
    var_list = ["geocode", "race", "EST_CVAP"]

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        # ## Normally, for MDF we convert counts into individual rows, the same number as the count
        # ## and apply recodes to MDF format. Won't do it for CVAP here and just output geocode, race and EST_CVAP count

        def getRaceCounter(node):
            DP_counts = node.getDenseSyn()
            PL94_counts = node.invar['pl94counts']
            final_counts = np.where(DP_counts > PL94_counts, PL94_counts, DP_counts)
            return [Row(geocode=node.geocode, race=raceid, EST_CVAP=int(race_count)) for raceid, race_count in enumerate(final_counts) if race_count > 0]

        df = rdd.flatMap(getRaceCounter).toDF()

        df = df.select(self.var_list)

        return df