#######################################################
# Block Node Writer Notes
# Updates:
#   14 Dec 2020 - slg - updated to add info to dfxml file
#   27 Feb 2019 - bam: Updated to receive, as input, a blocknode rdd instead of a dictionary of rdds
#   16 Aug 2018 - bam
#
# How to use block_node_writer in the config file:
#
# Use in the [writer] section of the config file
# writer: programs.block_node_writer.BlockNodeWriter
#
#
# Other attributes and options:
# output_fname      : the output path where the data will be stored
#                     automatically detects if s3 path or not
#
# produce_flag      : whether or not to write the data to file
#                     Use 1 to save / 0 to not save the data
#
# minimize_nodes    : whether or not to call the node's stripForSave function to minimize the memory impact of each node
#                     Use 1 to minimize / 0 to not minimize the nodes
#                     default is 0
#
# num_parts         : indicates how to repartition the rdd for faster saving
#                     default = 100
#
#######################################################
# For quick copying:
#
# [writer]
# writer:
# output_fname:
# produce_flag:
# minimize_nodes:
# num_parts:
#
#######################################################

import logging
import xml.etree.ElementTree as ET


from programs.writer.writer import DASDecennialWriter
import das_utils                # in das_decennial directory

from constants import CC

class BlockNodeWriter(DASDecennialWriter):
    """ Writer class which saves all bottom-level (block) geonodes as a pickled RDD of GeounitNode class"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.s3cat = False
        self.write_metadata = False

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving. Remove unneeded attributes. """
        if self.getboolean(CC.MINIMIZE_NODES, default=False):
            rdd = rdd.map(lambda node: node.stripForSave()).persist()
        return rdd

    def saveRDD(self, path, rdd):
        """Saves the RDD which is already coalesced to appropriate number of parts and transformed"""

        dvs_singleton = None
        if self.setup.dvs_enabled:
            if len(path) > CC.MAX_S3_PATH_LEN:
                logging.warning("len(path)=%s  path=%s",len(path),path)
                logging.warning("Path too long for DVS annotation")
            else:
                from programs.python_dvs.dvs import DVS_Singleton
                dvs_singleton = DVS_Singleton()

        logging.debug("Saving data to directory: {}".format(path))
        ET.SubElement(self.das.dfxml_writer.doc, CC.DAS_OUTPUT).text = path
        das_utils.savePickledRDD(path, rdd, dvs_singleton=dvs_singleton)
