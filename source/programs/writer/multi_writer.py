"""
Creates several writers and runs them to output in several formats
"""
from programs.writer.mdf2020writer import MDF2020PersonWriter, MDF2020PersonAnyHistogramWriter, MDF2020HouseholdWriter, MDF2020HouseholdAnyHistogramWriter, DHCP_MDF2020_Writer, MDF2020PersonPL94HistogramWriter, MDF2020H1HistogramWriter, CVAPWriter
from programs.writer.cef_2020.dhch_to_mdf2020_writer import DHCH_MDF2020_Writer
from programs.writer.pickled_block_data_writer import PickledBlockDataWriter
from programs.writer.ipums_1940.ipums_1940_writer import IPUMSPersonWriter
from programs.writer.h1writer import H1Writer
from programs.writer.cvap_writer import CVAP_Writer as CVAPCountsWriter
from programs.writer.block_node_writer import BlockNodeWriter
from programs.writer.writer import DASDecennialWriter
# from das_framework.driver import AbstractDASWriter
from exceptions import DASConfigError

from constants import CC

class MultiWriter(DASDecennialWriter):

    SUPPORTED_WRITERS = {
        'MDFPersonAny': MDF2020PersonAnyHistogramWriter,
        'MDFPL942020': MDF2020PersonPL94HistogramWriter,
        'DHCP_MDF': MDF2020PersonWriter,
        'DHCP2020_MDF': DHCP_MDF2020_Writer,
        'MDFHouseholdAny': MDF2020HouseholdAnyHistogramWriter,
        'MDF2020H1': MDF2020H1HistogramWriter,
        'DHCH_MDF': MDF2020HouseholdWriter,
        'DHCH2020_MDF': DHCH_MDF2020_Writer,
        'BlockNodes': BlockNodeWriter,
        'BlockNodeDicts': PickledBlockDataWriter,
        '1940Persons': IPUMSPersonWriter,
        'H1': H1Writer,
        'CVAP': CVAPWriter,
        'CVAPCounts': CVAPCountsWriter
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.writer_names = self.gettuple(CC.MULTIWRITER_WRITERS)
        self.writers = []
        for i, wname in enumerate(self.writer_names):
            try:
                writer_class = self.SUPPORTED_WRITERS[wname]
            except KeyError:
                raise DASConfigError(f"Writer {wname} is not supported in MultiWriter", CC.MULTIWRITER_WRITERS, CC.WRITER)
            postfix = self.getconfig(CC.OUTPUT_DATAFILE_NAME, default='data')
            w = writer_class(name=CC.WRITER, config=self.config, setup=self.setup, das=self.das)
            assert isinstance(w, DASDecennialWriter)

            w.setOutputFileDataName(f"{postfix}-{wname}")
            if i > 0:
                w.unsetOverwriteFlag()
            self.writers.append(w)

    def setOutputFileDataName(self, name):
        for i, w in enumerate(self.writers):
            assert isinstance(w, DASDecennialWriter)
            w.setOutputFileDataName(f"{name}-{self.writer_names[i]}")
            w.unsetOverwriteFlag()

    def unsetOverwriteFlag(self):
        for i, w in enumerate(self.writers):
            w.unsetOverwriteFlag()

    def write(self, privatized_data):
        return [writer.write(privatized_data) for writer in self.writers]

    def saveRDD(self, path, rdd):
        raise NotImplementedError("Multi-writer does not directly save RDDs (its contained writers do).")

    def transformRDDForSaving(self, rdd):
         raise NotImplementedError("Multi-writer does not directly transform RDDs (its contained writers do).")
