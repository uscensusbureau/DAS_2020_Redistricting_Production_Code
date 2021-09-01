import logging
from programs.nodes.nodes import GEOCODE
from programs.schema.schemas.schemamaker import SchemaMaker
from constants import CC


class MDF2020Recoder:

    schema_name = None

    def __init__(self, geocode_dict=CC.DEFAULT_GEOCODE_DICT):
        self.schema = SchemaMaker.fromName(self.schema_name)

        self.mangled_dimnames = list(map(lambda name: name + f"_{self.schema.name}", self.schema.dimnames))

        self.geocode = GEOCODE

        self.geocode_dict = geocode_dict

    def niuFiller(self,  length, type='str'):
        if type == 'int':
            return 0
        elif type == 'str':
            return '0' * length
        else:
            raise ValueError("Only filling int or str")

    def notSupprortedFiller(self,  length, type='str'):
        if type == 'int':
            return int(10 ** length - 1)
        elif type == 'str':
            return '9' * length
        else:
            raise ValueError("Only filling int or str")

    def getMangledName(self, name):
        return self.mangled_dimnames[self.schema.dimnames.index(name)]

    def format_geocode(self, geocode, index_1, index_2):
        try:
            if type(index_1) is str:
                index_1 = self.geocode_dict[index_1]
            if type(index_2) is str:
                index_2 = self.geocode_dict[index_2]
        except KeyError as e:
            logging.error(f'Attempted to find index_1:{index_1} or index_2:{index_2} in self.geocode_dict: {self.geocode_dict}')
            raise e

        truncated_geocode = geocode[index_1:index_2]

        width = index_2 - index_1

        return str(truncated_geocode) + ('X' * (width - len(str(truncated_geocode))))

    @staticmethod
    def schema_build_id_recoder(row):
        """
        adds the SCHEMA_BUILD_ID column to the row
        CHAR(5)
        row: dict
        """
        row['SCHEMA_BUILD_ID'] = "1.1.0"
        return row

    def tabblkst_recoder(self, row):
        """
        adds the TABBLKST column to the row
        refers to the 2020 Tabulation State (FIPS)
        CHAR(2)
        row: dict
        """
        row['TABBLKST'] = self.format_geocode(row[self.geocode], 0, CC.GEOLEVEL_STATE)
        return row

    def tabblkcou_recoder(self, row):
        """
        adds the TABBLKCOU column to the row
        refers to the 2020 Tabulation County (FIPS)
        CHAR(3)
        row: dict
        """
        row['TABBLKCOU'] = self.format_geocode(row[self.geocode], CC.GEOLEVEL_STATE, CC.GEOLEVEL_COUNTY)
        return row

    def tabtractce_recoder(self, row):
        """
        adds the TABTRACTCE column to the row
        refers to the 2020 Tabulation Census Tract
        CHAR(6)
        row: dict
        """
        row['TABTRACTCE'] = self.format_geocode(row[self.geocode], CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_TRACT)
        return row

    def tabblkgrpce_recoder(self, row):
        """
        adds the TABBLKGRPCE column to the row
        refers to the 2020 Census Block Group
        CHAR(1)
        row: dict
        """
        row['TABBLKGRPCE'] = self.format_geocode(row[self.geocode], CC.GEOLEVEL_TRACT, CC.GEOLEVEL_BLOCK_GROUP)
        return row

    def tabblk_recoder(self, row):
        """
        adds the TABBLK column to the row
        refers to the 2020 Block Number
        CHAR(4)
        row: dict
        """
        row['TABBLK'] = self.format_geocode(row[self.geocode], 'Block_Group', CC.GEOLEVEL_BLOCK)
        return row
