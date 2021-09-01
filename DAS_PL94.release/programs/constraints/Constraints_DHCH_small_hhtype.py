import numpy as np
import programs.constraints.Constraints_DHCH as Constraints

from constants import CC


class ConstraintsCreator(Constraints.ConstraintsCreator):
    """
       This class is what is used to create constraint objects from what is listed in the config file, for household schema
       see base class for keyword arguments description
       Schema: HH_SEX, HH_AGE, HH_HISPANIC, HH_RACE, HH_ELDERLY, HH_TENSHORT, HH_HHTYPE
       Schema dims: 2, 9, 2, 7, 4, 2, 522
       """

    schemaname = CC.SCHEMA_DHCH_small_hhtype
