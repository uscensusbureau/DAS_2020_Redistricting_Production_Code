""" Module that makes invariants from schema names, raw data and invariant names """
from typing import Union, Iterable, Dict
import numpy as np

from programs.schema.schemas.schemamaker import SchemaMaker, _unit_schema_dict
import programs.sparse

from exceptions import DASValueError
from constants import CC

__HistData__ = Union[programs.sparse.multiSparse, np.ndarray]


class InvariantsMaker:

    @staticmethod
    def make(schema: str, raw: __HistData__, raw_housing: __HistData__, invariant_names: Iterable[str]) -> Dict[str, np.ndarray]:
        """
        Makes invariants dict, corresponding to person/household and unit schemas with raw data, containing invariants listed
        :param person_schemaname: Person or household schema, for which to make invariants
        :param unit_schemaname: Unit schema for which to make invariants
        :param raw: Raw person or household data
        :param raw_housing: Raw unit data
        :param invariant_names: which invariants to make
        :return: dict of invariants (presented as numpy arrays)
        """
        raw = InvariantsMaker.checkHistType(raw, "Person/Household")
        raw_housing = InvariantsMaker.checkHistType(raw_housing, "Unit")

        person_schema = SchemaMaker.fromName(schema)
        unit_schema = SchemaMaker.fromName(_unit_schema_dict[schema])

        schema_data_recodename = {
            # All schemas have these
            "gqhh_vect": (unit_schema, raw_housing, CC.HHGQ_UNIT_VECTOR),
            "gqhh_tot": (unit_schema, raw_housing, "total"),
            "gq_vect": (unit_schema, raw_housing, CC.HHGQ_UNIT_TOTALGQ),

            # Person schemas have these
            "tot": (person_schema, raw, "total"),
            "va": (person_schema, raw, CC.VOTING_TOTAL),

            # Household schemas have these
            "tot_hu": (unit_schema, raw_housing, CC.HHGQ_UNIT_HOUSING),

            # CVAP schems has this:
            "pl94counts": (unit_schema, raw_housing, 'detailed')
        }

        invariants_dict = {}
        for name in invariant_names:
            assert name in schema_data_recodename, f"Provided invariant name '{name}' is not implemented."
            schema, data, query_name = schema_data_recodename[name]
            query = schema.getQuery(query_name)
            invariants_dict[name] = np.array(query.answerWithShape(data)).astype(int)
        return invariants_dict

    @staticmethod
    def checkHistType(hist, name):
        if hist is None:
            raise DASValueError(f"{name} histogram should be present to create invariants", hist)
        if isinstance(hist, programs.sparse.multiSparse):
            return hist.toDense()
        if isinstance(hist, np.ndarray):
            return hist

        raise TypeError(f"{name} histogram should be either {programs.sparse.multiSparse} or {np.ndarray}, not {type(hist)}")
