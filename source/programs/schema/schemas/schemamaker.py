import programs.schema.schema as sk
from constants import CC
from programs.schema.attributes.age import AgeAttr as AGE
from programs.schema.attributes.cenrace import CenraceAttr as CENRACE
from programs.schema.attributes.citizen import CitizenAttr as CITIZEN
from programs.schema.attributes.hhgq_person_simple import HHGQPersonSimpleAttr as HHGQ
from programs.schema.attributes.hisp import HispAttr as HISPANIC
from programs.schema.attributes.rel import RelAttr as REL
from programs.schema.attributes.sex import SexAttr as SEX
from programs.schema.attributes.hhage import HHAgeAttr as HH_AGE
from programs.schema.attributes.hhelderly import HHElderlyAttr as HH_ELDERLY
from programs.schema.attributes.hhhisp import HHHispAttr as HH_HISPANIC
from programs.schema.attributes.hhmulti import HHMultiAttr as HH_MULTI
from programs.schema.attributes.hhrace import HHRaceAttr as HH_RACE
from programs.schema.attributes.race_cvap import CVAPRace
from programs.schema.attributes.race import RaceAttr as RACE
from programs.schema.attributes.hhsex import HHSexAttr as HH_SEX
from programs.schema.attributes.hhsize import HHSizeAttr as HH_SIZE
from programs.schema.attributes.hhtype import HHTypeAttr as HH_TYPE
from programs.schema.attributes.tenure import TenureAttr as TENURE
from programs.schema.attributes.tenure2 import Tenure2LevelAttr as TENURE2LEV
from programs.schema.attributes.relgq import RelGQAttr as RELGQ
from programs.schema.attributes.votingage import VotingAgeAttr as VOTING_AGE
from programs.schema.attributes.hhgq1940 import HHGQ1940Attr as HHGQ_1940
from programs.schema.attributes.age1940 import Age1940Attr as AGE_1940
from programs.schema.attributes.sex1940 import Sex1940Attr as SEX_1940
from programs.schema.attributes.hisp1940 import Hispanic1940Attr as HISPANIC_1940
from programs.schema.attributes.citizen1940 import Citizen1940Attr as CITIZEN_1940
from programs.schema.attributes.race1940 import Race1940Attr as RACE_1940
from programs.schema.attributes.agecat import AgecatAttr as AGECAT
from programs.schema.attributes.hhgq_unit_demoproduct import HHGQUnitDemoProductAttr as HHGQ_UNIT
from programs.schema.attributes.hhgq_unit_simple import HHGQUnitSimpleAttr as HHGQ_UNIT_SIMPLE
from programs.schema.attributes.hhgq_unit_simple_recoded import HHGQUnitSimpleRecodedAttr as HHGQ_UNIT_SIMPLE_RECODED
from programs.schema.attributes.hhgq import HHGQAttr as HHGQ_UNIT_DHCP
from programs.schema.attributes.tenvacgq import TenureVacancyGQAttr as TENVACGQ

from programs.schema.attributes.dhch.hhtype_dhch import HHTypeDHCHAttr as HH_TYPE_DHCH
from programs.schema.attributes.hhtenshort import HHTenShortAttr as HH_TENSHORT

from programs.schema.attributes.h1 import H1Attr as H1

_schema_dict = {
    CC.SCHEMA_DHCP: [
        RELGQ, SEX, AGE, HISPANIC, CENRACE
    ],

    CC.SCHEMA_DHCH: [
        SEX, HH_AGE, HH_HISPANIC, RACE, HH_ELDERLY, HH_TENSHORT, HH_TYPE_DHCH
    ],


    CC.SCHEMA_DHCH_small_hhtype: [
        SEX, HH_AGE, HH_HISPANIC, RACE, HH_ELDERLY, HH_TENSHORT, HH_TYPE
    ],

    CC.SCHEMA_DHCH_LITE: [
        HH_SEX, HH_HISPANIC, HH_RACE, HH_SIZE, HH_ELDERLY, HH_MULTI
    ],

    CC.SCHEMA_H1: [
        H1,
    ],

    CC.SCHEMA_H1_2020: [
        H1,
    ],

    CC.SCHEMA_REDUCED_DHCP_HHGQ: [
        HHGQ, SEX, AGE, HISPANIC, CENRACE, CITIZEN
    ],

    CC.SCHEMA_SF1: [
        REL, SEX, AGE, HISPANIC, CENRACE
    ],

    CC.SCHEMA_HOUSEHOLD2010: [
        HH_SEX, HH_AGE, HH_HISPANIC, HH_RACE, HH_SIZE, HH_TYPE, HH_ELDERLY, HH_MULTI
    ],

    CC.SCHEMA_HOUSEHOLDSMALL: [
        HH_SEX, HH_TYPE
    ],

    CC.SCHEMA_HOUSEHOLD2010_TENVACS: [
        HH_SEX, HH_AGE, HH_HISPANIC, HH_RACE, HH_SIZE, HH_TYPE, HH_ELDERLY, HH_MULTI, TENURE2LEV
    ],

    CC.SCHEMA_1940: [
        HHGQ_1940, SEX_1940, AGE_1940, HISPANIC_1940, RACE_1940, CITIZEN_1940
    ],

    CC.SCHEMA_PL94: [
        HHGQ, VOTING_AGE, HISPANIC, CENRACE
    ],

    CC.SCHEMA_PL94_2020: [
        HHGQ, VOTING_AGE, HISPANIC, CENRACE
    ],

    CC.SCHEMA_PL94_CVAP: [
        HHGQ, VOTING_AGE, HISPANIC, CENRACE, CITIZEN
    ],

    CC.SCHEMA_PL94_P12: [
        HHGQ, SEX, AGECAT, HISPANIC, CENRACE
    ],

    # # SF1_JOIN doesn't work because the same dimensions/recodes have the same names
    # CC.SCHEMA_SF1_JOIN: [
    #    REL, SEX, AGE, HISPANIC, CENRACE, HH_SEX, HH_AGE, HH_HISPANIC, HH_RACE, HH_SIZE, HH_TYPE, HH_ELDERLY, HH_MULTI
    # ],

    CC.SCHEMA_TEN_UNIT_2010: [
        HH_SEX, HH_AGE, HH_HISPANIC, HH_RACE, HH_SIZE, HH_TYPE, HH_ELDERLY, HH_MULTI, TENURE
    ],

    CC.SCHEMA_UNIT_TABLE_10: [
        HHGQ_UNIT
    ],

    CC.SCHEMA_UNIT_TABLE_10_VACS: [
        TENVACGQ
    ],

    CC.SCHEMA_UNIT_TABLE_10_SIMPLE: [
        HHGQ_UNIT_SIMPLE
    ],

    CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED: [
        HHGQ_UNIT_SIMPLE_RECODED
    ],

    CC.SCHEMA_UNIT_TABLE_10_DHCP: [
        HHGQ_UNIT_DHCP
    ],

    CC.SCHEMA_CVAP: [
        CVAPRace
    ],


}

_schema_dict.update(
    {
        # These are old schema names, putting in thr dict for backward compatibility
        CC.DAS_PL94:            _schema_dict[CC.SCHEMA_PL94],
        CC.DAS_PL94_CVAP:       _schema_dict[CC.SCHEMA_PL94_CVAP],
        CC.DAS_PL94_P12:        _schema_dict[CC.SCHEMA_PL94_P12],
        CC.DAS_1940:            _schema_dict[CC.SCHEMA_1940],
        CC.DAS_SF1:             _schema_dict[CC.SCHEMA_SF1],
        CC.DAS_DHCP_HHGQ:       _schema_dict[CC.SCHEMA_REDUCED_DHCP_HHGQ],
        CC.DAS_Household2010:   _schema_dict[CC.SCHEMA_HOUSEHOLD2010],
        CC.DAS_TenUnit2010:     _schema_dict[CC.SCHEMA_TEN_UNIT_2010],
    })

_unit_schema_dict = {
    CC.SCHEMA_DHCP: CC.SCHEMA_UNIT_TABLE_10_DHCP,  # CC.SCHEMA_UNIT_TABLE_10,
    CC.SCHEMA_DHCH: CC.SCHEMA_UNIT_TABLE_10_VACS,
    CC.SCHEMA_DHCH_small_hhtype: CC.SCHEMA_UNIT_TABLE_10_VACS,
    CC.SCHEMA_DHCH_LITE: CC.SCHEMA_UNIT_TABLE_10,
    CC.SCHEMA_H1: CC.SCHEMA_UNIT_TABLE_10,
    CC.SCHEMA_H1_2020: CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED,
    CC.SCHEMA_REDUCED_DHCP_HHGQ: CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED,
    CC.SCHEMA_SF1: CC.SCHEMA_UNIT_TABLE_10,
    CC.SCHEMA_HOUSEHOLD2010: CC.SCHEMA_UNIT_TABLE_10,
    CC.SCHEMA_HOUSEHOLDSMALL: CC.SCHEMA_UNIT_TABLE_10_VACS,
    CC.SCHEMA_HOUSEHOLD2010_TENVACS: CC.SCHEMA_UNIT_TABLE_10_VACS,
    CC.SCHEMA_1940: CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED,
    CC.SCHEMA_PL94: CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED,
    CC.SCHEMA_PL94_2020: CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED,
    CC.SCHEMA_PL94_CVAP: CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED,
    CC.SCHEMA_PL94_P12: CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED,
    # SF1_JOIN doesn't work because the same dimensions/recodes have the same names
    # CC.SCHEMA_SF1_JOIN:
    CC.SCHEMA_TEN_UNIT_2010: CC.SCHEMA_UNIT_TABLE_10,
    CC.SCHEMA_CVAP: CC.SCHEMA_CVAP,
}

_unit_schema_dict.update(
    {
        # These are old schema names, putting in thr dict for backward compatibility
        CC.DAS_PL94:            _unit_schema_dict[CC.SCHEMA_PL94],
        CC.DAS_PL94_CVAP:       _unit_schema_dict[CC.SCHEMA_PL94_CVAP],
        CC.DAS_PL94_P12:        _unit_schema_dict[CC.SCHEMA_PL94_P12],
        CC.DAS_1940:            _unit_schema_dict[CC.SCHEMA_1940],
        CC.DAS_SF1:             _unit_schema_dict[CC.SCHEMA_SF1],
        CC.DAS_DHCP_HHGQ:       _unit_schema_dict[CC.SCHEMA_REDUCED_DHCP_HHGQ],
        CC.DAS_Household2010:   _unit_schema_dict[CC.SCHEMA_HOUSEHOLD2010],
        CC.DAS_TenUnit2010:     _unit_schema_dict[CC.SCHEMA_TEN_UNIT_2010],
    })


def buildSchema(name=CC.SCHEMA_REDUCED_DHCP_HHGQ, path=None):
    return SchemaMaker.fromName(name, path)


class SchemaMaker:

    @staticmethod
    def fromName(name, path=None):
        return SchemaMaker.fromAttlist(name, _schema_dict[name], path)

    @staticmethod
    def fromAttlist(name, attlist, path=None):
        # prepare schema information
        dimnames = [att.getName() for att in attlist]
        shape = tuple(att.getSize() for att in attlist)
        leveldict = {att.getName(): att.getLevels() for att in attlist}
        levels = sk.unravelLevels(leveldict)

        # construct schema object
        myschema = sk.Schema(name, dimnames, shape, recodes=None, levels=levels)

        for att in attlist:
            att_recodes = att.getRecodes()
            for myrecode in att_recodes:
                myschema.addRecode(*myrecode)

        ###############################################################################
        # Save Schema (as a JSON file)
        ###############################################################################
        if path is not None:
            myschema.saveAsJSON(path)
        return myschema
