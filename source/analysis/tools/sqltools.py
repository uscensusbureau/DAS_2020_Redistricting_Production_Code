import os
import shutil
import das_utils as du
import numpy as np
import pandas
import time
import multiprocessing as mp
import subprocess
import re
import shutil

import analysis.constants as AC
from constants import CC as con
from constants import CC

import programs.schema.schemas.schemamaker as schemamaker


def getRecodesFromSchema(schema_name):
    schema = schemamaker.SchemaMaker.fromName(schema_name)
    recode_dict = getRecodesFromList(schema.dimnames)
    return recode_dict


def getRecodesFromList(column_list):
    missing_columns = set(column_list) - set(_all_recode_dicts.keys())
    assert len(missing_columns) == 0, f"The following recodes don't exist, please add them to sqltools.py: {missing_columns}"
    return { name: sql for name, sql in _all_recode_dicts.items() if name in column_list }


def getIndices(schema_attr_levels):
    """ 
    extracts the index from each level by removing the [] from the [i]
    e.g. toy_level_dict = {
             'a': [0],
             'b': [1],
             'c': [2]
         }
         getIndices(toy_level_dict) => [0, 1, 2]
    """
    return np.array(list(schema_attr_levels.values())).flatten().tolist()


############################################################
# Recodes for Schema Attributes - (2020 Census MDF -> Attr)
############################################################

def recode_HHGQPersonSimpleAttr():
    """ 
    Recodes the RTYPE and GQTYPE variables from the 2020 Census MDF Spec
    to the attribute in: programs/schema/attributes/hhgq_person_simple.py
    """
    RTYPE = "RTYPE"
    GQTYPE = "GQTYPE"
    
    sql = ['case']
    sql += [f"when {RTYPE} = '3' and {GQTYPE} = '000' then '0'"]
    sql += [f"when {RTYPE} = '5' and {GQTYPE} = '101' then '1'"]
    sql += [f"when {RTYPE} = '5' and {GQTYPE} = '201' then '2'"]
    sql += [f"when {RTYPE} = '5' and {GQTYPE} = '301' then '3'"]
    sql += [f"when {RTYPE} = '5' and {GQTYPE} = '401' then '4'"]
    sql += [f"when {RTYPE} = '5' and {GQTYPE} = '501' then '5'"]
    sql += [f"when {RTYPE} = '5' and {GQTYPE} = '601' then '6'"]
    sql += [f"when {RTYPE} = '5' and {GQTYPE} = '701' then '7'"]
    sql += ['else -1']
    sql += ['end']
    sql = "\n".join(sql)
    
    sqldict = { schemamaker.HHGQ.getName(): sql }
    return sqldict
    

def recode_SexAttr():
    """
    Recodes the QSEX variable from the 2020 Census MDF Spec
    to the attribute in: programs/schema/attributes/sex.py
    """
    QSEX = "QSEX"
    
    sql = ['case']
    sql += [f"when {QSEX} = '1' then '0'"]
    sql += [f"when {QSEX} = '2' then '1'"]
    sql += ['else -1']
    sql += ['end']
    sql = "\n".join(sql)

    sqldict = { schemamaker.SEX.getName(): sql }
    return sqldict


def recode_AgeAttr():
    """
    Recodes the QAGE variable from the 2020 Census MDF Spec
    to the attribute in: programs/schema/attributes/age.py
    """
    QAGE = "QAGE"
    ages = getIndices(schemamaker.AGE.getLevels())
    
    sql = ['case']
    sql += [f"when {QAGE} = {the_age} then '{the_age}'" for the_age in ages]
    sql += ['else -1']
    sql += ['end']
    sql = "\n".join(sql)

    sqldict = { schemamaker.AGE.getName(): sql }
    return sqldict


def recode_HispAttr():
    """
    Recodes the CENHISP variable from the 2020 Census MDF Spec
    to the attribute in: programs/schema/attributes/hisp.py
    """
    CENHISP = "CENHISP"
    
    sql = ['case']
    sql += [f"when {CENHISP} = '1' then '0'"]
    sql += [f"when {CENHISP} = '2' then '1'"]
    sql += ['else -1']
    sql += ['end']
    sql = "\n".join(sql)

    sqldict = { schemamaker.HISPANIC.getName(): sql }
    return sqldict


def recode_CenraceAttr():
    """
    Recodes the CENRACE variable from the 2020 Census MDF Spec
    to the attribute in: programs/schema/attributes/cenrace.py
    """
    CENRACE = "CENRACE"
    
    #   mdf's code : attr's index
    # e.g. { 
    #         "01" : "0", 
    #         "02" : "1", 
    #         ... 
    #      }
    leveldict = { str(x[0]+1).zfill(2) : str(x[0]) for x in schemamaker.CENRACE.getLevels().values() }

    sql = ['case']
    sql += [f"when CENRACE = {mdf_val} then {attr_val}" for mdf_val, attr_val in leveldict.items()]
    sql += ["else -1"]
    sql += ["end"]
    sql = "\n".join(sql)
    
    sqldict = { schemamaker.CENRACE.getName(): sql }
    return sqldict


def recode_CitizenAttr():
    """
    Recodes the CITIZEN variable from the 2020 Census MDF Spec (<= v6)
    to the attribute in: programs/schema/attributes/citizen.py
    """
    CITIZEN = "CITIZEN"
    
    sql = ['case']
    sql += [f"when {CITIZEN} = '2' then '0'"]
    sql += [f"when {CITIZEN} = '1' then '1'"]
    sql += ['else -1']
    sql += ['end']
    sql = "\n".join(sql)

    sqldict = { schemamaker.CITIZEN.getName(): sql }
    return sqldict


def recode_VotingAgeAttr():
    """
    Recodes the QAGE variable from the 2020 Census MDF Spec (v7.1) to the
    attribute in: programs/schema/attributes/votingage.py
    """
    QAGE = "QAGE"
    
    # get the max age from the AgeAttr
    ages = getIndices(schemamaker.AGE.getLevels())

    sql = ['case']
    # under 18 (0-17)
    sql += [f"when {QAGE} = '{age}' then '0'" for age in ages[:18]]
    # 18+
    sql += [f"when {QAGE} = '{age}' then '1'" for age in ages[18:]]
    sql += ['else -1']
    sql += ['end']
    sql = "\n".join(sql)
    
    sqldict = { schemamaker.VOTING_AGE.getName(): sql }
    return sqldict
    


def recode_RelGQAttr():
    """
    Recodes the LIVE_ALONE, RELSHIP, RTYPE, and GQTYPE variables from the 2020 Census
    MDF Spec (v7.1) to the attribute in: programs/schema/attributes/relgq.py
    """
    LIVE_ALONE = "LIVE_ALONE"
    RELSHIP = "RELSHIP"
    RTYPE = "RTYPE"
    GQTYPE = "GQTYPE"
    
    sql = ['case']
    # live alone / not live alone
    sql += [f"when {LIVE_ALONE} = '1' and {RELSHIP} = '20' and {RTYPE} = '3' and {GQTYPE} = '000' then '0'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '20' and {RTYPE} = '3' and {GQTYPE} = '000' then '1'"]
    # relationship
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '21' and {RTYPE} = '3' and {GQTYPE} = '000' then '2'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '22' and {RTYPE} = '3' and {GQTYPE} = '000' then '3'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '23' and {RTYPE} = '3' and {GQTYPE} = '000' then '4'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '24' and {RTYPE} = '3' and {GQTYPE} = '000' then '5'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '25' and {RTYPE} = '3' and {GQTYPE} = '000' then '6'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '26' and {RTYPE} = '3' and {GQTYPE} = '000' then '7'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '27' and {RTYPE} = '3' and {GQTYPE} = '000' then '8'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '28' and {RTYPE} = '3' and {GQTYPE} = '000' then '9'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '29' and {RTYPE} = '3' and {GQTYPE} = '000' then '10'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '30' and {RTYPE} = '3' and {GQTYPE} = '000' then '11'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '31' and {RTYPE} = '3' and {GQTYPE} = '000' then '12'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '32' and {RTYPE} = '3' and {GQTYPE} = '000' then '13'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '33' and {RTYPE} = '3' and {GQTYPE} = '000' then '14'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '34' and {RTYPE} = '3' and {GQTYPE} = '000' then '15'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '35' and {RTYPE} = '3' and {GQTYPE} = '000' then '16'"]
    sql += [f"when {LIVE_ALONE} = '2' and {RELSHIP} = '36' and {RTYPE} = '3' and {GQTYPE} = '000' then '17'"]
    # gq types
    # institutional
    # 100s
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '101' then '18'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '102' then '19'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '103' then '20'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '104' then '21'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '105' then '22'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '106' then '23'"]
    # 200s
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '201' then '24'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '202' then '25'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '203' then '26'"]
    # 300s
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '301' then '27'"]
    # 400s
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '401' then '28'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '402' then '29'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '403' then '30'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '404' then '31'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '37' and {RTYPE} = '5' and {GQTYPE} = '405' then '32'"]
    # non-institutional
    # 500s
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '38' and {RTYPE} = '5' and {GQTYPE} = '501' then '33'"]
    # 600s
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '38' and {RTYPE} = '5' and {GQTYPE} = '601' then '34'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '38' and {RTYPE} = '5' and {GQTYPE} = '602' then '35'"]
    # 700s
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '38' and {RTYPE} = '5' and {GQTYPE} = '701' then '36'"]
    # 800s
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '38' and {RTYPE} = '5' and {GQTYPE} = '801' then '37'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '38' and {RTYPE} = '5' and {GQTYPE} = '802' then '38'"]
    # 900s
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '38' and {RTYPE} = '5' and {GQTYPE} = '900' then '39'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '38' and {RTYPE} = '5' and {GQTYPE} = '901' then '40'"]
    sql += [f"when {LIVE_ALONE} = '0' and {RELSHIP} = '38' and {RTYPE} = '5' and {GQTYPE} = '997' then '41'"]

    sql += ['else -1']
    sql += ['end']
    sql = "\n".join(sql)

    sqldict = { schemamaker.RELGQ.getName(): sql }
    return sqldict
    

_all_recode_dicts = du.mergedicts([
    recode_HHGQPersonSimpleAttr(),
    recode_SexAttr(),
    recode_AgeAttr(),
    recode_HispAttr(),
    recode_CenraceAttr(),
    recode_CitizenAttr(),
    recode_VotingAgeAttr(),
    recode_RelGQAttr()
])
