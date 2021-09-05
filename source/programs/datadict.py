import programs.schema.schema
from constants import CC

###############################################################################
# Table module imports and dictionary
###############################################################################
import programs.schema.table_building.building_PL94_tables as Table_PL94
import programs.schema.table_building.building_PL94_P12_tables as Table_PL94_P12
import programs.schema.table_building.building_PL94_CVAP_tables as Table_PL94_CVAP
import programs.schema.table_building.building_SF1_tables as Table_SF1
import programs.schema.table_building.building_Household2010_tables as Table_Household2010
import programs.schema.table_building.building_DHCP_HHGQ_tables as Table_DHCP_HHGQ


###############################################################################
# Schema module imports and dictionary
###############################################################################
import programs.schema.schemas.schemamaker as schemamaker


###############################################################################
# Constraint module imports and dictionary
###############################################################################
import programs.constraints.Constraints_PL94 as Constraints_PL94
import programs.constraints.Constraints_PL94_CVAP as Constraints_PL94_CVAP
import programs.constraints.Constraints_PL94_P12 as Constraints_PL94_P12
import programs.constraints.Constraints_PL94_2020 as Constraints_PL94_2020
import programs.constraints.Constraints_1940 as Constraints_1940
import programs.constraints.Constraints_SF1 as Constraints_SF1
import programs.constraints.Constraints_DHCP_HHGQ as Constraints_DHCP_HHGQ
import programs.constraints.Constraints_Household2010 as Constraints_Household2010
import programs.constraints.Constraints_Household2010_vacs as Constraints_Household2010_VACS
import programs.constraints.Constraints_Household_small as Constraints_Household_small
import programs.constraints.Constraints_TenUnit2010 as Constraints_TenUnit2010
import programs.constraints.Constraints_DHCP as Constraints_DHCP
import programs.constraints.Constraints_DHCH as Constraints_DHCH
import programs.constraints.Constraints_DHCH_small_hhtype as Constraints_DHCH_small_hhtype
import programs.constraints.Constraints_H1 as Constraints_H1
import programs.constraints.Constraints_H1_2020 as Constraints_H1_2020
import programs.constraints.Constraints_CVAP as Constraints_CVAP

###############################################################################
# Functions for Accessing Schemas, Invariants, and Constraints
###############################################################################
def getTableBuilder(schema_name: str) -> programs.schema.table_building.tablebuilder:
    """
    returns the TableBuilder object from the tablebuilder module associated with schema_name

    Inputs:
        schema_name (str): the name of the table builder schema class desired

    Outputs:
        a TableBuilder object
    """
    tablebuilder_modules = {
        CC.DAS_PL94: Table_PL94,
        CC.DAS_PL94_P12: Table_PL94_P12,
        CC.DAS_PL94_CVAP: Table_PL94_CVAP,
        CC.DAS_SF1: Table_SF1,
        CC.DAS_Household2010: Table_Household2010,
        CC.DAS_DHCP_HHGQ: Table_DHCP_HHGQ
    }
    assert schema_name in tablebuilder_modules, f"The '{schema_name}' tablebuilder module can't be found."
    tablebuilder = tablebuilder_modules[schema_name].getTableBuilder()
    return tablebuilder


# def getSchema(schema_name: str) -> programs.schema.schema:
#     """
#     returns the Schema object from the schema module associated with schema_name
#
#     Inputs:
#         schema_name (str): the name of the schema class desired
#
#     Outputs:
#         a Schema object
#     """
#     schema = schemamaker.SchemaMaker.fromName(name=schema_name)
#     return schema


def getConstraintsModule(schema_name):
    """
    returns the constraints module associated with the schema_name

    Inputs:
        schema_name (str): the name of the schema class desired

    Outputs:
        a constraints module
    """
    constraint_modules = {
        CC.DAS_PL94: Constraints_PL94,
        CC.SCHEMA_PL94_2020: Constraints_PL94_2020,
        CC.DAS_PL94_CVAP: Constraints_PL94_CVAP,
        CC.DAS_PL94_P12: Constraints_PL94_P12,
        CC.DAS_1940: Constraints_1940,
        CC.DAS_SF1: Constraints_SF1,
        CC.DAS_DHCP_HHGQ: Constraints_DHCP_HHGQ,
        CC.DAS_Household2010: Constraints_Household2010,
        CC.SCHEMA_HOUSEHOLD2010_TENVACS: Constraints_Household2010_VACS,
        CC.SCHEMA_HOUSEHOLDSMALL: Constraints_Household_small,
        CC.DAS_TenUnit2010: Constraints_TenUnit2010,
        CC.SCHEMA_REDUCED_DHCP_HHGQ: Constraints_DHCP_HHGQ,
        CC.SCHEMA_DHCP: Constraints_DHCP,
        CC.SCHEMA_DHCH: Constraints_DHCH,
        CC.SCHEMA_DHCH_small_hhtype: Constraints_DHCH_small_hhtype,
        CC.SCHEMA_H1: Constraints_H1,
        CC.SCHEMA_H1_2020: Constraints_H1_2020,
        CC.SCHEMA_CVAP: Constraints_CVAP,
    }
    assert schema_name in constraint_modules, f"The '{schema_name}' constraint module can't be found."
    return constraint_modules[schema_name]
