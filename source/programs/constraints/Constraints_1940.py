"""
Constraints for 1940 schema. Largely inherited from PL94
"""
import programs.constraints.Constraints_DHCP_HHGQ as Constraints_DHCP_HHGQ
from constants import CC


class ConstraintsCreator(Constraints_DHCP_HHGQ.ConstraintsCreator):
    """
    This class is what is used to create constraint objects from what is listed in the config file
    Inputs:
        hist_shape: the shape of the underlying histogram
        invariants: the list of invariants (already created)
        constraint_names: the names of the constraints to be created (from the list below)
    """
    attr_hhgq = CC.ATTR_HHGQ_1940
    schemaname = CC.SCHEMA_1940

    # Used only in unit tests
    implemented = (
        "total",
        "hhgq_total_lb",
        "hhgq_total_ub"
    )

