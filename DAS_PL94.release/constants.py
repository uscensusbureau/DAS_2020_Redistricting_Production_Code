"""
Module for program-wide constants.

Usage in the calling program:

    from constants import CC

Then to reference CONSTANT, just use:

    CC.CONSTANT

Attempting to assign to C.CONSTANT will raise an error.

 """

__version__ = "0.1.0"
import re


# newstyle:

class _DAS_CONSTANTS:
    def __setattr__(self, name, value):
        raise TypeError(f"Attempt to set read/only constant {name}")

    # pylint: disable=bad-whitespace

    ################
    # primary configuration file
    ################
    # ctool.env.JSONConfigReader() will read the version in git, but if it can't be found
    # it will automatically read this one.
    DAS_CONFIG_JSON='/mnt/gits/das-vm-config/das_config.json'


    ################
    # Metadata in DFXML
    ################
    DAS_DFXML="das_dfxml"
    DAS_DFXML_VERSION="1.0"
    DAS_CONFIG='das_config'
    DAS_INPUT='das_input'
    DAS_OUTPUT='das_output'
    DAS_S3CAT='das_s3cat'


    ###########################################
    # Budget
    ###########################################
    BUDGET_DEC_DIGIT_TOLERANCE = 7
    BUDGET = "budget"
    EPSILON_BUDGET_TOTAL = "epsilon_budget_total"
    GLOBAL_SCALE = "global_scale"
    APPROX_DP_DELTA = "approx_dp_delta"
    GEOLEVEL_BUDGET_PROP = "geolevel_budget_prop"
    ZERO_ERROR_GEOLEVELS = "zero_error_geolevels"
    ONLY_DYADIC_RATIONALS = "only_dyadic_rationals"
    PRINT_PER_ATTR_EPSILONS = "print_per_attr_epsilons"
    STRATEGY = "strategy"

    ## DP mechanism noise primitives
    PRIMITIVE_FRACTION_DENOM_LIMIT = 1048576
    PRIVACY_FRAMEWORK = "privacy_framework"
    ZCDP = "zcdp"
    PURE_DP = "pure_dp"
    DP_MECHANISM = "dp_mechanism"
    GEOMETRIC_MECHANISM = "geometric_mechanism"
    LAPLACE_MECHANISM = "laplace_mechanism"
    GAUSSIAN_MECHANISM = "gaussian_mechanism"
    DISCRETE_GAUSSIAN_MECHANISM = "discrete_gaussian_mechanism"
    ROUNDED_CONTINUOUS_GAUSSIAN_MECHANISM = "rounded_continuous_gaussian_mechanism"
    FLOAT_DISCRETE_GAUSSIAN_MECHANISM = "float_discrete_gaussian_mechanism" # TODO: remove this? Uses copious float-ops; but, its speed is useful for large-scale experiments


    ###########################################
    # Environment Variables
    ###########################################
    DAS_ENVIRONMENT   = 'DAS_ENVIRONMENT'
    ITE               = 'ITE'
    DAS_S3ROOT_ENV    = 'DAS_S3ROOT'
    DAS_S3MGMT_ENV    = 'DAS_S3MGMT'
    DAS_S3MGMT_ACL_ENV = 'DAS_S3MGMT_ACL'
    DAS_LIGHTS_OUT    = 'DAS_LIGHTS_OUT' # set TRUE if we are running in lights out mode
    CLUSTERID_ENV     = 'CLUSTERID'
    CLUSTERID_UNKNOWN = 'j-UNKNOWN'
    PYTHON_VERSION    = 'PYTHON_VERSION'
    MASTER_IP         = 'MASTER_IP'
    INSTANCEID        = 'INSTANCEID'

    #########################################
    # Constraints
    #########################################
    CONSTRAINTS = "constraints"
    FAILSAFE_SCHEMA = "failsafe_schema"

    ###########################################
    # String, regex, IO
    ###########################################
    S3_PREFIX = "s3://"
    HDFS_PREFIX = "hdfs://"

    ################################################################
    ### das2020_driver
    ################################################################
    UPLOAD_LOGFILE = 'upload_logfile'

    # regex for removing whitespace characters and splitting by commas
    # use with the re module
    # re.split(DELIM, string_to_split)
    # "   apple  , hat, fork,spoon,    pineapple" => ['apple', 'hat', 'fork', 'spoon', 'pineapple']
    REGEX_CONFIG_DELIM = r"^\s+|\s*,\s*|\s+$"

    ###########################################
    # Analysis Constants
    ###########################################
    RUN_PREFIX = "run_"
    RUN_INFEAS = "feas_dict.json"
    RUN_DATA = "data/"

    ###########################################
    # Engine constants
    ###########################################
    NUM_ENGINE_PARTITIONS = "numEnginePartitions"
    QUERIESPROP = "queriesprop"
    UNITQUERIESPROP = "unitqueriesprop"
    DPQUERIES = "DPqueries"
    UNITDPQUERIES = "UnitDPqueries"
    ROUNDERQUERIES = "RounderQueries"
    POOL_MEASUREMENTS = "pool_measurements"

    L2_QUERY_ORDERING = "L2QO"
    L2_CONSTRAIN_TO_QUERY_ORDERING = "L2CQO"
    ROUNDER_QUERY_ORDERING = "RQO"

    CHECK_BUDGET = "check_budget"

    RETURN_ALL_LEVELS = "return_all_levels"

    ENGINE = "engine"
    DELETERAW = "delete_raw"
    MINIMALSCHEMA = "minimalSchema"

    BACKUP = "backup"
    ROUND_BACKUP = "round_backup"
    L2_FEAS_FAIL = "L2 backup/failsafe failed!"
    L1_FEAS_FAIL = "L1 rounder (or L1 backup/failsafe) failed!"
    PARENT_CONSTRAINTS = "cons_parent"
    PARENT_CONSTRAINTS_ROUND = "cons_parent_round"

    WORKLOAD = "workload"

    SAVENOISY = "save_noisy"
    RELOADNOISY = "reload_noisy"
    SAVED_NOISY_APP_ID = "saved_noisy_app_id"
    SAVEOPTIMIZED = "save_optimized"
    OPTIMIZATION_START_FROM_LEVEL = "optimization_start_from_level"
    NOISY_MEASUREMENTS_POSTFIX = "noisy_measurements_postfix"
    APPID_NO_SPARK = "APPID_No_Spark"

    ################################################################
    # logging
    ################################################################
    import das_framework.driver
    LOGGING_SECTION = das_framework.driver.LOGGING_SECTION
    LOGFILENAME_OPTION = das_framework.driver.LOGFILENAME_OPTION
    LOGLEVEL_OPTION = das_framework.driver.LOGLEVEL_OPTION
    LOGFOLDER_OPTION = das_framework.driver.LOGFOLDER_OPTION


    ###########################################
    #  Gurobi
    ###########################################

    GUROBI = 'gurobi'
    GUROBI_PATH = 'gurobi_path'
    GUROBI_SECTION = "gurobi"
    GRB_LICENSE_FILE = 'GRB_LICENSE_FILE'
    GUROBI_LIC = "gurobi_lic"
    GUROBI_LIC_CREATE = "gurobi_lic_create"
    GUROBI_LIC_CREATE_FNAME = '/tmp/gurobi.lic'
    GUROBI_LIC_FAIL_RATE = 'gurobi_lic_fail_rate'
    GUROBI_LIC_FAIL_DEFAULT = 0.0
    TOKENSERVER = 'tokenserver'
    PORT = 'port'
    GUROBI_LOGFILE_NAME = 'gurobi_logfile_name'
    OUTPUT_FLAG = 'OutputFlag'
    OPTIMALITY_TOL = 'OptimalityTol'
    BAR_CONV_TOL = 'BarConvTol'
    BAR_QCP_CONV_TOL = 'BarQCPConvTol'
    BAR_ITER_LIMIT = 'BarIterLimit'
    FEASIBILITY_TOL = 'FeasibilityTol'
    THREADS = 'Threads'
    PRESOLVE = 'Presolve'
    PYTHON_PRESOLVE = "python_presolve"
    NUMERIC_FOCUS = 'NumericFocus'
    ENVIRONMENT = 'ENVIRONMENT'
    SYSLOG_UDP = 514
    CLUSTER_OPTION = 'cluster'  # cluster type, should be Census or EDU
    CENSUS_CLUSTER = 'Census'
    EDU_CLUSTER = 'EDU'         # if cluster_option=EDU use academic gurobi
    GRB_APP_NAME = "GRB_APP_NAME"
    GRB_ISV_NAME = "GRB_ISV_NAME"
    GRB_ENV3 = "GRB_Env3"
    GRB_ENV4 = "GRB_Env4"
    TIME_LIMIT = "TimeLimit"
    SAVE_LP_PATH = "save_lp_path"
    SAVE_LP_PATTERN = 'save_lp_pattern'
    SAVE_LP_PATH_DEFAULT = '$DAS_S3ROOT/lpfiles/$JBID/$MISSION_NAME'
    SAVE_LP_SECONDS = 'save_lp_seconds'
    SAVE_LP_SECONDS_DEFAULT = 10000
    RANDOM_REPORT_FREQUENCY = 'random_report_frequency'

    OPTIMIZER = 'optimizer'

    # This means that the maximum retry time would be (2^17 + (random number
    # between 0 and 1)) * 0.01 which would be about 1310 seconds (21.8 minutes)
    # and the summation of all the times would be on the order of (2^18-1 +
    # 0.5*17)*0.01 which is 2621.515 seconds or about 43 minutes.
    GUROBI_LICENSE_MAX_RETRIES = 17
    GUROBI_LICENSE_RETRY_EXPONENTIAL_BASE = 2.000
    GUROBI_LICENSE_RETRY_JITTER = 1
    GUROBI_LICENSE_RETRY_QUANTUM = 0.01

    L2_GRB_METHOD = 'l2_grb_method'
    L2_GRB_PRESOLVE = 'l2_grb_presolve'
    L2_GRB_PRESPARSIFY = 'l2_grb_presparsify'
    NONNEG_LB = "nonneg_lb"

    RECORD_GUROBI_STATS_OPTION = 'record_gurobi_stats'

    # Sequential optimizers (which coordinate L2, Rounder passes)

    SEQ_OPT_OPTIMIZATION_APPROACH = "seq_optimization_approach"                         # config option name
    L2_PLUS_ROUNDER_WITH_BACKUP = "L2PlusRounderWithBackup"                             # The option possible values
    L2_PLUS_ROUNDER_WITH_BACKUP_INTERLEAVED = "L2PlusRounderWithBackup_interleaved"     # The option possible values

    # L2 optimization approaches
    L2_OPTIMIZATION_APPROACH = 'l2_optimization_approach'                               # config option name
    DATA_IND_USER_SPECIFIED_QUERY_NPASS = 'DataIndUserSpecifiedQueriesNPass'            # The option possible values
    DATA_IND_USER_SPECIFIED_QUERY_NPASS_PLUS = "DataIndUserSpecifiedQueriesNPassPlus"   # The option possible values
    SINGLE_PASS_REGULAR = 'SinglePassRegular'                                           # The option possible values

    # For DATA_IND_USER_SPECIFIED_QUERY_NPASS, tolerance options
    DATA_IND_NPASS_TOL_TYPE = "DataIndNPass_toleranceType"                              # config option name
    CONST_TOL = "const_tol"                                                             # The option possible values
    CONST_TOL_VAL = "const_tol_val"                                                     # The option possible values
    OPT_TOL = "opt_tol"                                                                 # The option possible values
    OPT_TOL_SLACK = "opt_tol_slack"

    # Rounder optimization approaches                                                   # config option name
    ROUNDER_OPTIMIZATION_APPROACH = 'rounder_optimization_approach'                     # The option possible values
    CELLWISE_ROUNDER = 'CellWiseRounder'                                                # The option possible values
    MULTIPASS_ROUNDER = 'MultipassRounder'                                              # The option possible values
    MULTIPASS_QUERY_ROUNDER = 'MultipassQueryRounder'
    OUTER_PASS = 'outer_pass'

    PRINT_GUROBI_STATS_OPTION = 'print_gurobi_stats'

    L2_ACCEPTABLE_STATUSES = 'L2_acceptable_statuses'
    ROUNDER_ACCEPTABLE_STATUSES = 'Rounder_acceptable_statuses'

    ###########################################
    # GEODICT
    ###########################################
    GEOLEVELS = "geolevels"
    GEODICT = "geodict"
    GEODICT_GEOLEVELS = "geolevel_names"
    GEODICT_LENGTHS = "geolevel_leng"
    GEOCODE_LENGTH = "geocode_length"
    GEO_TOPLEVEL = "geo_toplevel"
    GEO_BOTTOMLEVEL = "geo_bottomlevel"
    GEO_PATH = "geo_path"
    SPINE = "spine"
    AIAN_AREAS = "aian_areas"
    ENTITY_THRESHOLD = "entity_threshold"
    FANOUT_CUTOFF = "fanout_cutoff"
    REDEFINE_COUNTIES = "redefine_counties"
    BYPASS_CUTOFF = "bypass_cutoff"
    OPT_SPINE = "opt_spine"
    AIAN_SPINE = "aian_spine"
    NON_AIAN_SPINE = "non_aian_spine"
    SPINE_TYPE_ALLOWED = (OPT_SPINE, AIAN_SPINE, NON_AIAN_SPINE)
    AIAN_RANGES_PATH = "aian_ranges_path"
    STRONG_MCD_STATES = "strong_mcd_states"
    IGNORE_GQS_IN_BLOCK_GROUPS = "ignore_gqs_in_block_groups"
    TARGET_ORIG_BLOCK_GROUPS = "target_orig_block_groups"
    TARGET_DAS_AIAN_AREAS = "target_das_aian_areas"

    NOT_AN_AIAN_AREA = "9" * 4
    NOT_AN_AIAN_STATE = "99"
    NOT_A_PLACE = "9" * 5
    NOT_AN_OSE = "9" * 5
    NOT_AN_AIAN_TRACT = "9" * 11
    NOT_AN_AIAN_BLOCK = "9" * 16
    NOT_A_MCD = "9" * 5
    STRONG_MCD_COUNTY = "9" * 3

    ## Geolevel names:
    GEOLEVEL_TRACT = "Tract"
    GEOLEVEL_BLOCK = "Block"
    GEOLEVEL_BLOCK_GROUP = "Block_Group"
    GEOLEVEL_COUNTY = "County"
    GEOLEVEL_STATE = "State"
    GEOLEVEL_US = "US"
    GEOLEVEL_USPR = "US+PR"

    DEFAULT_GEOCODE_DICT = {
        GEOLEVEL_STATE: 2,
        GEOLEVEL_COUNTY: 5,
        GEOLEVEL_TRACT: 11,
        GEOLEVEL_BLOCK_GROUP: 12,
        GEOLEVEL_BLOCK: 16
    }

    ###########################################
    # Workload Optimization (HDMM)
    ###########################################
    HDMM_STRATEGY_TYPE = "strategy_type"
    HDMM = "hdmm"
    PIDENTITY_STRATEGY = "pidentity"
    MARGINAL_STRATEGY = "marginal"
    NONE_STRATEGY = "none_strategy"
    WORKLOAD_WEIGHT_NAME = "workload_weight_name"
    UNIFORM_WORKLOAD_WEIGHTS = "uniform_workload_weights"
    PS_PARAMETERS = "ps_parameters"

    #########################
    # checkpoint
    ############################
    CHECKPOINT = "checkpoint"
    CHECKPOINT_POSTFIX = "checkpoint_postfix"
    CHECKPOINT_SECTION = "checkpoint"
    CHECKPOINT_IGNORE_READ = "checkpoint_ignore_read"
    CHECKPOINT_IGNORE_WRITE = "checkpoint_ignore_write"

    ######################################
    # [writer]
    #####################################
    DESCRIPTIVE_NAME = "descriptive_name"
    RUN_TYPE = 'run_type'
    DEV_RUN_TYPE = 'dev'
    PROD_RUN_TYPE = 'prod'
    DEV_RUN_TYPE_PATH = f'$DAS_S3ROOT/runs/{DEV_RUN_TYPE}/$JBID'
    PROD_RUN_TYPE_PATH = f'$DAS_S3ROOT/runs/{PROD_RUN_TYPE}/$JBID'
    OUTPUT_PATH = 'output_path'
    WRITER = "writer"
    WRITER_SECTION = "writer"
    OVERWRITE_FLAG = "overwrite_flag"
    DEFAULT_CLASSIFICATION_LEVEL = "Classification level not specified in config file"
    CLASSIFICATION_LEVEL = "classification_level"
    SPLIT_BY_STATE = "split_by_state"
    STATE_CODES = "state_codes"
    FEAS_DICT_JSON = "feas_dict.json"
    OUTPUT_FNAME = "output_fname"  # deprecated
    OUTPUT_DATAFILE_NAME = 'output_datafile_name'
    WRITE_METADATA = 'write_metadata'
    WRITE_METADATA_DEFAULT = True
    WRITE_ALL_GEOLEVELS = "write_all_geolevels"
    KEEP_ATTRS = "keep_attrs"
    PRODUCE = "produce_flag"
    NUM_PARTS = "num_parts"
    NOISY_PARTITIONS_BY_LEVEL = "noisy_partitions_by_level"
    MINIMIZE_NODES = "minimize_nodes"
    # JSON = "json"
    # WRITE_JSON_KEEP_ATTRS = "json_keepAttrs"
    # BLANK = ""
    GIT_EXECUTABLE = "git"
    UTF_8 = "utf-8"
    GIT_COMMIT = "git_commit"   # must be the dashboard das_runs column name, not the command
    SAVE_GIT_COMMIT = "save_git_commit"
    S3CAT = 's3cat'
    S3CAT_DEFAULT = True
    S3CAT_VERBOSE = 's3cat_verbose'
    S3CAT_SUFFIX = 's3cat_suffix'
    MDF_PARTITIONS_FOR_S3CAT = 50
    MULTIWRITER_WRITERS = "multiwriter_writers"
    MAX_S3_PATH_LEN = 2048
    MAX_SAVED_S3_FILES_PER_GEOLEVEL = 50
    DAS_S3_LOGFILE_TEMPLATE = "{DAS_S3ROOT}-logs/{CLUSTERID}/DAS/das2020logs/{fname}"

    # Certificate
    CERTIFICATE_FILENAME = 'das2020_certificate.tex'
    CERTIFICATE_SUFFIX   = 'CERTIFICATE_SUFFIX'
    CERTIFICATE_NAME     = 'CERTIFICATE_NAME'
    CERTIFICATE_TITLE    = 'CERTIFICATE_TITLE'
    CERTIFICATE_PERSON1  = 'CERTIFICATE_PERSON1'
    CERTIFICATE_TITLE1   = 'CERTIFICATE_TITLE1'
    CERTIFICATE_PERSON2  = 'CERTIFICATE_PERSON2'
    CERTIFICATE_TITLE2   = 'CERTIFICATE_TITLE2'
    CERTIFICATE_PERSON3 = 'CERTIFICATE_PERSON3'
    CERTIFICATE_TITLE3 = 'CERTIFICATE_TITLE3'
    DRB_CLR_NUM = 'DRB_clearance_number'


    ###########################################
    # Config Constants (Analysis)
    ###########################################
    ANALYSIS = "Analysis"
    ANALYSIS_SECTIOn = 'analysis'
    INPUT_PATH = "input_path"
    SUM = "sum"
    COLLECT = "collect"
    AGGTYPES = "aggtypes"
    QUERIES = "queries"
    METRICS = "metrics"
    RUN_PREFIX_OPTION = "run_prefix"
    RUN_CONTENTS_OPTION = "run_contents"
    SCHEMA = "schema"
    GROUPS = "groups"
    ANALYSIS_GEODICT = "Geodict"
    NUMCORES = "numcores"
    RECALCULATE_RESULTS = "recalculate_results"
    REBUILD_RESMAS = "rebuild_resmas"
    ANALYSIS_LOG = "analysis_logfile"
    VALIDATOR_SECTION = "validator"
    VALIDATE_AT_LEVEL = "validate_at_level"

    SLDL = "SLDL"
    SLDU = "SLDU"

    JOB_FLOW_JSON = '/emr/instance-controller/lib/info/job-flow.json'
    LOCAL1_LOG_PATH = '/var/log/local1.log'
    DEFAULT_LOG_FILE = LOCAL1_LOG_PATH
    FIND_JSON_RE = re.compile(r"(\{.*\})") # extract a JSON object

    # when statistics are recorded they are associated with a point:
    POINT_ENV_CREATE     = 0
    POINT_MODBUILD_END   = 1
    POINT_PRESOLVE_START = 2
    POINT_PRESOLVE_END   = 3
    POINT_OPTIMIZE_END   = 4
    POINT_FAILSAFE_START = 10
    POINT_FAILSAFE_END   = 11
    POINT_NAMES = {POINT_ENV_CREATE: 'ENV_CREATE',
                   POINT_MODBUILD_END: 'MODEL_BUILD_END',
                   POINT_PRESOLVE_START: 'PRESOLVE_START',
                   POINT_PRESOLVE_END: 'PRESOLVE_END',
                   POINT_OPTIMIZE_END: 'OPTIMIZE_END',
                   POINT_FAILSAFE_START: 'FAILSAFE_START',
                   POINT_FAILSAFE_END: 'FAILSAFE_END'}

    PLOT_HUES = {'US': 0,
                 'USA': 0,
                 'US+PR': 0,
                 'State': 0.2,
                 'County': 0.4,
                 'Enumeration District': 0.5,
                 'Tract_Group': 0.5,
                 'Tract': 0.6,
                 'Block_Group': 0.8,
                 'Block': 0.9}

    ################################################################
    ### Stages #####################################################
    ################################################################
    STAGE_LEVELWISE = "STAGE_LEVELWISE"
    STAGE_TOPDOWN = "STAGE_TOPDOWN"
    STAGE_BOTTOMUP = "STAGE_BOTTOMUP"
    STAGE_ARBITRARY = "STAGE_ARBITRARY"
    STAGE_CHECKPOINT = "STAGE_CHECKPOINT"
    ################################################################
    ### Monitoring #################################################
    ################################################################
    MONITORING_SECTION = 'monitoring'
    PRINT_HEARTBEAT = 'print_heartbeat'
    PRINT_HEARTBEAT_FREQUENCY = 'print_heartbeat_frequency'
    HEARTBEAT_FREQUENCY = 'Heartbeat_frequency'
    HEARTBEAT_FREQUENCY_DEFAULT = 60
    SEND_STACKTRACE = 'send_stacktrace'

    NOTIFY_DASHBOARD_GUROBI_SUCCESS = 'notify_dashboard_gurobi_success'
    NOTIFY_DASHBOARD_GUROBI_RETRY = 'notify_dashboard_gurobi_retry'
    DAS_OBJECT_CACHE_ENV          = 'DAS_OBJECT_CACHE'
    DVS_OBJECT_CACHE_ENV          = 'DVS_OBJECT_CACHE'
    DAS_SQS_URL                   = 'DAS_SQS_URL'
    DASHBOARD_DESTINATION         = 'DASHBOARD_DESTINATION'

    ####################################
    #  Workload Names                  #
    ####################################
    WORKLOAD_DHCP_HHGQ = "DHCP_HHGQ_Workload"
    WORKLOAD_DHCP_HHGQ_DETAILED = "DHCP_HHGQ_Workload_Detailed"
    WORKLOAD_DHCP_PL94_CVAP = "DHCP_PL94_CVAP"
    WORKLOAD_HOUSEHOLD2010 = "HOUSEHOLD2010_WORKLOAD"
    WORKLOAD_DETAILED = "GENERIC_WORKLOAD_DETAILED"
    WORKLOAD_DHCP_TEST = "DHCP_TEST"
    DEMO_PRODUCTS_WORKLOAD = "demo_products_workload"

    ####################################
    # Workload weight names            #
    ####################################
    DEMO_PRODUCTS_WEIGHTS = "demo_products_weights"

    ####################################
    # Schema names for SchemaMaker     #
    ####################################
    SCHEMA_REDUCED_DHCP_HHGQ = "REDUCED_DHCP_HHGQ_SCHEMA"
    SCHEMA_SF1 = "SF1_SCHEMA"
    SCHEMA_HOUSEHOLD2010 = "HOUSEHOLD2010_SCHEMA"
    SCHEMA_HOUSEHOLD2010_TENVACS = "HOUSEHOLD2010TENVACS_SCHEMA"
    SCHEMA_HOUSEHOLDSMALL = "HOUSEHOLDSMALL_SCHEMA"
    # SCHEMA_TENUNIT2010 = "HOUSEHOLDTENURE2010_SCHEMA"
    SCHEMA_1940 = "1940_SCHEMA"
    SCHEMA_PL94 = "PL94_SCHEMA"
    SCHEMA_PL94_2020 = "PL94_2020_SCHEMA"
    SCHEMA_PL94_CVAP = "PL94_CVAP_SCHEMA"
    SCHEMA_PL94_P12 = "PL94_P12_SCHEMA"
    SCHEMA_DHCH_LITE = "DHCH_LITE_SCHEMA"
    SCHEMA_SF1_JOIN = "SF1_JOIN_SCHEMA"
    SCHEMA_TEN_UNIT_2010 = "TEN_UNIT_2010_SCHEMA"
    SCHEMA_UNIT_TABLE_10 = "UNIT_TABLE_10_SCHEMA"
    SCHEMA_UNIT_TABLE_10_VACS = "UNIT_TABLE_10_SCHEMA_VACS"
    SCHEMA_UNIT_TABLE_10_SIMPLE = "UNIT_TABLE_10_SIMPLE_SCHEMA"
    SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED = "UNIT_TABLE_10_SIMPLE_RECODED_SCHEMA"
    SCHEMA_UNIT_TABLE_10_DHCP = "UNIT_TABLE_10_DHCP_SCHEMA"
    SCHEMA_DHCP = "DHCP_SCHEMA"
    SCHEMA_DHCH = "DHCH_SCHEMA"
    SCHEMA_DHCH_small_hhtype = "SCHEMA_DHCH_small_hhtype"
    SCHEMA_H1 = "H1_SCHEMA"
    SCHEMA_H1_2020 = "H1_2020_SCHEMA"
    SCHEMA_CVAP = "CVAP_SCHEMA"

    DAS_PL94 = "PL94"
    DAS_PL94_CVAP = "PL94_CVAP"
    DAS_PL94_P12 = "PL94_P12"
    DAS_1940 = "1940"
    DAS_SF1 = "SF1_Person"
    DAS_DHCP_HHGQ = "DHCP_HHGQ"
    DAS_Household2010 = "Household2010"
    DAS_TenUnit2010 = "TenUnit2010"

    # query crosses should be denoted by separating variables with an asterisk padded by whitespace
    # Example: "hhgq * sex * age"
    SCHEMA_CROSS_SPLIT_DELIM = r"^\s+|\s*\*\s*|\s+$"
    SCHEMA_CROSS_JOIN_DELIM = " * "

    #########################################
    #  Attribute Names for MDF.Unit Output  #
    #########################################
    ATTR_MDF_SCHEMA_TYPE_CODE = "SCHEMA_TYPE_CODE"
    ATTR_MDF_SCHEMA_BUILD_ID = "SCHEMA_BUILD_ID"
    ATTR_MDF_TABBLKST = "TABBLKST"
    ATTR_MDF_TABBLKCOU = "TABBLKCOU"
    ATTR_MDF_TABTRACTCE = "TABTRACTCE"
    ATTR_MDF_TABBLKGRPCE = "TABBLKGRPCE"
    ATTR_MDF_TABBLK = "TABBLK"

    ATTR_MDF_RTYPE = "rtype"
    ATTR_MDF_GQTYPE = "gqtype"
    ATTR_MDF_TEN = "ten"
    ATTR_MDF_VACS = "vacs"
    ATTR_MDF_HHSIZE = "hhsize"
    ATTR_MDF_HHT = "hht"
    ATTR_MDF_HHT2 = "hht2"
    ATTR_MDF_CPLT = "cplt"
    ATTR_MDF_UPART = "upart"
    ATTR_MDF_MULTG = "multg"
    ATTR_MDF_HHLDRAGE = "THHLDRAGE"
    ATTR_MDF_HHSPAN = "THHSPAN"
    ATTR_MDF_HHRACE = "THHRACE"
    ATTR_MDF_PAOC = "paoc"
    ATTR_MDF_P18 = "TP18"
    ATTR_MDF_P60 = "TP60"
    ATTR_MDF_P65 = "TP65"
    ATTR_MDF_P75 = "TP75"
    ATTR_MDF_PAC = "pac"
    ATTR_MDF_HHSEX = "hhsex"


    ####################################
    #  Attribute Names for SchemaMaker #
    ####################################
    # DHCP
    ATTR_HHGQ = "hhgq"
    ATTR_SEX = "sex"
    ATTR_VOTING = "votingage"
    ATTR_HISP = "hispanic"
    ATTR_CENRACE = "cenrace"
    ATTR_CVAPRACE = "cvaprace"
    ATTR_RACE = "race"
    ATTR_AGE = "age"
    ATTR_AGECAT = "agecat"
    ATTR_CITIZEN = "citizen"
    ATTR_REL = "rel"
    ATTR_RELGQ = "relgq"
    ATTR_CENRACE_DAS = "cenrace_das"  # Used for input recoding
    # DHCH
    # ATTR_HHSEX_DHCH = "hhsex_dhch"
    # ATTR_HHAGE_DHCH = "hhage"
    # ATTR_HHHISP_DHCH = "hhhisp"
    # ATTR_HHRACE_DHCH = "hhrace_dhch"
    # ATTR_ELDERLY_DHCH = "elderly"
    # ATTR_TENSHORT_DHCH = "tenshort"
    ATTR_HHTYPE_DHCH = "hhtype_dhch"

    ATTR_HHAGE = "hhage"
    ATTR_HHHISP = "hisp"
    ATTR_HHRACE = "hhrace"
    ATTR_HHSIZE = "size"
    ATTR_HHTYPE = "hhtype"
    ATTR_HHELDERLY = "elderly"
    ATTR_HHTENSHORT = "hhtenshort"

    ATTR_HHSEX = "hhsex"

    # Recode variables necessary for producing HHTYPE
    ATTR_SIZE = "SIZE"
    ATTR_CHILD_UNDER_18 = "CHILD_UNDER_18"
    ATTR_OWN_CHILD_UNDER_6_ONLY = "OWN_CHILD_UNDER_6_ONLY"
    ATTR_OWN_CHILD_UNDER_18 = "OWN_CHILD_UNDER_18"
    ATTR_OWN_CHILD_BETWEEN_6_AND_17 = "OWN_CHILD_BETWEEN_6_AND_17"
    ATTR_OWN_CHILD_IN_BOTH_RANGES = "OWN_CHILD_IN_BOTH_RANGES"
    ATTR_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER = "CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER"
    ATTR_CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER = "CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER"
    ATTR_CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER = "CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER"
    ATTR_CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER = "CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER"
    ATTR_MULTIG = "MULTIG"
    ATTR_MARRIED = "MARRIED"
    ATTR_MARRIED_SAME_SEX = "MARRIED_SAME_SEX"
    ATTR_MARRIED_OPPOSITE_SEX = "MARRIED_OPPOSITE_SEX"
    ATTR_COHABITING = "COHABITING"
    ATTR_COHABITING_SAME_SEX = "COHABITING_SAME_SEX"
    ATTR_COHABITING_OPPOSITE_SEX = "COHABITING_OPPOSITE_SEX"
    ATTR_NO_SPOUSE_OR_PARTNER = "NO_SPOUSE_OR_PARTNER"
    ATTR_WITH_RELATIVES = "WITH_RELATIVES"
    ATTR_NOT_ALONE = "NOT_ALONE"
    ATTR_NO_OWN_CHILDREN_UNDER_18 = "NO_OWN_CHILD_UNDER_18"
    ATTR_NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER = "NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER"
    ATTR_NO_RELATIVES = "NO_RELATIVES"

    ATTR_CPLT = "couple_type"
    ATTR_UPART = "presence_of_unmarried_partner"
    ATTR_HHLDRAGE = "age_of_householder"
    ATTR_HHSPAN = "hispanic_householder"
    ATTR_PAOC = "presence_and_age_of_own_children_under_18"
    ATTR_AC = "presence_of_child"
    ATTR_OC = "own_child"
    ATTR_PAC = "presence_and_age_of_children_under_18"
    # Only attribute for the Unit table to be used
    ATTR_TENVACGQ = "tenvacgq"


    #Old/experimental DHCH attributes
    ATTR_HHMULTI = "multi"
    ATTR_TENURE = "tenure"
    ATTR_TEN2LEV = "ten2"


    ATTR_VACS = "vacs"
    VACANT = "vacant"

    ATTR_H1 = "h1"
    VACANT_COUNT = "vacant_count"

    # UNIT TABLE 10
    ATTR_HHGQ_UNIT = "hhgq_unit"
    ATTR_HHGQ_UNIT_SIMPLE = "hhgq_unit_simple"
    ATTR_HHGQ_UNIT_SIMPLE_RECODED = "hhgq_unit_simple_recoded"

    HHGQ_UNIT_VECTOR = "hhgq_vector"
    HHGQ_UNIT_HOUSING = "housing_units"
    HHGQ_UNIT_TOTALGQ = "total_gq"
    HHGQ_8LEV = "hhgq8lev"
    SPOUSES_UNMARRIED_PARTNERS = "spouses_unmarried_partners"
    #RELGQ_SPOUSE_OR_PARTNER = "relgq_spouse_or_partner"
    PARENTS = "parents"
    PEOPLE_IN_HOUSEHOLDS = "people_in_households"



    AGECAT_VOTINGAGE = "votingage"
    # AGECAT_VOTING = "voting"
    # AGECAT_NONVOTING = "nonvoting"
    AGE_ONLY_100_PLUS = "age_only_100_plus"

    # DHCP Unit Table 10 20190610
    # ATTR_HHGQ_UNIT_DHCP = "hhgq_unit_dhcp" -- Renamed it to HHGQ="hhgq", no longer needed

    # 1940

    ATTR_VOTING_AGE = "votingage"
    VOTING_TOTAL = "voting"
    NONVOTING_TOTAL = "nonvoting"

    ATTR_HHGQ_1940 = "hhgq1940"
    HHGQ_1940_HOUSEHOLD_TOTAL = "household"
    HHGQ_1940_GQLEVELS = "gqlevels"
    HHGQ_1940_INSTLEVELS = "instlevels"
    HHGQ_1940_MENTAL_TOTAL = "gqMentalTotal"
    HHGQ_1940_ELDERLY_TOTAL = "gqElderlyTotal"
    HHGQ_1940_ROOMING_TOTAL = "gqRoomingTotal"

    ATTR_CITIZEN_1940 = "citizen1940"
    ATTR_RACE_1940 = "cenrace1940"
    ATTR_AGE_1940 = "age1940"
    ATTR_SEX_1940 = "sex1940"
    ATTR_HISPANIC_1940 = "hispanic1940"

    ####################################
    # Recode Names for SchemaMaker     #
    ####################################
    # DHCP
    GENERIC_RECODE_TOTAL = "total"
    GENERIC_RECODE_DETAILED = "detailed"

    HISP_TOTAL = "hispTotal"
    HISP_NOT_TOTAL = "notHispTotal"

    SEX_MALE = "male"
    SEX_FEMALE = "female"
    SEX_VECT = "sex_vect"

    CENRACE_NUM = "numraces"
    CENRACE_MAJOR = "majorRaces"
    CENRACE_COMB = "racecomb"
    CENRACE_11_CATS = "cenrace11cats"
    CENRACE_WHITEALONE = "whiteAlone"
    CENRACE_BLACKALONE = "blackAlone"
    CENRACE_AIANALONE = "aianAlone"
    CENRACE_ASIANALONE = "asianAlone"
    CENRACE_NHOPIALONE = "nhopiAlone"
    CENRACE_SORALONE = "sorAlone"
    CENRACE_TOMR = "tomr"
    CENRACE_ALLRACES = "allraces"
    CENRACE_RACECOMBOS = "racecombos"
    RACE7LEV = "race7lev"
    CENRACE_7LEV_TWO_COMB = "cenrace_7lev_two_comb"
    CENRACE_WHITECOMBO = "whitecombo"
    CENRACE_BLACKCOMBO = "blackcombo"
    CENRACE_AIANCOMBO = "aiancombo"
    CENRACE_ASIANCOMBO = "asiancombo"
    CENRACE_NHOPICOMBO = "nhopicombo"
    CENRACE_SORCOMBO = "sorcombo"
    CENRACE_ONERACE = "onerace"
    CENRACE_TWORACES = "tworaces"
    CENRACE_THREERACES = "threeraces"
    CENRACE_FOURRACES = "fourraces"
    CENRACE_FIVERACES = "fiveraces"
    CENRACE_SIXRACES = "sixraces"

    RELGQ_HOUSEHOLDERS = "householders"

    HHGQ_HOUSEHOLD_TOTAL = "householdTotal"
    HHGQ_2LEVEL = "hhgq_2level"
    HHGQ_GQLEVELS = "gqlevels"
    HHGQ_INSTLEVELS = "instlevels"
    HHGQ_HHINSTLEVELS = "hhinstlevels"
    HHGQ_GQTOTAL = "gqTotal"
    HHGQ_INST_TOTAL = "instTotal"
    HHGQ_CORRECTIONAL_TOTAL = "gqCorrectionalTotal"
    HHGQ_JUVENILE_TOTAL = "gqJuvenileTotal"
    HHGQ_NURSING_TOTAL = "gqNursingTotal"
    HHGQ_OTHERINST_TOTAL  = "gqOtherInstTotal"
    HHGQ_NONINST_TOTAL = "noninstTotal"
    HHGQ_COLLEGE_TOTAL = "gqCollegeTotal"
    HHGQ_MILITARY_TOTAL = "gqMilitaryTotal"
    HHGQ_OTHERNONINST_TOTAL = "gqOtherNoninstTotal"
    HHGQ_MAJOR_TYPES = "hhgqMajorTypes"
    HHGQ_SPINE_TYPES = "hhgqSpineTypes"
    HHGQ_INST_NONINST = "hhgqInstNoninst"
    AGE_5YEARGROUPS = "5yeargroups"
    AGE_CAT43 = "agecat43"
    AGE_GROUP4 = "agegroup4"
    AGE_GROUP16 = "agegroup16"
    AGE_GROUP64 = "agegroup64"
    AGE_VOTINGLEVELS = "votingage"  # It's votingage with voting age and age cat, and many .ini files
    # AGE_VOTING_TOTAL = VOTING_TOTAL
    # AGE_NONVOTING_TOTAL = NONVOTING_TOTAL
    AGE_UNDER15_TOTAL = "under15total"
    AGE_PREFIX_AGECATS = "prefix_agecats"
    AGE_RANGE_AGECATS = "range_agecats"
    AGE_BINARYSPLIT_AGECATS = "binarysplit_agecats"
    AGE_LT15_TOTAL = "age_lessthan15"
    AGE_LT20_TOTAL = "age_lessthan20"
    AGE_LT30_TOTAL = "age_lessthan30"
    AGE_GT25_TOTAL = "age_greaterthan25"
    AGE_GT89_TOTAL = "age_greaterthan89"

    AGE_GT74_TOTAL = "age_gt74"
    AGE_GT20_TOTAL = "age_gt20"
    AGE_LT15_GT89_TOTAL = "age_lt15_gt89"
    AGE_LT3_GT30_TOTAL = "age_lt3_gt30"
    AGE_LT16_TOTAL = "age_lt16"
    AGE_LT16_GT75_TOTAL = "age_lt16_gt75"
    AGE_LT17GT65_TOTAL = "age_lt17gt65"
    AGE_LT16GT65_TOTAL = "age_lt16gt65"
    AGE_PC03_LEVELS = "agecatPCO3"
    AGE_PC04_LEVELS = "agecatPCO4"
    AGE_CAT85PLUS = "agecat85plus"
    AGE_CAT100PLUS = "agecat100plus"
    AGE_CAT17PLUS = "agecat17plus"
    AGE_CAT25PLUS = "agecat25plus"
    AGE_18PLUS = "age18plus"
    AGE_POPEST3GROUPS = "agePopEst3groups"
    AGE_POPEST2GROUPS = "agePopEst2groups"
    AGE_POPEST16PLUS = "agePopEst16plus"
    AGE_POPEST18GROUPS = "agePopEst18groups"
    AGE_18_64_116 = "age_18_64_116"
    REL_PARENTS = "rel_parents"
    REL_CHILDREN = "rel_children"
    REL_CANTBEKIDS = "rel_cantbekids"

    # DHCH
    HHSEX_MALE = "male"
    HHSEX_FEMALE = "female"

    HHRACE_WHITEALONE = "whiteonly"

    HHSIZE_ONE = "size1"
    HHSIZE_TWO = "size2"
    HHSIZE_THREE = "size3"
    HHSIZE_FOUR = "size4"
    HHSIZE_TWOPLUS = "size2plus"
    HHSIZE_NONVACANT_VECT = "sizex0"
    HHSIZE_NOTALONE_VECT = "sizex01"
    HHSIZE_VACANT = "vacant"
    HHSIZE_ALONE_BINARY = "solo"

    HHTYPE_FAMILY = "family"
    HHTYPE_NONFAMILY = "nonfamily"
    HHTYPE_FAMILY_MARRIED = "marriedfamily"
    HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR = "married_with_children_indicator"
    HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS = "married_with_children_levels"
    HHTYPE_FAMILY_OTHER = "otherfamily"
    HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR = "other_with_children_indicator"
    HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS = "other_with_children_levels"
    HHTYPE_ALONE = "alone"
    HHTYPE_NOT_ALONE = "invert_alone"
    HHTYPE_NONFAMILY_NOT_ALONE = "notalone"
    HHTYPE_NOT_SIZE_TWO = "non2type"
    HHTYPE_SIZE_TWO_WITH_CHILD = "type2_onechild"
    HHTYPE_SIZE_TWO_WITH_CHILD_EXCLUDING_HH = "type2_onechildxhh"
    HHTYPE_SIZE_TWO_COUPLE = "type2_couple"
    HHTYPE_NOT_SIZE_THREE = "non3type"
    HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN = "type3_twochild"
    HHTYPE_SIZE_THREE_NOT_MULTI = "type3_notmulti"
    HHTYPE_SIZE_THREE_COUPLE_WITH_ONE_CHILD = "type3_couple_with_child"
    HHTYPE_SIZE_FOUR_NOT_MULTI = "type4_notmulti"
    HHTYPE_SIZE_FOUR_COUPLE_WITH_TWO_CHILDREN = "type4_couple_with_twochildren"
    HHTYPE_COHABITING = "cohabiting"
    HHTYPE_COHABITING_WITH_CHILDREN_INDICATOR = "cohabiting_with_children_indicator"
    HHTYPE_NO_SPOUSE_OR_PARTNER = "no_spouse_or_partner"
    HHTYPE_NO_SPOUSE_OR_PARTNER_LEVELS = "no_spouse_or_partner_levels"
    HHTYPE_COUPLE_LEVELS = "couplelevels"
    HHTYPE_OPPOSITE_SEX_LEVELS = "oppositelevels"
    HHTYPE_SAME_SEX_LEVELS = "samelevels"
    HHTYPE_NOT_MULTI = "never_multi"
    HHTYPE_OWNCHILD_UNDERSIX = "ownchild_under6"
    HHTYPE_OWNCHILD_UNDER18 = "ownchild_under18"
    HHTYPE_CHILD_ALONE = "child_alone"

    HHAGE_OVER65 = "hhover65"
    HHAGE_OVER24 = "hhover24"
    HHAGE_H18 = "hhageH18"
    HHAGE_15TO24 = "age_15to24"
    HHAGE_UNDER60 = "age_under60"
    HHAGE_60TO64 = "age_60to64"
    HHAGE_65TO74 = "age_65to74"
    HHAGE_75PLUS = "age_75plus"
    HHAGE_60PLUS = "age_60plus"

    HHELDERLY_PRESENCE_NOT_60PLUS = "pres_under60"
    HHELDERLY_PRESENCE_NOT_65PLUS = "pres_under65"
    HHELDERLY_PRESENCE_75PLUS = "elderly3"
    HHELDERLY_PRESENCE_OVER60 = "invert_elderly0"
    HHELDERLY_NO1UNDER60ORSOME1OVER64 = "invert_elderly1"
    HHELDERLY_NO1UNDER65ORSOME1OVER74 = "invert_elderly2"
    HHELDERLY_PRESENCE_NOT_75PLUS = "invert_elderly3"
    HHELDERLY_PRESENCE_60 = "presence60"
    HHELDERLY_PRESENCE_65 = "presence65"
    HHELDERLY_PRESENCE_75 = "presence75"

    HHMULTI_TRUE = "yes_multi"
    HHMULTI_FALSE = "no_multi"

    TEN_OWNED = "tenure_owned"
    TEN_2LEV = "tenure_2lev"

    ################################################################
    ### dashboard ##################################################
    ################################################################
    MISSION_NAME = 'MISSION_NAME'
    APPLICATIONID = 'applicationId'
    APPLICATIONID_ENV = 'APPLICATIONID'
    DAS_RUN_UUID = 'DAS_RUN_UUID'
    DAS_RUN_ID_LOWER = 'das_run_id'
    RUN_ID_LOWER = 'run_id'
    UNICODE_BULLET = "•"
    NUM_GEOLEVELS = 'num_geolevels'
    NUM_GEOUNITS = 'num_geounits'

    ################################################################
    ### [Stats]  ###################################################
    ################################################################

    START = 'START'
    """Property for the stats dictionary. Time an application is started"""
    STOP = 'STOP'
    """Property for the stats dictionary. Time an application is stoped"""

    TERMINATED = 'terminated'
    """Property for the stats dictionary. True if an application is terminated stoped"""

    FUNC = 'func'
    """Property for the stats dictionary. The function in which a stats dictionary was made"""

    STATS_SECTION = 'stats'
    STATS_DIR = "stats_dir"
    STATS_DIR_DEFAULT = "$DAS_S3ROOT/rpc/upload"
    NOTIFY_GC_MASTER = 'notify_gc_master'

    ## [gurobi stats]
    DEFAULT_GUROBI_STATISTICS_FILENAME = "$DAS_S3ROOT-logs/$CLUSTERID/DAS/runs/$APPLICATIONID/GurobiStatistics"
    GUROBI_STATISTICS_FILENAME = "gurobi_statistics_filename"
    STATISTICS_PATHNAME_DELIMITER = UNICODE_BULLET
    STATISTICS_PARTITIONS = "stats_partitions"
    MAX_STATISTICS_PARTITIONS = 100

    ################################################################
    ### [gurobi] ###################################################
    ################################################################
    THREADS_R2R    = 'threads_root2root'     # how many threads Gurobi to use for root-node-to-root-node optimization
    DEFAULT_THREADS_R2R       = 64
    THREADS_GEOLEVEL_PREFIX = "threads_"

    ###########################################
    # Reader Constants
    ###########################################
    READER = "reader"
    PATH = "path"
    TABLES = "tables"
    TABLE_CLASS = "class"
    VARS = "variables"
    RECODE_VARS = "recode_variables"
    PRE_RECODER = "recoder"
    LEGAL = "legal"
    VAR_TYPE = "type"
    NEWRECODER = "newrecoder"
    HEADER = "header"
    DELIMITER = "delimiter"
    CSV_COMMENT = "comment"
    LINKAGE = "linkage"
    CTABLES = "constraint_tables"
    PTABLE = "privacy_table"
    THECONSTRAINTS = "theConstraints"
    INVARIANTS = "Invariants"
    THEINVARIANTS = "theInvariants"
    GEOGRAPHY = "geography"
    HISTOGRAM = "histogram"
    MEASURE_RDD_TIMES = "measure_rdd_times"
    UNIQUE = "unique"
    PICKLEDPATH = "pickled.path"

    # spar_table partitioner
    NUM_READER_PARTITIONS = "numReaderPartitions"
    RANGE_PARTITION = "rangePartition"
    READER_PARTITION_LEN = "readerPartitionLen"
    GRFC_PATH = "grfc_path"
    # table_reader partitioner
    PARTITION_BY_BLOCK_GROUP = "partition_by_block_group"

    VALIDATE_INPUT_DATA_CONSTRAINTS = "validate_input_data_constraints"

    ###########################################
    # Error Metrics Constants
    ###########################################
    ERROR_METRICS = "error_metrics"
    QUERIES2MEASURE = "queries2measure"
    L1_RELATIVE_ERROR_GEOLEVELS = "l1_relative_error_geolevels"
    L1_RELATIVE_ERROR_QUERIES = "l1_relative_error_queries"
    POPULATION_CUTOFF = "population_cutoff"
    L1_RELATIVE_DENOM_QUERY = "l1_relative_denom_query"
    L1_RELATIVE_DENOM_LEVEL = "l1_relative_denom_level"
    TOTAL_POP_RELATIVE_ERROR_GEOLEVELS = "total_pop_relative_error_geolevels"
    PRINT_AIAN_STATE_TOTAL_L1_ERRORS = "print_aian_state_total_l1_errors"
    PRINT_COUNTY_TOTAL_AND_VOTINGAGE = "print_county_total_and_votingage"
    PRINT_H1_COUNTY_METRICS = "print_h1_county_metrics"
    PRINT_AIANS_L1_ERROR_ON_TOTAL_POP = "print_aians_l1_error_on_total_pop"
    PRINT_PLACE_MCD_OSE_BG_L1_ERROR_ON_TOTAL_POP = "print_place_mcd_ose_bg_l1_error_on_total_pop"
    PRINT_BLOCK_AND_COUNTY_TOTAL_POP_ERRORS = "print_block_and_county_total_pop_errors"
    PRINT_BLAU_QUINTILE_ERRORS = "print_blau_quintile_errors"
    PRINT_8_CELL_CENRACE_HISP_ERRORS = "print_8_cell_cenrace_hisp_errors"

    ###########################################
    # Config Constants (DAS)
    ###########################################
    CONFIG_INI = "config.ini"
    US_GEOCODE = ""
    ROOT_GEOCODE = ''  # was previously hard-coded in nodes.py:277
    PRE_RELEASE = 'pre_release'
    POSTPROCESS_ONLY = "postprocess_only"
    RELEASE_SUFFIX = '.release.zip'

    ###########################################
    # Spark Constants
    ###########################################

    SPARK = "spark"
    NAME = "name"
    MASTER = "master"
    LOGLEVEL = "loglevel"
    ZIPFILE = "ZIPFILE"
    # MAX_STATS_PARTS = 8  # number of parts for saving statistics
    SPARK_LOCAL_DIR = 'spark.local.dir'
    SPARK_EVENTLOG_DIR = 'spark.eventLog.dir'

    ################################################################
    ### [environment] ##############################################
    ##############################################
    ENVIRONMENT_SECTION = 'environment'

    ################################################################
    ### [setup] ####################################################
    ################################################################
    SETUP_SECTION = 'setup'
    SETUP_ENVIRONMENT = 'environment'
    EMR_TASK_NODES = 'emr_task_nodes'  # can also appear in [takedown]
    EMR_CORE_NODES = 'emr_core_nodes'

    ################################################################
    ### [takedown] #################################################
    ################################################################
    TAKEDOWN_SECTION = 'takedown'


    ################################################################
    ### [alert] ####################################################
    ################################################################
    ALERT_SECTION = 'alert'
    MESSAGE = 'message'


    ################################################################
    ### DVS    #####################################################
    ################################################################
    DVS_SECTION  = 'logging'    # dvs goes in the logging section
    DVS_ENABLED  = 'dvs_enabled'
    DVS_ENDPOINT = 'dvs_endpoint'
    DVS_DEFAULT  = False        # by default, it is disabled
    DVS_THREADS  = 90           # use 50 threads

#### EXPORTABLE CONSTANTS #####

CC   = _DAS_CONSTANTS()

###############################

# pylint: disable=bad-whitespace

## Legacy constants

OPTIMIZER = 'optimizer'

#YMDT_RE                 = re.compile(r'(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d:\d\d:\d\d)')
#STATS_URL               = 'https://dasexperimental.ite.ti.census.gov/das/runs/{year}/{month}/{basename}/'

################################################################
## From nodes.py
################################################################

RAW = "raw"
RAW_HOUSING = "raw_housing"
SYN = "syn"
UNIT_SYN = "unit_syn"
CONS = "cons"
INVAR = "invar"
GEOCODEDICT = "geocodeDict"
GEOCODE = "geocode"
GEOLEVEL = "geolevel"
DP_QUERIES = "dp_queries"
UNIT_DP_QUERIES = "unit_dp_queries"
PARENTGEOCODE = "parentGeocode"
DETAILED = "detailed"  # Detailed DPQuery name. Also a config value for it.

###########################################
# HDMM closed-form error engine constants
###########################################
HDMM_ERROR_MODE = "errorMode"
HDMM_ERROR_TYPE = "errorType"
HDMM_ERROR = "hdmmError"

TENABILITY_OUTPUT_ROOT = "tenabilityOutputRoot"

ERROR_GEOLEVELS = "errorGeolevels"
ERROR_EPSILONS = "errorEpsilons"

###########################################
# Geographic Level Constants (crosswalk.py and Analysis)
###########################################
US = 'US'
STATE = 'STATE'
COUNTY = 'COUNTY'
TRACT_GROUP = 'TRACT_GROUP'
TRACT = 'TRACT'
BLOCK_GROUP = 'BLOCK_GROUP'
BLOCK = 'BLOCK'

SLDU = 'SLDU'       # State Legislative District (Upper Chamber) (Year 1)
SLDL = 'SLDL'       # State Legislative District (Lower Chamber) (Year 1)

CD = 'CD'           # Congressional District (111th)
VTD = 'VTD'         # Voting District
COUSUB = 'COUSUB'   # County Subdivision (FIPS)
SUBMCD = 'SUBMCD'   # Subminor Civil Division (FIPS)
UA = 'UA'           # Urban Areas
CBSA = 'CBSA'       # Metropolitan Statistical Area
METDIV = 'METDIV'   # Metropolitan Division
CSA = 'CSA'         # Combined Statistical Area
UGA = 'UGA'         # Urban Growth Area
PUMA = 'PUMA'       # Public Use Microdata Area
PLACE = 'PLACE'     # Place (FIPS)
ZCTA5 = 'ZCTA5'     # ZIP Code Tabulation Area (5-digit)
