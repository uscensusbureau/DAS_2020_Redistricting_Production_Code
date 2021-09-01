ORIG = "orig"
PRIV = "priv"
GEOCODE = "geocode"
GEOLEVEL = "geolevel"
RUN_ID = "run_id"
BUDGET_GROUP = "budget_group"
PLB = "plb"
LEVEL = "level"
QUERY = "query"
QUERY_SIZE = "query_size"
WAS_MISSING = "was_missing"
TABLE_INDEX = "table_index"

S3_BASE = "s3://uscb-decennial-ite-das/users/"
LINUX_BASE = "/mnt/users/"

SPARSE = "sparse"
DENSE = "dense"

DECILES = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
QUARTILES = [0.0, 0.25, 0.5, 0.75, 1.0]
PERCENTILES = [x/100 for x in range(0,101)]

EXPERIMENT_FRAMEWORK_NESTED = "experiment_framework_nested"
EXPERIMENT_FRAMEWORK_FLAT = "experiment_framework_flat"

NOT_AN_AIAN_STATE = "99"
NOT_A_PLACE = "9" * 5
NOT_AN_OSE = "9" * 5
NOT_AN_AIAN_TRACT = "9" * 11
NOT_AN_AIAN_BLOCK = "9" * 16
NOT_A_COUNTY_NSMCD = "9" * 3
