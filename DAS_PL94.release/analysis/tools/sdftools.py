import os
import shutil
import das_utils as du
import numpy as np
import pandas
import time
import math
import multiprocessing as mp
from operator import add

from pyspark.sql import SparkSession

from constants import CC
import analysis.constants as AC
import constants as C
import analysis.tools.crosswalk as crosswalk
import functools
from programs.schema.schemas.schemamaker import SchemaMaker

from pyspark.sql import functions as sf
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, DecimalType, StringType, LongType, DoubleType, StructField, StructType


def aggregateGeolevels(spark_session, spark_df, geolevels, verbose=True):
    """
    Take a spark dataframe and aggregate blocks to get geounits at higher geolevels

    Parameters
    ==========
    spark_session : SparkSession
        Needed for accessing the crosswalk file that joins to the dataframe (if not already joined)

    spark_df : Spark DataFrame
        The data in spark histogram form (most likely)
        But it can also be a general spark dataframe as long as it contains the geocode column

    geolevels : str or list of str
        One or more geolevels that exist in the crosswalk file

    verbose  :  bool
        Explanations of operations will be printed if True

    Returns
    =======
    a spark dataframe with geounits at all geolevels specified
    """
    geolevels = du.aslist(geolevels)

    if not np.all([x in spark_df.columns for x in geolevels]):
        # grab only the geolevels that need to come from the crosswalk file
        # e.g. if STATE and COUNTY come from the buildGeolatticeTrunkColumns function, and the user wants
        #      [STATE, COUNTY, SLDL, SLDU], then SLDL and SLDU need to come from the crosswalk file, but
        #       STATE and COUNTY don't, since they already exist.
        join_geolevels = [x for x in geolevels if x not in spark_df.columns]
        crossdf = crosswalk.getCrosswalkDF(spark_session, join_geolevels)
        if verbose:
            print_item(crossdf, "Columns needed for geolevel aggregation aren't present in the df, so load the crosswalk file data")
        spark_df = spark_df.join(crossdf, on=AC.GEOCODE).persist()
        if verbose:
            print_item(spark_df, "Join the crosswalk file data to the df")

    for i, geolevel in enumerate(geolevels):
        numerical_columns = [x for x in spark_df.columns if x not in filterColumnsByType(spark_df, filter_dtype="string")]
        if verbose:
            print_item(numerical_columns, "The numerical columns, which are the ones we want to sum across when aggregating geolevels")
        group = [x for x in spark_df.columns if x not in [AC.GEOCODE] + geolevels + numerical_columns] + [geolevel]
        if verbose:
            print_item(group, f"What we want to group by. This excludes the columns {[AC.GEOCODE] + [x for x in geolevels if x not in [geolevel]] + numerical_columns} because we need to sum across the numeric columns and only group by the columns above to get the {geolevel} values.")
            print_item(spark_df.columns, "Columns in the df")
        geodf = spark_df.groupBy(group).sum().persist()
        if verbose:
            print_item(geodf, f"After selecting and grouping by {group}")
        geodf = stripSQLFromColumns(geodf)
        if verbose:
            print_item(geodf, "After removing additional info from the names of the columns")
        geodf = geodf.withColumnRenamed(geolevel, AC.GEOCODE).persist()
        if verbose:
            print_item(geodf, f"Rename the {geolevel} column to {AC.GEOCODE}")
        geodf = geodf.withColumn(AC.GEOLEVEL, sf.lit(geolevel)).persist()
        if verbose:
            print_item(geodf, f"Added column for {AC.GEOLEVEL} to maintain that these rows are associated with the geolevel {geolevel}")
        if i == 0:
            df = geodf.persist()
        else:
            df = df.unionByName(geodf).persist()

    if verbose:
        show(df, "The Geolevel DF")
        show(df.select(AC.GEOLEVEL).distinct(), "With these geolevels")

    return df


def answerQuery(spark_df, schema, queryname, labels=False, merge_dims=False, verbose=True):
    """
    Answers a single query using SQL

    Parameters
    ==========
    spark_df : Spark DataFrame
        The data in sparse histogram format

    schema : programs.schema.schema.Schema
        The schema for the data contained in the spark dataframe

    queryname : str
        The name of the query to answer

    labels : bool (default = False)
        True: Use level names rather than level indices
        False: Use level indices rather than level names

    merge_dims : bool (default = False)
        True: Merge all schema dimnames columns into a single column called 'level'
              and concatenate the levels of each column using the " BY " keyword
        False: Keep all schema dimnames columns that are associated with the query

        e.g. PL94 Schema
        query = "sex * votingage"

        merge_dims == False:
              sex    |   votingage       |       query
            -------------------------------------------------
            "Male"   | "Voting Age"      |  "sex * votingage"
            "Male"   | "Non-voting Age"  |  "sex * votingage"
            "Female" | "Voting Age"      |  "sex * votingage"
            "Female" | "Non-voting Age"  |  "sex * votingage"

        merge_dims == True:
                     level               |       query
            -------------------------------------------------
            "Male BY Voting Age"         |  "sex * votingage"
            "Male BY Non-voting Age"     |  "sex * votingage"
            "Female BY Voting Age"       |  "sex * votingage"
            "Female BY Non-voting Age"   |  "sex * votingage"


    Returns
    =======
    a spark dataframe with the query answered

    Notes
    =====
    - adds a column for the query_name
    - only computes one query at a time
    - returned dataframes can be unioned easily if merge_dims is True; if False, it requires more manual effort to union them
      since they will contain different numbers of columns depending on the queries asked
    - for unioning dataframes where merge_dims is False, each one can be transformed into the 'level' 'query' format
      by using the makeSingleQueryColumn function in this module
    - this function will group by all non-numeric columns, meaning that it will aggregate over each of the numeric columns
        - this sometimes makes sense (i.e. for 'orig' and 'priv'), but sometimes may not (i.e. 'L1')
    """
    assert isinstance(queryname, str), "Since you can only ask a single query at a time, the 'queryname' must be a string"
    seed = schema.getQuerySeed(queryname)
    sqldict = seed.getSparkSQL(labels)

    # use the sql dictionary to recode the levels in each applicable column
    for i,(dim,sql) in enumerate(sqldict.items()):
        spark_df = spark_df.withColumn(dim, sf.expr(sql)).persist()
        # queries that contain subsets will oftentimes contain "-1"s from the sql expression
        # we will want to remove the corresponding "-1" levels
        spark_df = spark_df.filter(sf.col(dim) != "-1").persist()

    for dim in [x for x in schema.dimnames if x not in seed.keepdims]:
        spark_df = spark_df.drop(dim).persist()

    numerical_columns = [x for x in spark_df.columns if x not in filterColumnsByType(spark_df, filter_dtype="string")]
    group = [x for x in spark_df.columns if x not in numerical_columns]
    if verbose:
        print_item(numerical_columns, f"Answer query '{queryname}' by summing rows in each of the the above columns, grouped by {group}")
    spark_df = spark_df.groupBy(group).sum().persist()

    if merge_dims:
        spark_df = makeSingleQueryColumn(spark_df, schema).persist()

    spark_df = spark_df.withColumn(AC.QUERY, sf.lit(queryname)).persist()
    spark_df = stripSQLFromColumns(spark_df)

    return spark_df


def answerQueries(sparkdf, schema, querynames, labels=True, verbose=True):
    """
    Answers one or more queries using SQL

    Parameters
    ==========
    spark_df : Spark DataFrame
        The data in sparse histogram format

    schema : programs.schema.schema.Schema
        The schema for the data contained in the spark dataframe

    queryname : str
        The name of the query to answer

    labels : bool (default = False)
        True: Use level names rather than level indices
        False: Use level indices rather than level names


    Notes
    =====
    - This function answers multiple queries by answering each
      individual query and then unioning the separate dfs into
      a single one.
    - In order to accomplish this, the individual query dfs' remaining
      schema-dimname columns are merged to form a single column named 'level'

      e.g. PL94 Schema
           query = "sex * votingage"

           Before merging the dimensions that remained:

              sex    |   votingage       |       query
            -------------------------------------------------
            "Male"   | "Voting Age"      |  "sex * votingage"
            "Male"   | "Non-voting Age"  |  "sex * votingage"
            "Female" | "Voting Age"      |  "sex * votingage"
            "Female" | "Non-voting Age"  |  "sex * votingage"


            After merging the dimensions that remained:

                     level               |       query
            -------------------------------------------------
            "Male BY Voting Age"         |  "sex * votingage"
            "Male BY Non-voting Age"     |  "sex * votingage"
            "Female BY Voting Age"       |  "sex * votingage"
            "Female BY Non-voting Age"   |  "sex * votingage"

    - This form may not fit all needs, so an alternative way is to use
      a loop and answer each query, one at a time using the answerQuery() function
      with merge_dims=False instead.

    """
    if verbose:
        show(du.aslist(querynames), "Queries to be answered and combined into a single Spark DF")
    for i, query in enumerate(du.aslist(querynames)):
        # need to merge_dims to enable unioning
        merge_dims = True
        if i == 0:
            querydf = answerQuery(sparkdf, schema, query, labels=labels, merge_dims=merge_dims, verbose=verbose)
        else:
            qdf = answerQuery(sparkdf, schema, query, labels=labels, merge_dims=merge_dims, verbose=verbose)
            querydf = querydf.unionByName(qdf)

        querydf = querydf.persist()

    if verbose:
        show(querydf, "The Query DF")
        show(querydf.select(AC.QUERY).distinct(), "With these queries included")

    return querydf


def buildGeolatticeTrunkColumns(spark_df, geolevels):
    """
    For the geographic levels that fall into the central hierarchy of the Census Geolattice (i.e. the Geolattice Trunk),
    there is a "trick" that can be used, involving substrings of the geocodes, to access the geocodes of the geographies
    within the trunk.
    """
    geolattice_trunk_dict = {
        C.US: 0,
        C.STATE: 2,
        C.COUNTY: 5,
        C.TRACT_GROUP: 9,
        C.TRACT: 11,
        C.BLOCK_GROUP: 12,
        C.BLOCK: 16
    }
    geolevels = du.aslist(geolevels)
    assert np.all([x in geolattice_trunk_dict for x in geolevels]), f"Not all geolevels fall into the nested hierarchy; at least one in {geolevels} does not exist within the geolattice dictionary found in the analysis.tools.sdftools.buildGeolatticeTrunkColumns function."
    for geolevel in geolevels:
        if geolevel == C.US:
            spark_df = spark_df.withColumn(C.US, sf.col(C.US)).persist()
        else:
            ind = geolattice_trunk_dict[geolevel]
            spark_df = spark_df.withColumn(geolevel, sf.col(AC.GEOCODE)[0:ind]).persist()
    print_item(spark_df, f"The DF with geolattice trunk columns {geolevels} included")
    return spark_df



def leveldict(schema, query, level2index=True, column=AC.LEVEL):
    """
    Returns a sql statement that can be used to convert the levels' indices to labels / labels to indices

    Parameters
    ==========
    schema : programs.schema.schema.Schema
        The DAS Schema to use to interpret the levels of the query

    query : str
        The query

    level2index : bool (default = True)
        - If True, convert the level labels to indices
        - If False, convert the level indices to labels

    column : str (default = AC.LEVEL)
        - The column to rename using the sql returned by this function
        - The default is the level column


    Notes
    =====
    - Can be used to convert the levels of a 'crossed' query, but it works best with individual queries/dimensions
    """
    table = schema.getTable(query)
    levels = np.array(table[AC.LEVEL])
    indices = [str(x) for x in range(len(levels))]
    if level2index:
        key = levels
        val = indices
    else:
        key = indices
        val = levels
    leveldict = dict(zip(key, val))
    sql = levelsql(leveldict, column)
    return sql


def remove_not_in_area(df, geolevels):
    if 'Place' in geolevels:
        df = df.filter((df.geocode[2:7] != CC.NOT_A_PLACE) | (df.geolevel != "PLACE"))
    if 'AIAN_AREAS' in geolevels:
        df = df.filter((df.geocode != CC.NOT_AN_AIAN_AREA) | (df.geolevel != "AIAN_AREAS"))
    if 'FED_AIRS' in geolevels:
        df = df.filter((df.geocode != CC.NOT_AN_AIAN_AREA) | (df.geolevel != "FED_AIRS"))
    if 'OSE' in geolevels:
        df = df.filter(
            (sf.col(AC.GEOCODE).substr(sf.length(sf.col(AC.GEOCODE)) - 4, sf.length(sf.col(AC.GEOCODE))) != CC.NOT_AN_OSE) | (sf.col(AC.GEOLEVEL) != "OSE"))
    if 'MCD' in geolevels:
        df = df.filter(
            (sf.col(AC.GEOCODE).substr(sf.length(sf.col(AC.GEOCODE)) - 4, sf.length(sf.col(AC.GEOCODE))) != CC.NOT_A_MCD) | (sf.col(AC.GEOLEVEL) != "MCD"))
    if 'AIANTract' in geolevels:
        df = df.filter((df.geocode != CC.NOT_AN_AIAN_TRACT) | (df.geolevel != "AIANTract"))
    if 'AIANState' in geolevels:
        df = df.filter((df.geocode != CC.NOT_AN_AIAN_STATE) | (df.geolevel != "AIANState"))
    if 'AIANBlock' in geolevels:
        df = df.filter((df.geocode != CC.NOT_AN_AIAN_BLOCK) | (df.geolevel != "AIANBlock"))
    if 'COUNTY_NSMCD' in geolevels:
        df = df.filter((df.geocode != CC.STRONG_MCD_COUNTY) | (df.geolevel != "COUNTY_NSMCD"))
    return df


def levelsql(leveldict, column=AC.LEVEL, join="\n", sqlelse="-1"):
    sql_when = ["case"]
    sql_when += [f"when {column} = '{sql_from}' then '{sql_to}'" for sql_from, sql_to in leveldict.items()]
    sql_when += [f"else '{sqlelse}' end"]
    sql_when = join.join(sql_when)
    return sql_when


def getColumnTypes(df):
    fields = df.schema.fields
    fields = [x.jsonValue() for x in fields]
    types = [{'column': x['name'], 'type': x['type']} for x in fields]
    return types


def filterColumnsByType(df, filter_dtype="string"):
    types = getColumnTypes(df)
    keep = [x for x in types if x['type'] == filter_dtype]
    columns = [x['column'] for x in keep]
    return columns


def makeSingleQueryColumn(sparkdf, schema):
    """
    takes the schema-based columns (i.e. the names of the dimensions of the histogram)
    and combines the levels (row-by-row) to create a single AC.LEVEL column

    example
        cenrace | votingage
        1 Race  | Non-Voting Age
        2 Races | Non-Voting Age
        3 Races | Non-Voting Age
        4 Races | Non-Voting Age
        5 Races | Non-Voting Age
        6 Races | Non-Voting Age
        1 Race  | Voting Age
        2 Races | Voting Age
        3 Races | Voting Age
        4 Races | Voting Age
        5 Races | Voting Age
        6 Races | Voting Age

        Becomes

                level
        1 Race x Non-Voting Age
        2 Races x Non-Voting Age
        3 Races x Non-Voting Age
        4 Races x Non-Voting Age
        5 Races x Non-Voting Age
        6 Races x Non-Voting Age
        1 Race x Voting Age
        2 Races x Voting Age
        3 Races x Voting Age
        4 Races x Voting Age
        5 Races x Voting Age
        6 Races x Voting Age

    """
    columns = sparkdf.columns
    cols_to_change = [x for x in schema.dimnames if x in sparkdf.columns]
    if len(cols_to_change) == 0:
        df = sparkdf.withColumn(AC.LEVEL, sf.lit("total"))
    else:
        udf = sf.udf(lambda x: " BY ".join(x))
        cols = [sf.col(x) for x in cols_to_change]
        df = sparkdf.withColumn(AC.LEVEL, udf(sf.array(cols)))
        for col in cols_to_change:
            df = df.drop(col)
    return df



def stripSQLFromColumns(sparkdf, ignore=None):
    """
    removes everything except for the innermost substring flanked by parentheses.
    aims to remove the sum(.) and avg(.), etc. function names added to the columns during those operations.

    e.g. after computing a sum over geocode and an average over runs, we might have:
         "avg(sum(priv))" as one of the column names
         we use this function to strip away everything except for the AC.PRIV that's flanked
         by one or more sets of parentheses.
    """
    df = sparkdf
    sdf_columns = sparkdf.columns
    if ignore is None:
        ignore = []
    for col in sdf_columns:
        if col not in du.aslist(ignore):
            df = df.withColumnRenamed(col, stripParentheses(col)).persist()
    return df


def stripParentheses(s):
    return s.split("(")[-1].split(")")[0]


def findMissingRows(rowdicts, schema, queries):
    #tablebuilder = dd.getTableBuilder(schema)

    all_pairs = set(schema.getTableTuples(queries))
    extant_pairs = set([(x[AC.QUERY], x[AC.LEVEL]) for x in rowdicts])

    missing_pairs = list(all_pairs.difference(extant_pairs))

    return missing_pairs

def getMissingRowCounts(df, schema, workload, groupby=[AC.GEOCODE, AC.GEOLEVEL, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP]):
    assert len(workload)==1, "getNumMissingRows only implemented for a single query at a time."
    missing_rdd = (
        df.rdd
          .map(lambda row: row.asDict())
          .map(lambda row: (tuple([row[x] for x in du.aslist(groupby)]), row))
          .groupByKey()
          .mapValues(list)
          .flatMapValues(lambda rows: findMissingRows(rows, schema, workload))
          .map(lambda key_row: { k: v for (k,v) in list(zip(groupby + [AC.QUERY, AC.LEVEL], key_row[0] + key_row[1])) })
          .map(lambda row: Row(**row))
    )
    if not missing_rdd.isEmpty():
        missing_rows_df = missing_rdd.toDF().withColumn("missing", sf.lit(1))
        missing_rows_pandas_df = missing_rows_df.groupBy(AC.GEOLEVEL).sum().toPandas()
        print(f"missing_rows_pandas_df: {missing_rows_pandas_df.head(50)}")
        return missing_rows_pandas_df
    else:
        print(f"No missing counts in input df/workload. Returning Empty pandas DF.")
        return pandas.DataFrame()

def getFullWorkloadDF(df, schema, workload, groupby=[AC.GEOCODE, AC.GEOLEVEL, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP]):
    """
    Fills in any missing cells/rows given a specific workload.

    Parameters
    ==========
    df : Spark DataFrame

    schema : programs.schema.schema.Schema
        The schema associated with this data

    workload : str or list of str
        One or more 'querynames' or 'tablenames'

    groupby : str or list of str
        The columns used to create the "keys" for the underlying rdd to group on
        Default is [AC.GEOCODE, AC.RUN_ID] - This means it will create a workload 'table' for each
        (geounit, run) pair, which is what we want most often for this operation.

    Returns
    =======
    Spark DataFrame
        A dataframe where all missing rows have been included.

    """
    ### Steps ###
    # 1. change Row objects into dicts
    # 2. create a (key, value) pair for the rdd, where the key is a tuple of the values in the dict associated with the groupby keys
    # 3. group together rows by key (i.e. grab all cells within a geounit in a run)
    # 4. take the grouped together rows and turn them into a list
    # 5. find the missing values and flat map the values (flatMapValues() will keep the key for each value!)
    # 6. create the "missing" dict by creating a dictionary combining the key and the missing query info
    # 7. change all of the "missing" dicts into Row objects
    # 8. turn the rdd into a dataframe (if the rdd is not empty)
    # 9. create new columns for the missing_df (was_missing = "1", orig = 0, priv = 0)
    # 10. union the missing_df with the original (i.e. nonzero df)
    missing_rdd = (
        # steps 1 through 7
        df.rdd
          .map(lambda row: row.asDict())
          .map(lambda row: (tuple([row[x] for x in du.aslist(groupby)]), row))
          .groupByKey()
          .mapValues(list)
          .flatMapValues(lambda rows: findMissingRows(rows, schema, workload))
          .map(lambda key_row: { k: v for (k,v) in list(zip(groupby + [AC.QUERY, AC.LEVEL], key_row[0] + key_row[1])) })
          .map(lambda row: Row(**row))
    )

    df = df.withColumn(AC.WAS_MISSING, sf.lit("0"))

    if not missing_rdd.isEmpty():
        # steps 8 through 10
        missing_df = missing_rdd.toDF().persist()
        missing_df = missing_df.withColumn(AC.WAS_MISSING, sf.lit("1"))
        missing_df = missing_df.withColumn(AC.ORIG, sf.lit(0))
        missing_df = missing_df.withColumn(AC.PRIV, sf.lit(0))
        print_item(missing_df.count(), "Total number of missing rows across all data")
        df = df.select(missing_df.columns).unionByName(missing_df).persist()
    else:
        print_item(0, "Found zero missing rows across all data")

    print_item(df.count(), "Total number of rows in the data + missing df")

    return df


def print_item(item, label, show=20):
    if isinstance(item, (str, list, dict, int, float, tuple)):
        print(item)
    else: # assume spark df
        try:
            item.show(show)
        except AttributeError:
            print(item)
    print(f"^^^^^ {label} ^^^^^\n")


def show(item, label="", show=20):
    """ alias for print_item """
    print_item(item, label, show)


def get_columns(df, ignore=None):
    cols = df.columns
    if ignore is None:
        ignore = []
    cols = [x for x in cols if x not in ignore]
    return cols


# AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
def getTableCV(df, run_id, groupby=[AC.GEOCODE, AC.GEOLEVEL, AC.LEVEL, AC.QUERY], column=AC.PRIV, colname="CV"):
    # if fill=0.0 in class TableCV, then we can assume all of the rows and runs exists
    # as such, we can use the built-in SparkSQL 'var_samp' agg function
    # rather than calculating the sample_variance by hand

    # assuming groupby is 'geolevel' and 'query', this calculates the sample variance
    # across all runs for each (geounit, querylevel) pair in the dataframe
    # sample_var = df.groupBy(groupby).agg(sf.var_samp(sf.col(column))).persist()
    # sample_var.show(1000)
    sample_sd = df.groupBy(groupby).agg(sf.stddev_samp(sf.col(column))).persist()
    print_item(sample_sd, "Sample Std Dev")

    # grab the first run's priv count and perform max(1,first_run['priv'])
    max_udf = sf.udf(lambda count: max(1,count))
    max_udf_column = f"max(1,{column})"
    first_run = df.filter(sf.col(AC.RUN_ID) == run_id).persist()
    first_run = first_run.withColumn(max_udf_column, max_udf(sf.col(column))).persist()

    # create join_key to join sample_var with first_run and drop non-important
    # first_run columns (i.e. select only the ones we want to keep)
    sample_sd = sample_sd.withColumn("join_key",
        sf.concat(
            sf.col(AC.GEOLEVEL), sf.lit("."),
            sf.col(AC.GEOCODE),  sf.lit("."),
            sf.col(AC.LEVEL),    sf.lit("."),
            sf.col(AC.QUERY)
        )
    ).persist()
    first_run = first_run.withColumn("join_key",
        sf.concat(
            sf.col(AC.GEOLEVEL), sf.lit("."),
            sf.col(AC.GEOCODE),  sf.lit("."),
            sf.col(AC.LEVEL),    sf.lit("."),
            sf.col(AC.QUERY)
        )
    ).persist()

    ###first_run = drop(first_run, [x for x in first_run.columns if x not in ['join_key', 'max(1,priv)']]).persist()
    first_run = first_run.select(['join_key', max_udf_column])

    # join sample_var and first_run
    sample_sd = sample_sd.join(first_run, 'join_key').select("*").persist()
    sample_sd = sample_sd.drop("join_key").persist()

    # calcualate CV = var_samp(priv) / max(1,priv)
    cv = sample_sd.withColumn(colname, sf.col(f"stddev_samp({column})") / sf.col(max_udf_column)).persist()

    # select only the important columns
    group = [AC.GEOLEVEL, AC.GEOCODE, AC.LEVEL, AC.QUERY, colname]
    cv = cv.sort(group).persist()
    return cv


def setQuantiles(quantiles=None):
    """ sets default quantiles if no argument; otherwise, returns the quantiles """
    if quantiles is None:
        quantiles = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    return quantiles


def quantiles2percentiles(quantiles):
    """ converts quantiles into percentiles """
    return [100*x for x in quantiles]


def getSum(df, groupby=[AC.RUN_ID, AC.GEOLEVEL, AC.GEOCODE]):
    return df.select("*").groupBy(groupby).sum().persist()

def getAverage(df, column, groupby):
    #avg_dict = { col: 'avg' for col in columns }
    #return df.select("*").groupBy(groupby).agg(avg_dict).persist()
    return df.select("*").groupBy(groupby).agg(sf.avg(column)).persist()


def getGroupQuantiles(df, columns, groupby, quantiles):
    """
    Takes the rows in the df (grouped by the columns listed in the groupby parameter) and calculates the
    quantiles for each of the columns listed in the columns parameter.

    Parameters
    ==========
    df : Spark DataFrame

    columns : str or list of str
        We will calculate the quantiles across each of the columns listed

    groupby : str or list of str
        We will create groups of rows based on the distinct levels in each of the columns in this list

    quantiles : float or list of floats
        - The quantiles to calculate
        - The list should be in ascending order
        - Each q_i should be: 0 <= q_i <= 1


    Returns
    =======
    a Spark DF with quantile information for each of the desired columns based on the groups provided


    Notes
    =====
    - This is currently the preferred method for producing quantiles
      within arbitrary groups of the data

    - To see an example of this function, run the following script (and look at the generated log/output file):
          das_decennial/analysis/vignettes/e0e_group_quantiles.py
    """
    columns = du.aslist(columns)
    groupby = du.aslist(groupby)
    quantiles = setQuantiles(quantiles)
    rdd = getRowGroupsAsRDD(df, groupby).persist()
    rdd = rdd.flatMapValues(lambda rows: getRowGroupQuantiles(rows, columns, quantiles)).persist()
    rdd = rdd.map(lambda key_row: { k: v for (k,v) in list(zip(groupby, key_row[0])) + list(key_row[1].items()) })
    rdd = rdd.map(lambda row: Row(**row))
    df = rdd.toDF().persist()
    return df


def getRowGroupQuantiles(rows, columns, quantiles):
    """
    Calculates quantiles for a group of rows

    Parameters
    ==========
    rows : a list of dictionaries (i.e. Spark Row objects converted to dicts via row.asDict())
        This is a list of rows associated with a group

    columns : str or list of str
        We will calculate the quantiles for each of these columns

    quantiles : float or list of float
        - The quantiles to calculate (e.g. the median [0.5], quartiles [0.0, 0.25, 0.5, 0.75, 1.0], etc.)
        - Should be in ascending order and each float should exist in [0,1]

    Returns
    =======
    a list of dictionaries (that most often will be turned into Spark Row objects)

    Notes
    =====
    - Uses pandas and numpy to help calculate and organize the quantiles
    - This is a Spark mapper function
        - It is often used with RDDs and the .flatMapValues function to return the list of rows in a way that
          makes it easier to build the Spark DF
    """
    pdf = pandas.DataFrame(rows)
    cols = {}
    for col in du.aslist(columns):
        cols[col] = np.quantile(pdf[col], quantiles)
    cols['quantile'] = [str(x) for x in quantiles]
    records = pandas.DataFrame(cols).to_dict('records')
    return records


def getRowGroupsAsRDD(df, groupby):
    """
    Groups rows

    Parameters
    ==========
    df : Spark DF

    groupby : str or list of str
        These construct the unique tuple keys that the Spark RDD will group on

    Returns
    =======
    an RDD where each element is a list of dictionaries associated with a particular group
    """
    rdd = (
        df.rdd
          .map(lambda row: row.asDict())
          .map(lambda row: (tuple([row[x] for x in du.aslist(groupby)]), row))
          .groupByKey()
          .mapValues(list)
    )
    return rdd


def getQuantiles(df, column, quantiles=None, relative_error=0.01):
    """
    Uses the built-in approxQuantile function to calculate quantiles across a
    column in the dataframe.

    Parameters
    ==========
    df : Spark DataFrame

    column : str
        The column on which to calculate the quantiles

    quantiles : float or list of floats
        Defaults to the value set in setQuantiles
        Each float must exist in [0,1]

    relative_error: float
        Needed by approxQuantile
        default is 0.01

    Returns
    =======
    list
        A list of quantiles calculated across the entire column in the dataframe

    Notes
    =====
    - Returns a list, not a dataframe
    - Doesn't work with a groupBy call; only the entire column, so use filter calls
      manually to calcualte across different groups (i.e. levels of each column to group by)
    """
    quantiles = setQuantiles(quantiles)
    return df.approxQuantile(column, quantiles, relative_error)

# AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
def getL1(df, colname="L1", col1=AC.PRIV, col2=AC.ORIG):
    return df.withColumn(colname, sf.abs(sf.col(col1) - sf.col(col2))).persist()


def getL2(df, colname="L2", col1=AC.PRIV, col2=AC.ORIG):
    return df.withColumn(colname, sf.pow(sf.col(col1) - sf.col(col2), sf.lit(2))).persist()


def mapL1Relative(grouped_values):
    # format on input is:
    # ((priv_query_val1, orig_query_val1, priv_sum, orig_sum), (priv_query_val2, orig_query_val2, priv_sum, orig_sum), ...)
    priv_vals = []
    orig_vals = []
    for row in grouped_values:
        priv_vals.append(row[0])
        orig_vals.append(row[1])
        priv_sum = row[2]
        orig_sum = row[3]

    max_col_ind = np.argmax(priv_vals)
    result = [int(priv_sum), int(orig_sum)]

    # Note that the metric requires both of the sums above to be non-zero. When all elements of priv_vals are correctly
    # priv_vals are correctly zero, define L1Relative to be zero. Also, when either priv_sum or orig_sum is zero and the
    # other is not, return 2 so that these geounits can be filtered out easily before taking averages and quantiles.
    # (Note that this is the only case in which this function returns a value greater than one.)
    if orig_sum == 0. and priv_sum == 0.:
        result.append(0.)
    elif orig_sum > 0. and priv_sum > 0.:
        result.append(float(np.abs(priv_vals[max_col_ind] / priv_sum - orig_vals[max_col_ind] / orig_sum)))
    else:
        result.append(2.)
    return result

# AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
def getL1Relative(df, colname="L1Relative", col1=AC.PRIV, col2=AC.ORIG, denom_query="total", denom_level="total"):
    spark = SparkSession.builder.getOrCreate()
    groupby = [AC.GEOLEVEL, AC.GEOCODE]

    rdd_queries = (df.rdd.filter(lambda row: row[AC.QUERY] != denom_query)
                        .map(lambda row: (tuple(row[x] for x in groupby), (row[col1], row[col2], row[AC.QUERY]))))
    # Format: (tuple_key, ((priv_query_val, orig_query_val, query_name))

    n_numerators = rdd_queries.count()
    assert n_numerators > 0, "There are no queries in the dataframe to define the numerator of L1 relative error."

    rdd_denoms = (df.rdd.filter(lambda row: row[AC.QUERY] == denom_query and row[AC.LEVEL] == denom_level)
                       .map(lambda row: (tuple(row[x] for x in groupby), (row[col1], row[col2]))))
    # Format: (tuple_key, (priv_sum, orig_sum))

    n_denoms = rdd_denoms.count()
    assert n_denoms > 0, f"The query {denom_query} with level {denom_level} is not available in the dataframe."

    rdd = rdd_queries.join(rdd_denoms)
    # Format: (tuple_key, ((priv_query_val, orig_query_val, query_name), (priv_sum, orig_sum)))

    n_full_frac = rdd.count()
    msg = f"Before join, there were {n_numerators} numerator rows, but after join there is {n_full_frac} rows"
    assert (n_full_frac == n_numerators), msg

    rdd = rdd.map(lambda row: (row[0] + (row[1][0][2],), row[1][0][:-1] + row[1][1]))
    # Format: (tuple_key_with_query_name, (priv_query_val, orig_query_val, priv_sum, orig_sum))

    grouped_rdd = rdd.groupByKey()
    # Format: (tuple_key_with_query_name, ((priv_query_val1, orig_query_val1, priv_sum, orig_sum), ...))

    grouped_rdd = grouped_rdd.mapValues(mapL1Relative)

    # After the next line, the column format will be (*(groupby + [AC.QUERY]), col1, col2, colname):
    grouped_rdd = grouped_rdd.map(lambda row: (*row[0], *row[1]))

    schema_list = [StructField(gb, StringType(), True) for gb in (groupby + [AC.QUERY])]
    schema_list.extend([StructField(col1, IntegerType(), True), StructField(col2, IntegerType(), True), StructField(colname, DoubleType(), True)])
    df_out = spark.createDataFrame(grouped_rdd, StructType(schema_list))
    return df_out

# AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
def getSignedError(df, colname="signed_error", col1=AC.PRIV, col2=AC.ORIG):
    return df.withColumn(colname, sf.col(col1) - sf.col(col2)).persist()


def getNonzero(df, groupby=[AC.RUN_ID, AC.GEOCODE, AC.GEOLEVEL], cols=[AC.PRIV, AC.ORIG]):
    for col in cols:
        df = df.withColumn(f"{col}_nonzero", sf.when(sf.col(col) > 0, 1).otherwise(0)).persist()
    group_nonzero = [f"{col}_nonzero" for col in cols]
    df = df.select(group_nonzero + groupby).groupBy(groupby).sum().persist()
    df = stripSQLFromColumns(df).persist()
    return df.persist()


def getCellDensity(df, schema, groupby=[AC.RUN_ID, AC.GEOCODE, AC.GEOLEVEL], cols=[AC.PRIV, AC.ORIG]):
    df = getNonzero(df, groupby=groupby, cols=cols)
    for col in cols:
        celldensity_col = f"{col}_celldensity"
        nonzero_col = f"{col}_nonzero"
        df = df.withColumn(celldensity_col, sf.col(nonzero_col) / sf.lit(schema.size)).persist()
    return df.persist()


def getCellSparsity(df, schema, groupby=[AC.RUN_ID, AC.GEOCODE, AC.GEOLEVEL], cols=[AC.PRIV, AC.ORIG]):
    df = getNonzero(df, groupby=groupby, cols=cols)
    for col in cols:
        cellsparsity_col = f"{col}_cellsparsity"
        nonzero_col = f"{col}_nonzero"
        df = df.withColumn(cellsparsity_col, sf.lit(1) - (sf.col(nonzero_col) / sf.lit(schema.size))).persist()
    return df.persist()


def getGeounitTVD(df, colname="1-TVD", col1=AC.PRIV, col2=AC.ORIG, groupby=[AC.GEOCODE, AC.GEOLEVEL, AC.RUN_ID, AC.BUDGET_GROUP, AC.PLB]):
    """
    computes the 1-TVD statistic for each cell in the query and the 1-TVD statistics up across levels, grouped by everything else
    """
    # L1 error for each cell
    df = getL1(df, colname="L1", col1=col1, col2=col2)
    df = stripSQLFromColumns(df)

    # 1-TVD for each geounit
    df = df.withColumn(colname, 1 - (sf.col("L1") / (2 * sf.col(AC.ORIG)))).persist()
    df = df.groupBy(groupby).sum().persist()
    return df.persist()


def getGeolevelTVD(df, colname="1-TVD", col1=AC.PRIV, col2=AC.ORIG, groupby=[AC.GEOLEVEL, AC.RUN_ID]):
    # L1 error for each cell
    df = getL1(df, colname="L1", col1=col1, col2=col2)

    # Sum L1 cell error over cells and over all geounits
    df = df.groupBy(groupby).sum().persist()
    df = stripSQLFromColumns(df)

    # 1-TVD for the geolevel (a single value)
    df = df.withColumn(colname, 1 - (sf.col("L1") / (2 * sf.col(AC.ORIG)))).persist()
    return df.persist()


def getCountBins(df, column=AC.ORIG, bins=[0,1,10,100,1000,10000]):
    udf = makeCountBinsFunction(bins, udf=True)
    df = df.withColumn(f"{column}_count_bin", udf(sf.col(column)))
    return df


def makeCountBinsFunction(bins, udf=False):
    def countbins(val):
        mybin = None
        for i,b in enumerate(bins[1:]):
            #print(f"{val} < {b} | {val < b}")
            if val < b:
                mybin = f"[{bins[i]}-{bins[i+1]})"
                break
        if mybin is None:
            mybin = f"{bins[-1]} +"
        return mybin
    if udf:
        fxn = sf.udf(lambda val: countbins(val))
    else:
        fxn = countbins
    return fxn


def bin_column(df, column, bins):
    """
    Creates a new column in the Spark DF that bins the values in the column parameter by the bins provided.

    Parameters
    ==========
    df : Spark DF

    column : str
        Name of one of the (numeric) columns in df

    bins : list of int / float
        e.g. bins = [0, 1, 5, 10]; the bins created will be:
             [0-1), [1-5), [5-10), 10+

    Returns
    =======
    a Spark DF with the new column

    Notes
    =====
    - The new column will be named f"{column}_binned", where column is the name of the column provided
      e.g. if column = "example", then the new column will be called "example_binned"
    - Oftentimes we want to bin by the CEF counts, so we would use column=AC.ORIG
    """
    sql = sqlbins(column, bins)
    df = df.withColumn(f"{column}_binned", sf.expr(sql)).persist()
    return df


def sqlbins(column, bins, join="\n"):
    """
    Returns a SQL string expression for binning values

    Parameters
    ==========
    column : str
        The column to bin

    bins : list of int or float
        - Need at least 2 values to create a bin
        - e.g. bins = [0, 1, 5, 10]; the bins created will be:
          [0-1), [1-5), [5-10), 10+

    Returns
    =======
    a SQL string expression

    e.g. for column = "A" and bins = [0, 1, 5], the returned SQL expression will be:
         "case when A < 0 then '(-Inf, 0)' when A >= 0 and A < 1 then '[0, 1)' when A >= 1 and A < 5 then '[1, 5)' else '[5, +Inf)'"
    """
    sql = ["case"]
    sql += [f"when {column} < {bins[0]} then '(-Inf, {bins[0]})'"]
    for i,b in enumerate(bins[1:]):
        sql += [f"when {column} >= {bins[i]} and {column} < {b} then '[{bins[i]}, {bins[i+1]})'"]
    sql += [f"when {column} >= {bins[-1]} then '[{bins[-1]}, +Inf)'"]
    sql += [f"else 'NaN' end"]
    sql = join.join(sql)
    return sql


def getSampleVariance(df, column, n, groupby=None):
    """
    Calculates the Sample Variance of a column using the provided sample size, n.

    Parameters
    ==========
    df : Spark DataFrame

    column : str
        The column on which to calculate the sample variance

    n : int
        The sample size to use

    groupby : list
        A list of column names on which to group by


    Returns
    =======
    Spark DataFrame
        A data frame with an additional column for the sample variance of the column requested


    Notes
    =====
    - Use groupby = ['query', geolevel] to calculate the sample variance of each
      query x geounit across all trials/runs
    """
    # 1. Create a column for x^2
    df = df.withColumn(f"{column}^2", sf.pow(sf.col(f"{column}"), sf.lit(2))).persist()

    # 2. Sum across "x" and "x^2"
    if groupby is None:
        df = df.sum().persist()
    else:
        df = df.groupBy(groupby).sum().persist()

    # 3. Create a column: Multiply "sum(x^2)" by n="the number of trials"
    df = df.withColumn(f"n*sum({column}^2)", sf.lit(n) * sf.col(f"sum({column}^2)")).persist()

    # 4. Create a column: Square "sum(x)"
    df = df.withColumn(f"sum({column})^2", sf.pow(sf.col(f"sum({column})"), sf.lit(2))).persist()

    # 5. Create a column: "n*sum(x^2)" - "sum(x)^2"
    df = df.withColumn(f"n*sum({column}^2)-sum({column})^2", sf.col(f"n*sum({column}^2)") - sf.col(f"sum({column})^2")).persist()

    # 6. Create a column: Divide the numerator by [n * (n-1)]
    denom = n * (n - 1)
    df = df.withColumn(f"sample_variance({column})", sf.col(f"n*sum({column}^2)-sum({column})^2") / sf.lit(denom)).persist()

    return df


def getAvgSignedErrorByTrueCountRuns(df, bins=[0,1,10,100,1000,10000],
                groupByCols=[AC.GEOCODE, AC.GEOLEVEL, AC.PLB, AC.BUDGET_GROUP, AC.QUERY, AC.LEVEL, 'orig_count_bin']):
    """
    calculates the signed error and relative error (given a column name of 're') , grouped by buckets of the orig column,
    averaged over the number of observations in a (geocode, geolevel, run_id, plb, budget_group, query, orig_count_bin) group.

    returns a spark dataframe with the following columns
    geocode, geolevel, run_id, plb, budget_group, query, level, orig_count_bin, signed_error, re
    """
    show("getSignedErrorByTrueCountRuns", "Function start")
    df = getCountBins(df, column=AC.ORIG, bins=bins).persist()
    print_item(df, "after assigning bins")

    # AC.PRIV means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
    df = getSignedError(df, colname="signed_error", col1=AC.PRIV, col2=AC.ORIG).persist()
    print_item(df, "after getting signed error")

    orig_denom_udf = sf.udf(lambda x: 1 if x == 0 else x)

    df = df.withColumn('orig_denom', orig_denom_udf(sf.col(AC.ORIG))).persist()

    df = df.withColumn("re", sf.col("signed_error") / orig_denom_udf(sf.col(AC.ORIG))).persist()
    df.show()

    df = df.groupBy(groupByCols).avg()
    df = stripSQLFromColumns(df).persist()
    print_item(df, "after averaging the signed error across the individual levels in each (query, geocode, geolevel, trial, plb, budget group, and original count bin)")

    df = df.select(groupByCols+['signed_error', 're']).persist()
    df.show(1000)
    show("getSignedErrorByTrueCountRuns", "Function end")
    return df

def getAvgAbsErrorByTrueCountRuns(df, bins=[0,1,10,100,1000,10000],
                groupByCols=[AC.GEOCODE, AC.GEOLEVEL, AC.PLB, AC.BUDGET_GROUP, AC.QUERY, AC.LEVEL, 'orig_count_bin']):
    """
    calculates the abs error and relative error (given a column name of 're') , grouped by buckets of the orig column,
    averaged over the number of observations in a (geocode, geolevel, run_id, plb, budget_group, query, orig_count_bin) group.

    returns a spark dataframe with the following columns
    geocode, geolevel, run_id, plb, budget_group, query, level, orig_count_bin, abs_error, re
    """
    show("getSignedErrorByTrueCountRuns", "Function start")
    df = getCountBins(df, column=AC.ORIG, bins=bins).persist()
    print_item(df, "after assigning bins")

    df = getL1(df, colname="abs_error", col1=AC.PRIV, col2=AC.ORIG).persist()
    print_item(df, "after getting abs error")

    orig_denom_udf = sf.udf(lambda x: 1 if x == 0 else x)

    df = df.withColumn('orig_denom', orig_denom_udf(sf.col(AC.ORIG))).persist()

    df = df.withColumn("re", sf.col("abs_error") / orig_denom_udf(sf.col(AC.ORIG))).persist()
    df.show()

    df = df.groupBy(groupByCols).avg()
    df = stripSQLFromColumns(df).persist()
    print_item(df, "after averaging the abs error across the individual levels in each (query, geocode, geolevel, trial, plb, budget group, and original count bin)")

    df = df.select(groupByCols+['abs_error', 're']).persist()
    df.show(1000)
    show("getAbsErrorByTrueCountRuns", "Function end")
    return df

###################################################################
# Functions related to the [category] * Age quantile calculations
###################################################################
def categoryByAgeQuantiles(df, schema, query, labels=False):
    """
    answers a "q * age" query and calculates the age quantiles per category

    categories = each level in 'q'

    e.g. "sex * age"
    categories = { Male, Female }

    this function would perform the following for the "sex * age" query
    1. Answer the query "sex * age"
    2. For each category in { Male, Female }
       a. Filter to only keep the rows that match the category (a.k.a. 'category-filter')
       b. Calculate the number of (geounit, trial) observations in the category (i.e. that survived 'category-filtering')
       c. Filter to keep the (geounit, trial) observations that have (DAS_total > 0) and (CEF_total > 0) (a.k.a. 'zero-filter')
       d. Calculate the nubmer of (geounit, trial) observations that survived 'zero-filtering'
       e. Calculate the quantiles across the age counts for each observation, and then the average across observations
          within a (quantile, geolevel, plb).
       f. Calculate the (num of obs that survived zero-filtering / num of obs that survived category-filtering)

    """
    qsplit = query.split(" * ")
    assert "age" in qsplit, "'age' must be one of the queries"
    assert len(qsplit) >= 2, "Need at least 1 query other than age to generate categories"
    df = answerQuery(df, schema, query, labels=labels)
    df.show()
    ageless_query = " * ".join([x for x in query.split(" * ") if x != "age"])
    categories = cartprodFromQuery(schema, ageless_query, as_records=True, labels=labels)

    geolevels = getUniqueColumnValues(df, AC.GEOLEVEL)

    catdict = {}
    for category in categories:
        print(category)
        cat_sql = " AND ".join([f"{column} = '{level}'" for column, level in category.items()])
        print(cat_sql)

        catdf = df.filter(cat_sql).persist()
        catobs = getNumObs(catdf)
        catobs = catobs.withColumnRenamed("count", "num_obs_cat_filter")
        print_item(catdf, f"Filtered by {cat_sql}")
        print_item(catobs, "Number of observations that survived 'category-filtering'")

        catdf = getValidGeounitTrials(catdf)
        valid_catobs = getNumObs(catdf)
        valid_catobs = valid_catobs.withColumnRenamed("count", "num_obs_zero_filter")
        print_item(catdf, f"Filtered by category totals (DAS > 0) and (CEF > 0)")
        print_item(valid_catobs, "Number of observations that survived 'zero-filtering'")

        if catdf.rdd.isEmpty():
            print(f"""
            Geounits with CEF > 0 and DAS > 0 in the Category DataFrame for category '{cat_sql}' are all empty,
            so the age quantiles for category '{cat_sql}' in the '{query}' query cannot be calculated.
            Returning an empty DataFrame
            """)
        else:
            catdf = getAgeQuantilesAvgL1(catdf, schema, labels)

        obs = catobs.join(valid_catobs, on=[AC.GEOLEVEL, AC.PLB]).persist()
        obs = obs.withColumn("survival_prop", sf.col("num_obs_zero_filter") / sf.col("num_obs_cat_filter")).persist()
        print_item(obs, "Survival Proportions of (geounit, trial) observations in (geolevel, plb)s")
        key = "_".join(".".join(list(category.values())).split(" "))
        # for those categories/levels with a slash in it (e.g. "Nursing Facilities / Skilled Nursing Facilities"), remove the
        # slash to avoid issues with linux interpreting it as a directory, replacing it with _or_ instead
        # => "Nursing_Facilities_or_Skilled_Nursing_Facilities"
        key = "_or_".join(key.split("/"))
        fsquery = ".".join(query.split(" * "))
        category_key = key
        key = f"{fsquery}--{key}"
        quantile_key = f"{key}--quantile_df"
        survival_key = f"{key}--survival_props"
        catdf = catdf.withColumn("category", sf.lit(category_key)).persist()
        catdf = catdf.withColumn("fsquery", sf.lit(fsquery)).persist()
        print_item(catdf, "Category Age Quantile DF")
        catdict[quantile_key] = catdf
        obs = obs.withColumn("category", sf.lit(category_key)).persist()
        obs = obs.withColumn("fsquery", sf.lit(fsquery)).persist()
        print_item(obs, "Survival Proportions DF")
        catdict[survival_key] = obs

    return catdict


def getNumObs(catdf):
    """ counts the number of observations (geounit, trial) pairs for each (geolevel, plb) """
    return catdf.groupBy([AC.GEOLEVEL, AC.GEOCODE, AC.PLB, AC.RUN_ID]).sum().groupBy([AC.GEOLEVEL, AC.PLB]).count()

def getUniqueColumnValues(df, column):
    return df.select(column).distinct().toPandas().values.flatten().tolist()

def getGeolevelCounts(df, geolevels=None):
    if geolevels is None:
        geolevels = getUniqueColumnValues(df, AC.GEOLEVEL)
    counts = {}
    for geolevel in geolevels:
        counts[geolevel] = df.filter(f"{AC.GEOLEVEL} = '{geolevel}'").select(AC.GEOCODE).distinct().count()
    return counts


def getValidGeounitTrials(catdf):
    catdf = catdf.orderBy([AC.GEOCODE, AC.RUN_ID, AC.PLB], ascending=False).persist()
    print_item(catdf, "Original category dataframe")
    print_item(catdf.count(), "Number of rows in the category df")

    totals = catdf.groupBy([AC.GEOCODE, AC.RUN_ID, AC.PLB, AC.QUERY, AC.GEOLEVEL]).sum().persist()
    totals = stripSQLFromColumns(totals).persist()
    totals = totals.orderBy([AC.GEOCODE, AC.RUN_ID, AC.PLB], ascending=False).persist()
    print_item(totals, "Totals for the category for every (geocode, trial, plb, query, and geolevel")
    print_item(totals.count(), "Number of rows in the totals df")

    # filter any blocks where the totals of the category are zero for either the CEF or the DAS
    filtered_totals = totals.filter((sf.col(AC.PRIV) > 0) & (sf.col(AC.ORIG) > 0)).persist()
    filtered_totals = filtered_totals.orderBy([AC.GEOCODE, AC.RUN_ID, AC.PLB], ascending=False).persist()
    print_item(filtered_totals, "Totals surviving the zero-filtering process")
    print_item(filtered_totals.count(), "Number of rows in the filtered totals df")

    # join keeps only those records that match
    groupby = [AC.GEOCODE, AC.RUN_ID, AC.PLB, AC.GEOLEVEL]
    valid = catdf.join(filtered_totals.select(groupby), on=groupby).persist()
    valid = valid.sort([AC.GEOCODE, AC.RUN_ID, AC.PLB]).persist()
    print_item(valid, "Joining the geocodes that survived with the original category dataframe")
    print_item(valid.count(), "Number of rows in the surviving df")
    return valid


def getAgeQuantilesAvgL1(valid_catdf, schema, labels):
    groupby = [AC.GEOCODE, AC.GEOLEVEL, AC.RUN_ID, AC.PLB, AC.QUERY]
    quantiles = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    df = getQuantilesOverPersonAges(df=valid_catdf, schema=schema, groupby=groupby, quantiles=quantiles, labels=labels).persist()
    df = df.select([AC.GEOLEVEL, AC.GEOCODE, AC.RUN_ID, AC.PLB, AC.QUERY, 'percentile', AC.ORIG, AC.PRIV]).persist()
    df = df.withColumn("quantile_L1", sf.abs(sf.col(AC.PRIV) - sf.col(AC.ORIG))).persist()

    block_quantile_avgs = df.groupBy([AC.GEOLEVEL, AC.PLB, AC.QUERY, AC.RUN_ID, 'percentile']).avg().persist()
    block_quantile_avgs = block_quantile_avgs.withColumn('percentile', sf.col('percentile').cast("float")).persist()
    block_quantile_avgs = block_quantile_avgs.sort('percentile').persist()
    return block_quantile_avgs


def cartprodFromQuery(schema, query, as_records=False, labels=False):
    seed = schema.getQuerySeed(query)
    dims, levels = [list(x) for x in zip(*seed.levels.items())]
    if not labels:
        levels = [list(range(len(x))) for x in levels]
    index = pandas.MultiIndex.from_product(levels, names = dims)
    df = pandas.DataFrame(index = index).reset_index()
    for col in df.columns:
        df[col] = df[col].astype('str')
    if as_records:
        df = df.to_dict('records')
    return df


def getQuantilesOverPersonAges(df, schema, groupby, quantiles, labels):
    groupby = du.aslist(groupby)
    quantiles = setQuantiles(quantiles)
    percentiles = quantiles2percentiles(quantiles)
    rdd = getRowGroupsAsRDD(df, groupby).persist()
    rdd = rdd.flatMapValues(lambda rows: getRowGroupQuantilesOverPersonAges(rows, schema, quantiles, labels)).persist()
    rdd = rdd.map(lambda key_row: { k: v for (k,v) in list(zip(groupby, key_row[0])) + list(key_row[1].items()) })
    rdd = rdd.map(lambda row: Row(**row))
    df = rdd.toDF().persist()
    return df

def getRowGroupQuantilesOverPersonAges(rows, schema, quantiles, labels):
    """
    # 'priv' means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
    creates microdata from the row counts and calcualtes the quantiles across the ages in the 'orig' microdata and the 'priv' microdata
    ex)
        rows = [
           { 'age':   '1', 'orig': '2', 'priv': '3' },
           { 'age':  '25', 'orig': '3', 'priv': '1' },
           { 'age': '115', 'orig': '1', 'priv': '3' }
        ]

        orig_microdata = [
            { 'age':   '1' },
            { 'age':   '1' },
            { 'age':  '25' },
            { 'age':  '25' },
            { 'age':  '25' },
            { 'age': '115' }
        ]

        priv_microdata = [
            { 'age':   '1' },
            { 'age':   '1' },
            { 'age':   '1' },
            { 'age':  '25' },
            { 'age': '115' },
            { 'age': '115' },
            { 'age': '115' }
        ]

        then, take these records and compute the quantiles

        then form new rows such that we have columns:

        quantile | orig | priv | ...,

        where ... are the other columns indicating things like plb, geocode, etc.
        and orig/priv indicate the quantiles for age (e.g. median age)

    Notes:
        - Only works when labels=False (at the moment) since the conversion from str to int requires the str to
          be able to be cast as an int, such as the index-notation vs label-notation for the levels of the age variable

    """
    orig_microdata = []
    priv_microdata = []

    for row in rows:
        orig_microdata += [row['age']]*row[AC.ORIG]
        priv_microdata += [row['age']]*row[AC.PRIV]

    if labels:
        age_levels = schema.getQueryLevel('age').flatten().tolist()
        age_leveldict = { k: str(v) for k,v in zip(age_levels, range(len(age_levels))) }
        orig_microdata = [age_leveldict[x] for x in orig_microdata]
        priv_microdata = [age_leveldict[x] for x in priv_microdata]

    # need to cast them as int since they start out as str (since they represent levels in a factor variable)
    orig_microdata = np.array(orig_microdata).astype('int')
    priv_microdata = np.array(priv_microdata).astype('int')

    orig_quantiles = np.quantile(orig_microdata, quantiles)
    priv_quantiles = np.quantile(priv_microdata, quantiles)

    cols = {}
    cols[AC.ORIG] = orig_quantiles
    cols[AC.PRIV] = priv_quantiles
    cols['quantile'] = [str(x) for x in quantiles]

    records = pandas.DataFrame(cols).to_dict('records')
    return records



########################################################
# Geolevel Sparsity
#
# 1. Count the number of nonzero rows in a group
#    for both AC.ORIG and AC.PRIV
# 2. Include the total number of zero and nonzero
#    cells that are in that group
# 3. Sum across all geounits, computing the total
#    number of [AC.ORIG, AC.PRIV] nonzeros and the
#    total number of zeros + nonzeros in a geolevel
# 4. Compute the proportion of [AC.ORIG, AC.PRIV]
#    zeros: 1-(nonzeros/total)
# 5. And, compute the geolevel_sparsity L1 error
#    between AC.PRIV and AC.ORIG
########################################################
def getCellSparsityByGroup(df, schema, groupby=[AC.GEOCODE, AC.GEOLEVEL, AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP, AC.QUERY]):
    count_nonzeros = lambda condition: sf.sum(sf.when(condition, 1).otherwise(0))
    df = df.groupBy(groupby).agg(
        count_nonzeros(sf.col(AC.ORIG) > 0).alias(f'{AC.ORIG}_num_nonzero_cells'),
        count_nonzeros(sf.col(AC.PRIV) > 0).alias(f'{AC.PRIV}_num_nonzero_cells')
    ).persist()
    queries = getUniqueColumnValues(df, AC.QUERY)
    sizedict = { query: schema.getQueryShape(query, size=True) for query in queries }
    size_sql = grouping2sql(AC.QUERY, sizedict)
    df = df.withColumn('num_cells', sf.expr(size_sql)).persist()
    df = df.withColumn('num_cells', sf.col('num_cells').cast('long')).persist()
    return df

def getCellSparsityAcrossGeolevelSuperHistogram(df, groupby=[AC.GEOLEVEL, AC.PLB, AC.BUDGET_GROUP, AC.QUERY, AC.RUN_ID]):
    """
    requires 'getCellSparsityByGroup' to have been called first
    """
    df = df.groupBy(groupby).sum()
    df = stripSQLFromColumns(df)
    df = df.withColumn(f"{AC.ORIG}_geolevel_sparsity", 1.0 - (sf.col(f'{AC.ORIG}_num_nonzero_cells') / sf.col('num_cells'))).persist()
    df = df.withColumn(f"{AC.PRIV}_geolevel_sparsity", 1.0 - (sf.col(f'{AC.PRIV}_num_nonzero_cells') / sf.col('num_cells'))).persist()
    df = df.withColumn("L1(geolevel_sparsity)", sf.abs(sf.col(f"{AC.PRIV}_geolevel_sparsity") - sf.col(f"{AC.ORIG}_geolevel_sparsity"))).persist()
    return df



def grouping2sql(column, recodedict):
    """
    Takes a dict and transforms it to a SQL statement for recoding levels
    """
    sql_when = ["case"]
    sql_when += [f"when {column} = '{index}' then '{level}'" for index,level in recodedict.items()]
    sql_when += ["else '-1' end"]
    sql_when = " ".join(sql_when)
    return sql_when


"""
Calculating Population Density Estimates for the CEF and DAS output

The DAS experiment data does not include all of the blocks (at least not in the data found here:
{AC.S3_BASE}kifer001/Aug29_experiments_hierarchicalAgeRangeTopDown_branchFactor4_output_danVariant1-2/td4/run_0000/)

Since that's the case, performing a join on any lower levels and aggregating will cause the land and water areas
to be incorrect if they happen to not exist in the experiment data, but have nonzero area in the GRFC. This would
also make the population density calculations incorrect.

The approach will be to aggregate, in parallel, the experiment data and the GRFC areas and join them at each
respective geographic level to calculate the population densities for each geographic unit.
"""
def aggregateGeolevelGRFC(df, geolevel, groupby=None):
    if groupby is None:
        groupby = [geolevel]
    df = df.groupBy(groupby).sum().persist()
    df = df.drop('geocode').persist()
    df = df.withColumnRenamed(geolevel, 'geocode').persist()
    df = df.withColumn('geolevel', sf.lit(geolevel)).persist()
    df = stripSQLFromColumns(df)
    return df

def population_density(spark, df, schema, geolevels):
    geolevels = du.aslist(geolevels)

    # get the geounit totals for each geounit in all geographic levels provided
    df = aggregateGeolevels(spark, df, geolevels)
    df = answerQuery(df, schema, "total")
    print_item(df, f"Get geounit totals for each geounit in all geographic levels in {geolevels}", 200)

    # load the block level areas and crosswalks
    block_areas = crosswalk.getAreaDF(spark)
    crossdf = crosswalk.getCrosswalkDF(spark, columns=geolevels)
    block_areas = block_areas.join(crossdf, on=AC.GEOCODE).persist()
    print_item(block_areas, "Block areas joined with crosswalk", show=100)

    # aggregate GRFC block areas to get geounit areas
    for i,geolevel in enumerate(geolevels):
        if i == 0:
            area = aggregateGeolevelGRFC(block_areas, geolevel)
        else:
            area = area.unionByName(aggregateGeolevelGRFC(block_areas, geolevel))

    print_item(area, "Area DF", show=200)

    df = df.join(area, on=[AC.GEOCODE, AC.GEOLEVEL]).persist()
    for col in [AC.ORIG, AC.PRIV]:
        df = df.withColumn(f"pop_density_{col}", sf.col(col) / sf.col("AREALAND")).persist()
    df = df.withColumn("diff_pop_density", sf.col(f"pop_density_{AC.PRIV}")  - sf.col(f"pop_density_{AC.ORIG}")).persist()
    df = df.withColumn("L1_pop_density", sf.abs(sf.col("diff_pop_density"))).persist()
    return df


#############################################################################
# NNLS != OLS Metrics
#############################################################################






#############################################################################
# Vikram's functions for Voting Rights metrics and DOJ metrics
#
# TODO: update functions to fit into most recent version of Analysis
#############################################################################
def getAnswers(spark, df, geolevels,schema, queries):
    """ Answers queries at the geolevels specified """
    geoleveldf = aggregateGeolevels(spark, df, geolevels)
    queryx = answerQueries(geoleveldf, schema, queries, labels=True)
    print_item(queryx, f"Query answers for geolevels {geolevels} and queries {queries}")
    return queryx


def votingMetricsSummary(spark, df,geolevels,queries,queries2,schema):
    # This computes race counts and provides summary statistics for specified district (upper/lower chambers, CD) using Ashwin and Elizabeth's metrics
    u=getAnswers(spark,df,geolevels,schema,queries)

    v=getAnswers(spark,df,geolevels,schema,queries2)
    print("u is")
    u.show()
    print ("v is")
    v.show()
    #u_df = u.df
    #v_df = v.df
    # 'priv' means "protected via the differential privacy routines in this code base" variable to be renamed after P.L.94-171 production
    v = v.select(['geocode', 'geolevel', 'run_id', 'plb', 'orig', 'priv'])
    v = v.withColumnRenamed('orig', 'orig_total')
    v = v.withColumnRenamed('priv', 'priv_total')

    groupby = ['geocode', 'geolevel', 'run_id', 'plb']
    # This gives summary statistics and race counts
    joined_df = u.join(v, on=groupby)
    print("Summary statistics and race counts for selected district:")
    joined_df.show()
    return joined_df

def votingMetricsAverage(joined_df):
    # This gives average race counts for Upper/Lower Chambers averaged over trials
    joined2_df = joined_df.withColumn('orig_race_divided_by_orig_total', sf.col('orig') / sf.col('orig_total'))
    avgd_df = joined2_df.groupBy(['geocode', 'geolevel', 'plb', 'level', 'query']).avg()
    joined_df3 = joined2_df.withColumn('priv_race_divided_by_priv_total', sf.col('priv') / sf.col('priv_total'))
    avgd_df2 = joined_df3.groupBy(['geocode', 'geolevel', 'plb', 'level', 'query']).avg()
    #This gives average race counts in district, averaged over trials
    final=avgd_df2.select(['geocode','geolevel','plb','level','avg(orig_race_divided_by_orig_total)','avg(priv_race_divided_by_priv_total)'])
    print("The average race counts in given district, averaged over trials are:")
    final.show()
    return final

def AvgPop(spark, df,geolevels,queries,schema):
    #These three functions are for DOJ metrics for Variation relative to Average
    #This function calculates average population of group g over DAS runs
    u=getAnswers(spark,df,geolevels,schema,queries)
    avgpopG = u.groupBy(['geocode','geolevel','level']).avg()
    return avgpopG

def Variation(spark,df,geolevels, queries, schema,avgpopG):
    #Calculates the Variation among population of group g over DAS runs
    u=getAnswers(spark,df,geolevels,schema,queries)
    hilk=u.select(['run_id']).distinct().count()
    groupz=['geocode', 'geolevel', 'level']
    g=getSampleVariance(u,'orig',hilk,groupby=groupz)
    k=getSampleVariance(u,'priv',hilk,groupby=groupz)
    columns_to_drop = ['sum(orig)', 'sum(priv)','sum(orig^2)','n*sum(orig^2)','sum(orig)^2','n*sum(orig^2)-sum(orig)^2']
    ga = g.drop(*columns_to_drop)
    columns_to_drop2 = ['sum(orig)', 'sum(priv)','sum(priv^2)','n*sum(priv^2)','sum(priv)^2','n*sum(priv^2)-sum(priv)^2']
    ka=k.drop(*columns_to_drop2)
    cap=avgpopG.join(ga,on=groupz)
    cap2=cap.join(ka,on=groupz)
    cap3=cap2.withColumn('sqrt var', sf.sqrt(sf.col('sample_variance(priv)')))
    #Relative Variation for each group over runs
    cap4=cap3.withColumn('relative var',sf.col('sqrt var')/sf.col('avg(priv)'))
    return cap4

def AvgRelVarGroups(cap4):
    # Calculates Average relative variation among population over G groups
    tap=cap4.groupBy('geocode','geolevel').agg(sf.round(sf.avg("relative var"), 4).alias("NEWVAR"))
    return tap

def VariationPop(spark,df,geolevels,queries,schema):
    # This is for Variation relative to Population.
    # This function calculates Relative Variation
    u=getAnswers(spark,df,geolevels,schema,queries)
    go = u.select(['geocode','geolevel','run_id','level','orig','priv'])
    go2=go.withColumn('sq',sf.lit(2))
    ye=go2.withColumn('diff',sf.col('priv')-sf.col('orig'))
    ye2=ye.withColumn('sq diff', sf.pow(sf.col('diff'),sf.col('sq')))
    # This counts the number of runs, for the denominator
    gi=ye2.select(['run_id']).distinct().count()
    ye3=ye2.withColumn('count',sf.lit(gi))
    ye4=ye3.withColumn('variation for each run',sf.col('sq diff')/sf.col('count'))
    groupz=['geocode', 'geolevel', 'level']
    ye5=ye4.groupby(['geocode','geolevel','level','orig']).agg(sf.round(sf.sum("variation for each run"),4).alias("TotVariation"))
    #yet=ye4.groupBy(['geocode','geolevel','level']).avg()
    # This gives Variation
    #ye6=ye5.join(yet,on=groupz)

    return ye5

def RelVarPop(ye5):
    #This gives relative variation
    ye6=ye5.withColumn('relative var', sf.sqrt(sf.col('TotVariation'))/sf.col('orig'))
    return ye6

def AvgRelVarPop(ye6):
    ye7=ye6.groupBy('geocode','geolevel').agg(sf.round(sf.avg("relative var"), 4).alias("AvgRelVar"))
    return ye7


def MAE(sdf):
    # This takes a dataset and returns the Mean Absolute Error
    number_of_rows = sdf.count()   # Get total number of rows
    sum_absdiff = sdf.groupBy().agg(sf.sum('L1')).collect()[0][0]    # Ge sum of all L1 errors

    if (number_of_rows > 0):
        MAE = (sum_absdiff/number_of_rows)

    else:
        MAE = 0

    return MAE


def RMS(sdf):
# This function takes a dataset and returns the Root Mean Squared Error

    number_of_rows = sdf.count()
    sum_absdiffsquared = sdf.groupBy().agg(sf.sum('L2')).collect()[0][0]

    if (number_of_rows > 0):

        RMS = (sum_absdiffsquared/number_of_rows)
        RMS = math.sqrt(RMS)


    else:
        RMS = 0

    return RMS

def Coe_of_variation(sdf, RMS):
    # This function returns the coefficient of variation
    if (sdf.count() > 0):
        avg_geographies = sdf.groupBy().avg('orig').collect()[0][0]
    #if (avg_geographies > 0):

        COV = 100*RMS/(avg_geographies)
    else:
        COV = 0

    return COV

def MAPE(sdf):
    #This function returns the Mean Absolute Percent Error
    number_of_rows = sdf.count()
    if (number_of_rows > 0):
        sdf=sdf.withColumn('abs diff div cef', sf.col('L1')/sf.col('orig'))
        sum_absdivcef = sdf.groupBy().agg(sf.sum('abs diff div cef')).collect()[0][0]
    #if (number_of_rows > 0):
        MAPE = 100*(sum_absdivcef/number_of_rows)
    else:
        MAPE = 0

    return MAPE

def MALPE(sdf):
    # This function returns the mean algebraic percent error
    number_of_rows = sdf.count()
    if (number_of_rows > 0):
        sdf=sdf.withColumn('diff', sf.col('priv')-sf.col('orig'))
        sdf=sdf.withColumn('diff div cef', sf.col('diff')/sf.col('orig'))
        sum_diffdivcef = sdf.groupBy().agg(sf.sum('diff div cef')).collect()[0][0]
    #if (number_of_rows > 0):
        MALPE = 100*(sum_diffdivcef/number_of_rows)
    else:
        MALPE = 0
    return MALPE

def Count_percentdiff_10percent(sdf):
   # This function returns the number of rows that satisfy 10% or greater percentage difference
    number_of_rows = sdf.count()
    if (number_of_rows > 0):

        sdf=sdf.withColumn('abs diff div cef', sf.col('L1')/sf.col('orig'))

        count = sdf.groupBy().agg(sf.count(sf.when(sf.col("abs diff div cef")>0.1, True))).collect()[0][0]
    else:
        count = 0
    return count

def mattsmetrics(sdf, spark):
    # This function performs all of the metrics listed above, MAE, MAPE, MALPE, RMS, COV, and percent threshold
    MAE_value = MAE(sdf)
    print("MAE is", MAE_value)

    RMS_value = RMS(sdf)
    print("RMS value is", RMS_value)
    CoV_value = Coe_of_variation(sdf, RMS_value)
    print("COV is:", CoV_value)
    MAPE_value = MAPE(sdf)
    print("MAPE is:", MAPE_value)
    MALPE_value=MALPE(sdf)
    print("MALPE is:", MALPE_value)
    greaterthan10percent=Count_percentdiff_10percent(sdf)
   # greaterthan10percent=greaterthan10percent.collect()[0][0]
    df=spark.createDataFrame([(MAE_value, "MAE")])
    newRow1 =  spark.createDataFrame([(RMS_value, "RMS")])
    newRow2 =  spark.createDataFrame([(CoV_value, "Cov_value")])
    newRow3 = spark.createDataFrame([(MAPE_value, "MAPE")])
    newRow4 = spark.createDataFrame([(MALPE_value, "MALPE_value")])
    newRow5 = spark.createDataFrame([(greaterthan10percent, "count perc diff > 10")])
    df=df.union(newRow1).union(newRow2).union(newRow3).union(newRow4).union(newRow5)
    return df

"""
def metrics_script(sdf, bucket_size, spark):
    print("BUCKET SIZE IS:", bucket_size)
    if (bucket_size=="NA"):  # No buckets required
        result=metrics_with_bucket(sdf, bucket_size, spark, key="A")
    if (bucket_size!="NA"):
        result=metrics_with_bucket(sdf, bucket_size, spark, key="B")
    return result
"""
def metrics_with_popbucket(sdf, bucket_size, spark, key):
    if (key == "A"):   # no buckets,
        spark_df2 = sdf.subtract(sdf.filter(sdf.level.rlike("Not")))
        spark_df2.show(100, False)
        print("Make sure 'Not' values are removed")
        metrics_dataframe = mattsmetrics(spark_df2,spark)
        Counts = spark_df2.count()
        print("Counts are", Counts)
        newRow = spark.createDataFrame([(Counts, "Counts")])
        complete_dataframe = metrics_dataframe.union(newRow)

    if (key == "B"): # Needs bucketing
        metrics_dataframe=[]
        for b in bucket_size:
            print("Bucket is:" , b)
            subset_sparkdf =sdf[sdf['orig_count_bin']==b] #subset into bins
            subset_sparkdf.show(100, False)
            print("Bucketed data")
            subset_sparkdf1=subset_sparkdf.subtract(subset_sparkdf.filter(subset_sparkdf.level.rlike("Not")))
            subset_sparkdf1.show(100, False)
            print("Make sure its bucketed and 'Not' values are removed")
            subset_metrics = mattsmetrics(subset_sparkdf1,spark)
            Counts = subset_sparkdf1.count()
            newRow = spark.createDataFrame([(b, "Bucket")])
            newRow1 = spark.createDataFrame([(Counts, "Counts")])
            new_union = subset_metrics.union(newRow).union(newRow1)
            metrics_dataframe.append(new_union)
        complete_dataframe=functools.reduce(DataFrame.unionAll, metrics_dataframe)
    return complete_dataframe

def metrics_with_3geolevels(sdf, spark, geolevels): # This function is for tables with geolevels of county, tract, and place
    metrics_dataframe=[]
    for g in geolevels:
        spark_df1=sdf[sdf['geolevel']==g]
        subset_sparkdf1=spark_df1.subtract(spark_df1.filter(spark_df1.level.rlike("Not")))
        subset_sparkdf1.show(100,False)
        subset_metrics = mattsmetrics(subset_sparkdf1,spark)
        Counts = subset_sparkdf1.count()
        print("Counts are", Counts)
        newRow = spark.createDataFrame([(Counts, "Counts")])
        newRow1 = spark.createDataFrame([(g, "Geolevel")])
        new_union=subset_metrics.union(newRow).union(newRow1)
        metrics_dataframe.append(new_union)

    complete_dataframe = functools.reduce(DataFrame.unionAll, metrics_dataframe)
    return complete_dataframe

def metrics_with_age(sdf, spark, age_list, key):
    metrics_dataframe=[]
    print("Table 32 processing")
    sdf.show(100,False)
    if (key=="A"):
        for age in age_list:
            subset = sdf.filter(sdf.level.rlike(age))
            subset.show(100, False)
            subset_metrics=mattsmetrics(subset, spark)
            Counts = subset.count()
            print("Counts are", Counts)
            newRow = spark.createDataFrame([(Counts, "Counts")])
           # newRow1 = spark.createDataFrame([(g, "Geolevel")])
            new_union=subset_metrics.union(newRow)
            metrics_dataframe.append(new_union)

        complete_dataframe = functools.reduce(DataFrame.unionAll, metrics_dataframe)

    if (key=="B"):
        for age in age_list:
            subset_sparkdf1=sdf.filter(sdf.level.rlike(age))
            subset_sparkdf1.show(100,False)
         #   metrics1=mattsmetrics(subset_sparkdf1, spark)
         #   Counts=subset_sparkdf1.count()
         #   print("Counts are", Counts)
         #   print("Age level is:", age)
         #   newRow = spark.createDataFrame([(Counts, "Counts")])
            newRow1 = spark.createDataFrame([(age, "Age level")])

         #   new_union=metrics1.union(newRow).union(newRow1)
         #   metrics_dataframe.append(new_union)
            subset_male=subset_sparkdf1.filter(subset_sparkdf1.level.rlike("Male"))
            subset_male.show(100,False)
            metrics_male=mattsmetrics(subset_male, spark)
            Counts=subset_male.count()
            newRow = spark.createDataFrame([(Counts, "Counts")])
            new_gender = spark.createDataFrame([("Male","Gender")])
            new_union=metrics_male.union(newRow).union(newRow1).union(new_gender)
            metrics_dataframe.append(new_union)
            subset_female=subset_sparkdf1.filter(subset_sparkdf1.level.rlike("Female"))
            subset_female.show(100,False)
            metrics_female=mattsmetrics(subset_female, spark)
            Counts=subset_female.count()
            newRow = spark.createDataFrame([(Counts, "Counts")])
            new_gender=spark.createDataFrame([("Female","Gender")])
            new_union=metrics_female.union(newRow).union(newRow1).union(new_gender)
            metrics_dataframe.append(new_union)
        complete_dataframe=functools.reduce(DataFrame.unionAll, metrics_dataframe)
    return complete_dataframe

def combined_metrics(schema, sdf, spark, geolevels, agekey, sexkey, bucketkey, key):
    if (key=="A"): # no buckets, Tables 1a, 2a, 3, 13, 17a

        spark_df1 = sdf.subtract(sdf.filter(sdf.level.rlike("Not")))
        spark_df1.show(100, False)
        print("Make sure 'Not' values are removed")
        metrics_dataframe = mattsmetrics(spark_df1,spark)
        Counts = spark_df1.count()
        print("Counts are", Counts)
        newRow = spark.createDataFrame([(Counts, "Counts")])
        complete_dataframe = metrics_dataframe.union(newRow)
#        file_name = f"{table_name}.csv"

    if (key=="B"):
        metrics_dataframe=[]
        if (bucketkey=='1'):
            buckets = ["[0-1000)", "[1000-5000)", "[5000-10000)", "[10000-50000)", "[50000-100000)", "100000 +"]
        if (bucketkey=='2'):
            buckets = ["[0-10)", "[10-100)", "100 +"]
        for b in buckets:
            print("Bucket is:" , b)
            subset_sparkdf =sdf[sdf['orig_count_bin']==b] #subset into bins
            subset_sparkdf.show(100, False)
            print("Bucketed data")
            subset_sparkdf1=subset_sparkdf.subtract(subset_sparkdf.filter(subset_sparkdf.level.rlike("Not")))
            subset_sparkdf1.show(100, False)
            print("Make sure its bucketed and 'Not' values are removed")
            subset_metrics = mattsmetrics(subset_sparkdf1,spark)
            Counts = subset_sparkdf1.count()
            newRow = spark.createDataFrame([(b, "Bucket")])
            newRow1 = spark.createDataFrame([(Counts, "Counts")])
            new_union = subset_metrics.union(newRow).union(newRow1)
            metrics_dataframe.append(new_union)
        complete_dataframe=functools.reduce(DataFrame.unionAll, metrics_dataframe)

    if (key=="C"):
        metrics_dataframe=[]
        for g in geolevels:
            spark_df1=sdf[sdf['geolevel']==g]
            subset_sparkdf1=spark_df1.subtract(spark_df1.filter(spark_df1.level.rlike("Not")))
            subset_sparkdf1.show(100,False)
            subset_metrics = mattsmetrics(subset_sparkdf1,spark)
            Counts = subset_sparkdf1.count()
            print("Counts are", Counts)
            newRow = spark.createDataFrame([(Counts, "Counts")])
            newRow1 = spark.createDataFrame([(g, "Geolevel")])
            new_union=subset_metrics.union(newRow).union(newRow1)
            metrics_dataframe.append(new_union)

        complete_dataframe = functools.reduce(DataFrame.unionAll, metrics_dataframe)
    if (key=="D"):
        metrics_dataframe=[]
        if (agekey=='1'):
            age_list = ["Under 18", "18 to 64", "65 years and over"]
        else:
            age_list = ["Under 5 years", "5 to 9 years", "10 to 14 years", "15 to 19 years", "20 to 24 years", \
"25 to 29 years", "30 to 34 years", "35 to 39 years", "40 to 44 years", "45 to 49 years", "50 to 54 years",\
 "55 to 59 years", "60 to 64 years", "65 to 69 years", "70 to 74 years", "75 to 79 years", "80 to 84 years"\
, "85 to 89 years", "90 to 94 years", "95 to 99 years", "100 to 104 years", "105 to 109 years", "110 to 115\
 years"]
        for age in age_list:
            subset = sdf.filter(sdf.level.rlike(age))
            subset.show(100, False)
            subset_metrics=mattsmetrics(subset, spark)
            Counts = subset.count()
            print("Counts are", Counts)
            newRow = spark.createDataFrame([(Counts, "Counts")])
           # newRow1 = spark.createDataFrame([(g, "Geolevel")])
            new_union=subset_metrics.union(newRow)
            metrics_dataframe.append(new_union)

        complete_dataframe = functools.reduce(DataFrame.unionAll, metrics_dataframe)
    if (key=="E"):
        metrics_dataframe=[]
        if (agekey=='1'):
            age_list = ["Under 18", "18 to 64", "65 years and over"]
        else:
            age_list = ["Under 5 years", "5 to 9 years", "10 to 14 years", "15 to 19 years", "20 to 24 years", \
"25 to 29 years", "30 to 34 years", "35 to 39 years", "40 to 44 years", "45 to 49 years", "50 to 54 years",\
 "55 to 59 years", "60 to 64 years", "65 to 69 years", "70 to 74 years", "75 to 79 years", "80 to 84 years"\
, "85 to 89 years", "90 to 94 years", "95 to 99 years", "100 to 104 years", "105 to 109 years", "110 to 115\
 years"]
        for age in age_list:
            subset_sparkdf1=sdf.filter(sdf.level.rlike(age))
            subset_sparkdf1.show(100,False)
            newRow1 = spark.createDataFrame([(age, "Age level")])

            subset_male=subset_sparkdf1.filter(subset_sparkdf1.level.rlike("Male"))
            subset_male.show(100,False)
            metrics_male=mattsmetrics(subset_male, spark)
            Counts=subset_male.count()
            newRow = spark.createDataFrame([(Counts, "Counts")])
            new_gender = spark.createDataFrame([("Male","Gender")])
            new_union=metrics_male.union(newRow).union(newRow1).union(new_gender)
            metrics_dataframe.append(new_union)
            subset_female=subset_sparkdf1.filter(subset_sparkdf1.level.rlike("Female"))
            subset_female.show(100,False)
            metrics_female=mattsmetrics(subset_female, spark)
            Counts=subset_female.count()
            newRow = spark.createDataFrame([(Counts, "Counts")])
            new_gender=spark.createDataFrame([("Female","Gender")])
            new_union=metrics_female.union(newRow).union(newRow1).union(new_gender)
            metrics_dataframe.append(new_union)
        complete_dataframe=functools.reduce(DataFrame.unionAll, metrics_dataframe)
    if (key=="F"):
        query1 = "total"
        query2= "hispanic*whiteAlone"
        spark_df1 = aggregateGeolevels(spark, sdf, geolevels)
        spark_df1 = answerQueries(spark_df1, schema, query1)
        spark_df1 = getFullWorkloadDF(spark_df1, schema, query1,groupby=[AC.GEOCODE, AC.GEOLEVEL, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP])
        spark_df2 = aggregateGeolevels(spark, sdf, geolevels)
        spark_df2 = answerQueries(spark_df2, schema, query2)
        spark_df2 = getFullWorkloadDF(spark_df2, schema, query2,groupby=[AC.GEOCODE, AC.GEOLEVEL, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP])
        spark_df1 = getL1(spark_df1, colname = "L1", col1=AC.PRIV, col2=AC.ORIG)
        spark_df1 = getL2(spark_df1, colname = "L2", col1=AC.PRIV, col2=AC.ORIG)
        spark_df2 = getL1(spark_df2, colname = "L1", col1=AC.PRIV, col2=AC.ORIG)
        spark_df2 = getL2(spark_df2, colname = "L2", col1=AC.PRIV, col2=AC.ORIG)
        spark_df1.show(100, False)
#        spark_df2.show(100, False)
        spark_df2=spark_df2.filter(spark_df2.level.rlike("Not"))
        spark_df2.show(100, False)
        spark_df1=spark_df1.select("geocode","orig","priv","level","L1","L2")
        spark_df2=spark_df2.select("geocode","orig","priv","level","L1","L2")
        spark_df2=spark_df2.withColumnRenamed("orig","orig1").withColumnRenamed("priv","priv1").withColumnRenamed("L1","L1_1").withColumnRenamed("L2","L2_2")
        print("count of 1", spark_df1.count())
        print("count of 2", spark_df2.count())
        spark_df3=spark_df1.join(spark_df2, ["geocode"])
        print("count of join", spark_df3.count())
        spark_df3.show(100,False)
        spark_df3=spark_df3.withColumn("fraction",sf.col('orig1')/sf.col('orig'))
        spark_df3.show(100,False)
        spark_df3 = getCountBins(spark_df3, column="fraction", bins=[0,0.10,0.49]).persist()
        spark_df3.show(100,False)
        buckets=["[0-0.1)", "[0.1-0.49)", "0.49 +"]
        metrics_dataframe=[]
        for b in buckets:
            subset_sparkdf =spark_df3[spark_df3['fraction_count_bin']==b]
            subset_sparkdf.show(100, False)
            subset_metrics = mattsmetrics(subset_sparkdf,spark)
            Counts = subset_sparkdf.count()
            newRow = spark.createDataFrame([(b, "Bucket")])
            newRow1 = spark.createDataFrame([(Counts, "Counts")])
            new_union = subset_metrics.union(newRow).union(newRow1)
            metrics_dataframe.append(new_union)
        complete_dataframe=functools.reduce(DataFrame.unionAll, metrics_dataframe)


    return complete_dataframe
