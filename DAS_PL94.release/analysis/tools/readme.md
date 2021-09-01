Analysis Tools
=

These are the modules that allow for the ingesting, transformation, manipulation, evaluation, visualization, and saving of DAS Run and Experiment data.

## [crosswalk.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/crosswalk.py)
The crosswalk.py module provides access to crosswalk files that map Blocks to different geographic units in different geographic levels. This module is used explicitly when calculating Population Densities, and is used implicitly when aggregating geographic levels using some of the sdftools.py module's functions.

#### Notable Functions
* [getCrosswalkDF](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/crosswalk.py#L52) - Returns a Spark DataFrame with the GRFC crosswalk columns.

## [datatools.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/datatools.py)
The datatools.py module contains the Analysis object, which builds Experiment objects. These Experiment objects are used to find, load, and transform the DAS Run/Experiment data into a Spark DataFrame for further analysis. The Analysis and Experiment objects contain valuable path information for saving the results of analysis.

#### Notable Classes and Functions
* [Analysis class](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/datatools.py#L56) - This is the starting point for building Experiments from one or more S3 paths pointing to DAS Run/Experiment data. 
    * It contains useful attributes, such as `save_location_linux` and `save_location_s3`, which are used for saving the results of analysis in a nice, organized way. 
    * In addition, it allows the **analysis_script** and/or **log** to be saved locally or to S3, and also allows the **local results folder** to be _zipped and saved in S3_.
    * Users can build experiments using the [make_experiment](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/datatools.py#L84) function.
* [PickledDASExperiment class](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/datatools.py#L183) - The Experiment object allows users to define a list of DAS runs or a DAS Experiment (using the S3 paths).  
    * The Experiment's [getDF](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/datatools.py#L205) function loads, transforms, and returns the Experiment data in a Spark DataFrame.
* [rdd2df](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/datatools.py#L480) - Transforms an RDD of **GeounitNodes**/**dicts with geounit node data** into a Spark DataFrame.
    * This is the function that can transform an RDD of GeounitNodes within the DAS into a Spark DF in the format required by most of the functions in Analysis.
    * The default is to transform the RDD into a Spark DF in **Sparse Historgram** form, which means that any cells in which both the CEF and DAS output counts are zero will not appear as rows in the Spark DF.

## [graphtools.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/graphtools.py)
The graphtools.py module is used to generate data visualizations for different kinds of analysis results. It currently contains functions for generating **lineplots** and **heatmaps** for the **Geolevel 1-TVD** metric across multiple geographic levels and privacy loss budgets, as well as **lineplots** for **age quantile calculations**.

## [mappers.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/mappers.py)
The mappers.py module contains the Spark mapper functions that perform a variety of transformations. The most important one is transforming the pickled RDDs of **GeounitNodes**/**dicts with geounit node data** into Spark Rows, which are then combined into a Spark DataFrame.

#### Notable Functions
* [getSparseDF_mapper](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/mappers.py#L60) - The Spark RDD mapper function for transforming the **GeounitNodes**/**dicts of GeounitNode data** into a list of Spark Rows.
    * This function works directly from the `multiSparse` object, accessing only the nonzero elements in the sparse matrix, which takes advantage of the sparsity in the histogram.
    * A row is created for each nonzero cell in the histogram.
    * This function creates the **Sparse Histogram** form for the Spark DataFrame.

##### Example Using getSparseDF_mapper
Given a single Geographic Unit with the following geocode and 2x2 raw and syn histograms,
```python
Geocode: "geo"
Raw Histogram:   Syn Histogram:
  =========        =========
  | 5 | 0 |        | 0 | 0 |
  =========        =========
  | 8 | 0 |        | 6 | 1 |
  =========        =========
```

Where the dimensions are 'A' and 'B', with coordinates
```python
          A=0     A=1
       =================
 B=0   | (0,0) | (0,1) |
       =================
 B=1   | (1,0) | (1,1) |
       =================
```
The getSparseDF_mapper function will return the following Rows, where **orig = the raw histogram** and **priv = the syn histogram**.
```python

=================================
| geocode | A | B | orig | priv |
=================================
|   geo   | 0 | 0 |   5  |   0  |
=================================
|   geo   | 1 | 0 |   8  |   6  |
=================================
|   geo   | 1 | 1 |   0  |   1  |
=================================
```
Note that the Row corresponding to the cell at **(0,1)** is missing. This is because both the raw and syn histograms were zero in that cell, so we don't need to add it.

## [sdftools.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/sdftools.py)
The sdftools.py module contains the bulk of the functions for manipulating Spark DataFrames that contain histogram data. These include (but are not limited to) **aggregating geolevels**, **answering queries**, **binning values in columns**, calculating **quantiles within groups**, **sparsity**, and **population density**, and computing a wide-variety of **metrics**.

#### Notable Functions
* [aggregateGeolevels](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/sdftools.py#L24) - Aggregates Block-level geographic units to form geographic units at higher geographic levels.
* [answerQuery](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/sdftools.py#L83) - Answers **a single query** using SQL.
    * Labels for the levels in the dimensions of the query are optional.
    * Queries can be anything that is supported by the DAS Schema for this data.
    * `merge_dims` affects how the schema dimensions are retained/collapsed. See the docstring in the answerQuery function for more details.
* [answerQueries](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/sdftools.py#L165) - Answers **one or more queries** using SQL.
    * The major difference between this function and `answerQuery` is that this one supports answering multiple queries and having them exist simultaneously in the same Spark DataFrame.
    * It supports multiple queries by forcing `merge_dims` to be True, which will concatenate the labels of each query dimension under a single column named 'level'. See the docstring in the answerQueries function for more details.
* [stripSQLFromColumns](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/sdftools.py#L340) - This function strips away the SQL that gets appended to the column names as different operations are performed.
    * After performing a sum, the columns that were aggregated will have a `sum(.)` around them. This function removes the `sum(.)`, leaving only what was inside. (e.g. `sum(orig)` => `orig`)
* [print_item](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/sdftools.py#L441) / [show](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/sdftools.py#L452) - These are essentially the same function and they are handy for creating print statements that print some object/value, as well as an annotation like: `^^^^^ useful comment here ^^^^^`
* [getGroupQuantiles](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/sdftools.py#L540) - Calculates the quantiles of columns within the rows in a group.

##### Example Using getGroupQuantiles
Given a Spark DataFrame with Block-level and State-level geographic units, and query answers for the total and sex queries, we might want to calculate the quantiles of the query answers across geographic level and query.
```python
import analysis.tools.sdftools as sdftools

# We want to calculate the quantiles for the CEF_input = 'orig' and DAS_output = 'priv' columns
columns = ['orig', 'priv']

# Note that since the Spark DF already contains Block and State-level geounits, as well as the 
# query answers for the total and sex queries, 'orig' and 'priv' represent query answers
# for each query and geounit in each geolevel

# Since we want to calculate quantiles across the geounits, we'll want to form different groups,
# such as
# (Block, total, total), (State, total, total), 
# (Block, sex, Male), (Block, sex, Female), (State, sex, Male), (State, sex, Female)
# We can form these groups easily in Spark SQL simply by including the columns we want to group by in a list
groupby = ['geolevel', 'query', 'level']

# For quantile calculation, let's look at the quartiles
quantiles = [0.0, 0.25, 0.5, 0.75, 1.0]

# Calculate the quartiles within each of the designated groups
quantile_df = sdftools.getGroupQuantiles(df, columns, groupby, quantiles)
```

## [setuptools.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/setuptools.py)
The setuptools.py module sets up the SparkSession and Analysis objects.

#### Notable Functions
* [setup](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/tools/setuptools.py#L10) - This is the only function in setuptools.py. It reads information passed in from the `run_analysis.sh` Bash script to modify the `save_location` path to include the log name (i.e. mission name and timestamp), sets up the SparkSession object, and builds and returns the Analysis object.
