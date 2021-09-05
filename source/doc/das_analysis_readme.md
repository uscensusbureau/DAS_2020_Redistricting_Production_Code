DAS Analysis
=

# Design
## Spark SQL
The analysis module uses SparkSQL to evaluate a variety of metrics on the output of the DAS compared with the CEF values. It does this through a series of steps, beginning with the DAS output RDD.
The analysis module should provide the ability to evaluate either a single run of the DAS or experiments containing many DAS trials.

The steps are:
1. Read in the pickled RDD of dictionaries (that represent the most salient information from the GeounitNode objects).
    * These are assumed to contain, at a minimum, the `geocode`, `raw`, and `syn` attributes from the GeounitNode class.
2. Transform the pickled RDD of dictionaries into a Spark DataFrame object with columns that match the programs.schema.schema.Schema object for the data product.
3. Use custom functions encapsulating SparkSQL and/or the pre-built analysis toolsets to compute desired statistics.
4. Save the statistics somewhere wehere they can be easily accessed for future evaluation and/or visualization.

## Toolsets
The following are the modules that encapsulate some of the common functionality we'll want for analyzing the data.

### [sdftools.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/sdfanalysis/analysis/sdftools.py)
This is the primary toolset for manipulating the Spark DataFrames. The most commonly used functions are:
* `getExperimentSparseDF`: Converts a list of pickled RDDs (using a list of string pointers to where the RDDs live in S3) into a single Spark DataFrame.
    * This takes advantage of the programs.sparse.multiSparse representation of the histogram by only building pyspark.sql.Row objects when the `raw count for a particular cell > 0` OR `syn count for a particular cell > 0`. In other words, it excludes all cells in which both the CEF and DAS output values are zero.
* `getGeolevelDF`: This takes advantage of the `data` Spark DataFrame when joined with the `crosswalk` Spark DataFrame. It returns the geolevel desired by aggregating over block nodes according (at present) to the crosswalks detailed in the crosswalk.py module.
* `getQueryDF`: This computes any of the `queries` found in the `programs.schema.schema.Schema` for this data.

### [metrictools.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/sdfanalysis/analysis/metrictools.py)
This is the toolset where metric classes and other metric-based functions exist.
* Each metric class is instantiated by the user prior to running analysis with the specific keyword arguments for calculating that particular metric. Each of these has a `metric.calculate` function that is automatically called from within analysis. Analysis will pass the `spark_dataframe`, `schema`, `geolevel`, and `query_name` to the calculate function, so the user needs to define what the class's calculate function will do with the information provided.
* Metric-based functions are passed into analysis as a dictionary of (name, function). These allow for custom SparkSQL and have the following interface:
    * `metric_function(spark_dataframe, schema)`
    * Note that the spark_dataframe passed in will be the Block-level dataframe prior to any queries or metrics being applied. 

## Helper Modules

### [mappers.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/sdfanalysis/analysis/mappers.py)
This module contains functions that generate dictionaries from the multiSparse objects. These dictionaries are then turned into Row objects in sdftools.py. There are currently two mapper functions in here:
* `getSparseDF_mapper(node, schema)`: This helps to create the Schema/Histogram rows.
* `getMicrodataDF_mapper`: This helps to create microdata records.

### [crosswalk.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/sdfanalysis/analysis/crosswalk.py)
This currently works by loading Simson and Noah's generated crosswalk files from S3. Eventually, it should use either the GRF-C file(s) and/or Simson's stats_2010 geolevel-crosswalk-generating functions. The crosswalk dataframe is joined to the dataframe containing the histogram data to allow for easy geolevel aggregations. This is done automatically by analysis at the moment, or by using the `sdftools.joinWithCrosswalkDF` function to join the crosswalk and data dataframes.

# How to run
After editing the code within the `if __name__ == "__main__":` of `dasan.py` for this specific analysis, run the following command:
* `bash dasan.sh`

The output will automatically be logged in `dasan.out`.

### [dasan.py](https://github.ti.census.gov/CB-DAS/das_decennial/blob/sdfanalysis/analysis/dasan.py)
This is the main analysis loop. The main idea is:
1. Create a list of experiments to analyze.
    * Where "experiment" = ("budget group" S3 path, schema_name for this data)
    * To analyze multiple experiments, create a list of the tuples
2. (Optional) For custom SQL, wrap the statements in functions and create a dictionary of them.
3. For everything else, create lists for the `geolevels` and `query_names`, as well as a dictionary for the `metric` classes to use.
    * These will define a space where each combination of `geolevel`, `query_name`, and `metric` will be calculated.
    * Note that these will be called, individually, by analysis through the `AnalysisTuple` class's `run` method.
4. All results, be they lists (from quantile calculations) or Spark DataFrames, are stored in a dictionary that will be passed to a function called `sdftools.editAndSaveResults`. This function can be replaced with a custom function when calling `main` in `dasan.py` where the results can be saved, visualized, etc. The default functionality of this function is to transform Spark DataFrames into Pandas DataFrames and save them as CSV files, and to save quantile information.
