Using Random Data
=
This folder contains a series of tutorial / example scripts for using Analysis.


These are example scripts where the analysis results are computed on randomly-generated data:
* [Exploring Random Data](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_random_data/exploring_random_data.py) - Demonstrates how to generate an RDD of random "geounit" data *(in dict form)* and then use the datatools.py module's **rdd2df** function to transform the RDD into a Spark DF.
* [Using Analysis in the DAS](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_random_data/using_analysis_in_das.py) - Building off of the previous script *(but using the GeounitNode class instead of dicts)*, this script provides examples for how to transform an RDD into a Spark DF and use the sdftools.py module to perform different operations on the data.
* [Saving Analysis Results](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_random_data/saving_analysis_results.py) - Shows how to use the Analysis/PickledDASExperiment object's **save_location_linux** and **save_location_s3** attributes to save results in a variety of forms and locations (local or in S3).
* [Filtering Data By Value or Quantile](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_random_data/filtering_toy_data_by_quantile.py) - *Work in progress* - Aims to demonstrate how to filter a Spark DF for rows that match certain criteria, such as existing within a certain quantile range or threshold.
* [Group Quantiles](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_random_data/group_quantiles.py) - Illustrates how to use the sdftools.py module's getGroupQuantiles function.
