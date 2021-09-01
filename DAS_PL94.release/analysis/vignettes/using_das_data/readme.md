Using DAS Experiment Data
=
This folder contains a series of tutorial / example scripts for using Analysis.


These are example scripts where the analysis results are computed on DAS Experiment data living in S3:
* [Loading Experiment Data](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_das_data/loading_experiment_data.py) - Shows how to use Analysis to load data from one or more pickled RDDs using S3 paths.
* [Aggregating Geolevels](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_das_data/aggregating_geolevels.py) - Demonstrates how to use the sdftools.py module to aggregate Block-level geographic units to form geographic units at higher geographic levels.
* [Answering Queries](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_das_data/answering_queries.py) - Provides examples for how to answer queries using the sdftools.py module.
* [Population Densities](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_das_data/population_densities.py) - An illustration of how to calculate population densities in a variety of geographic units at different geographic levels.
* [Bias and Relative Error By Bin](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_das_data/bias_and_relerror_by_bin.py) - Shows how to use the sdftools.py module to calculate the Bias and Relative Error within bins of the CEF counts.
* [Geolevel 1-TVD](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_das_data/geolevel_tvd.py) - Illustrates how to calculate the 1-TVD metric for the entire geographic level, as well as plot the results.
* [Joining CEF and MDF](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_das_data/join_mdf_and_cef.py) - Demonstrates one way to merge DAS Experiment/MDF (i.e. 'priv') and CEF (i.e. 'orig') data by using an outer-join and replacing null values with zeros.
* [Joining CEF and MDF: Different datasets](https://github.ti.census.gov/CB-DAS/das_decennial/blob/master/analysis/vignettes/using_das_data/join_mdf_and_cef_different_datasets.py) - Illustrates how to use the `datatools.getJoinedDF` function to merge DAS Experiment/MDF (i.e. 'priv') and CEF (i.e. 'orig') data.
