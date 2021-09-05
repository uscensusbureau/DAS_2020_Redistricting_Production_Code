Instructions for running das_decennial on the 1940 dataset
----------------------------------------------------------


1. Clone das_decennial's master branch to wherever you want it.  (You need to do this if you are reading this file on github and not in a cloned repo.)

2. Start with the example config file `das_decennial/configs/test_lecle_config_1940_mini.ini`

3. Copy this file to a new location in `das_decennial/configs` directory so that you will not be modifying our distribution file.

3. For each path (see below), change the path name to be where you have your data or where you want the output to be written. Path variables include:

    [LOGS]
    hdfs_logfile= hdfs:///tmp/lecle_test.log

    [reader]
    PersonData.path= s3://uscb-decennial-ite-das/1940/output_P.csv
    UnitData.path= s3://uscb-decennial-ite-das/1940/output_H.csv

    [gurobi]
    gurobi_lic= /apps/gurobi752/gurobi_client.lic
    gurobi_logfile_name= /mnt/tmp/PLgurobi0.log

    [writer]
    output_fname= s3://uscb-decennial-ite-das/users/lecle301/test1

    [validator]
    results_fname= /mnt/tmp/PL_results

Note: If you want to do a fast run, use the 'mini' distribution:

    [reader]
    PersonData.path= s3://uscb-decennial-ite-das/1940/output_P_subset_mini.csv
    UnitData.path= s3://uscb-decennial-ite-das/1940/output_H_subset_mini.csv


4. run the code: 

    $ export CONFIG=configs/test_lecle_config_1940_mini.ini   # change to your config file
    $ config=$CONFIG output=your_output_file.out bash run_cluster_small_lecle.sh

