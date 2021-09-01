# Data Integrity within the DAS

## System

1. On boot, DAS downloads and mounts a disk image. The SHA1 for the disk image is stored in the bootstrap file and DAS fails if the SHA1 does not match.
2. Validates other aspects of runtime environment and fails if the environment does not match.

## Inputs

1. DAS validates some edit rules on ingesting the CEF.

2. DAS compiles the CEF specification (currently a Microsoft Excel spreadsheet) into Python code that is used to ingest the CEF.

## Computation

1. Internal unit tests assure that code sections are properly implemented and that they have not been broken as the part of development. These unit tests are run on a regular basis using the `py.test` command.

2. DAS computes a differentially private accuracy metric after it runs and assures that the accuracy is less than 1.0.  (An accuracy of 1.0 would imply that no disclosure avoidance was performed.)

3. DAS will use the Intel hardware random number generator as a source of randomness.

## Output

1. DAS compiles the MDF specification (currently a Microsoft Word spreadsheet) into Python code that is used to create the MDF.

2. DAS output files include a metadata header that reports the number of records and schema information.

3. DAS reads the CEF and the MDF at the conclusion of the DAS run and verifies that the DSEP-approved invariants have been implemented properly.


# Integrity controls needed

1. Currently there is no way to validate the contents of the input file.



