# ROADMAP
This is the roadmap to all of the disclosure avoidance git repositories used at the U.S. Census Bureau. It documents repositories on the following systems:

* https://github.ti.census.gov/ - The GitHub server operated by the Census Technical Integrator for the 2020 production system.
* https://adsd-032.ss-inf.nsx1.census.gov/ - The GitLab server created by CSVD to provide for collaboration within the Census Bureau.
* https://github.com/ - The GitHub.com server, existing outside the Census Bureau's firewall, used for collaboration between the U.S. Census Bureau and our external academic partners.

# Top-Level Repositories
Note: repositories that are das-XXXX on the internal Git server are census-das-XXXX on the external server.

## Production Disclosure Avoidance System
* [das-ROADMAP](https://github.ti.census.gov/CB-DAS/das-ROADMAP) This repository
* [das_framework](https://github.ti.census.gov/CB-DAS/das_framework) The framework for building disclosure avoidance systems
* [das_decennial](https://github.ti.census.gov/CB-DAS/das_decennial) Repository for the system that runs the 2020 census of Population and Housing Disclosure Avoidance System. Includes sub-modules.
* [das-vm-config](https://github.ti.census.gov/CB-DAS/das-vm-config) Describes the configuration of the virtual machines and EMR cluster that are used by DAS. Also documents users.
* [hdmm](https://github.ti.census.gov/CB-DAS/hdmm) --- The High-Dimensional Matrix Mechanism

The 2020 Census is using the following repositories. The URLs on the TI GitHub server are provided:

## Related repositories (used as submodules)
* [das-e2e-release](https://github.ti.census.gov/CB-DAS/das-e2e-release) --- Public release of the 2018 E2E DAS.
* [das_analysis](https://github.ti.census.gov/CB-DAS/das_analysis) --- Analysis tools for the DAS
* [dfxml](https://github.ti.census.gov/CB-DAS/dfxml) -- Digital Forensics XML, used for recording provenance information about runs.
* [ctools](https://github.ti.census.gov/CB-DAS/ctools) -- Census tools for working with Amazon S3 and EMR.
* [census_etl](https://github.ti.census.gov/CB-DAS/census_etl)  -- Census ETL system
* [etl_2020](https://github.ti.census.gov/CB-DAS/etl_2020) -- Census-specific ETL tools for the 2020 Census. Uses census-etl
* [das-framework-demo](https://github.ti.census.gov/CB-DAS/das-framework-demo) A demonstration of using das-framework.


## Repositories that we hope to use one day:

* [das-deploy](https://github.ti.census.gov/CB-DAS/das-deploy) Controls the launch and deployment of the DAS cluster. In our development environment, this repository is monitored by the Jenkins system. Making a change to it and doing a commit may launch a cluster, so be careful.


## Reidentification Project
* [das-reid-processing](https://github.ti.census.gov/CB-DAS/das-reid-processing) -- master repository for the reconstruction and reidentification project
* reid-reconf -- legacy repository for just the reconstruction project. Deprecated.

# 2020 Census



## Critical Files
* [CONOPS_AWS](https://github.ti.census.gov/CB-DAS/das-vm-config/blob/master/CONOPS_AWS.md) How DAS uses Amazon Web Services

# Other notable repositories and locations
* https://github.com/dpcomp-org/census-das-dev/tree/master/workload - The  the code is for representing the SF1 queries in Python.  Look in sf1_tables.py and you can see sample output in comments.
