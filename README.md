# DAS 2020 Redistricting Production Code Release 

This is the release of the source code and documentation for the US
Census Bureau's Disclosure Avoidance System (DAS) which is used to protect
individual responses from the 2020 Decennial Census of Population and
Housing. This particular release consists of the code used to generate
the inputs used to tabulate the
[Public Law 94-171 Redistricting Data Summary Files](https://www.census.gov/programs-surveys/decennial-census/about/rdo/summary-files.html)
(P.L. 94-171) which are used, notably, for redistricting and official population
counts.

A unique benefit of differentially private system like the DAS is that
it is fully transparent; it is designed to allow full disclosure of
the algorithm code and the
[privacy parameters](https://www2.census.gov/programs-surveys/decennial/2020/program-management/data-product-planning/2010-demonstration-data-products/ppmf20210608/2021-06-08-privacy-loss_budgetallocation.pdf)
that underpin the random assignment of noise infusion into the
unprotected data. This code release allows individuals to analyze and
understand the code used in the disclosure avoidance process for this
important federal data publication.

This release contains the actual code that was used to transform the
Census Edited File (CEF) that was used in the 2020 Census into a
Microdata Detail File (MDF) that was then used to create the summary
file tabulations for the P.L. 94-171 data release.

Documentation of the DAS implementation can be found in the
[wiki](https://github.com/uscensusbureau/DAS_2020_Redistricting_Production_Code/wiki)
associated with this repostitory. In addition, papers describing the
underlying methods and mathematics will be added to this repository as
they are released.
