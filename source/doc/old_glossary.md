# Controlled vocabulary for the 2020 Census Disclosure Avoidance System

# Terms that are used within Census Decennial Team
These terms are defined outside the DAS team.
* _DRF_ - Decennial Response File. Collects information from the Internet Instrument, mailed-in surveys that are scanned, and NRFU

* _NRFU_ - Non-Response Follow Up, when Census goes out and works with a respondent to fill out the survey.

* _Primary Selection Algorithm_ - Chooses which records in the DRF are used to populate the CUF
* _CUF_ - Census Unedited File. Used for Congressional apportionment. Edited to produce the CEF. Note: The CUF contains names.

* _CEF_ - Census Edited File. The "Ground Truth" of the 2020 Census. The CEF does not contain names.


* _DAS_ - Disclosure Avoidance System. Transforms the CEF into the MDF.

* _MDF_ - Microdata Detail File. Produced by the CEF. Can be thought of as the CEF with "privacy edits" applied.

* _impute_ - When the Census Bureau adds a person or a group quarters that was not present in the DRF

* _allocation_ - When the Census Bureau supplies a missing value or changes a value that was clearly wrong. 
* _HDF_ - The Hundred Percent File, the 2010 analog of the MDF. 

# Terms defined by the DAS team.

## Common language for all DAS systems:
* _attributes_ - The variables or columns that, together with their levels, define each schema over which we are answering differentially private queries. A commonly used schema for SF-1 contains the attributes are: age, sex, race, ethnicity, and relationship to householder. Other data products and tabulations require additional schemas, e.g. to capture household-level information.
* _record_ - A choice of levels for each attribute in a schema. For the demographic schema example given previously, a record corresponds to a person.
* _privacy edits_ - Edits that are made to the CEF to produce the MDF. A privacy edit will not change an invariant. (Note that this language is only appropriate if a one-to-one correspondence is defined between the final MDF records and the initial CEF records, and doing so is only possible because total population by block is an invariant.)
* _Invariant_ - An attribute or a query that cannot be changed.
* _Histogram_ - A mapping of unique combinations of attribute levels to counts. Most often formed by taking the Cartesian product of the sets of levels for each attribute.
* _Geography Level_ - The level of geography for the top-down problem. Can be US, State, County, Tract, Block Group or Block.
* _Geography Identifier_ - Uniquely identifies a specific unit at a particular Geography Level. Also called the _GEOCODE_.
* _Geography_ - In this document, used to describe any unique geographical unit, such as a specific state or a specific census tract. Geographies at the same Geography Level cannot overlap, but Geographies at different levels can overlap. Also called the _GEOUNIT_.

## For any differentially private mechanism used to release Decennial data using the matrix mechanism:
* _(Query) Workload_ - The set of queries for which our differentially private measurements are chosen in an attempt to minimize error.
* _Query Model_ - The result of running the Query Workload on a specific geography. Also called the "measurements," the "exact data," and the "ground truth."
* _Noisy Measurements_ - The Query Model after differentially private noise has been provided. 
* _Parameterized Matrix Mechanism_ - The variant of the Matrix Mechanism that we plan to rely on as our primary differentially private mechanism. It exploits Kronecker product structure in our workloads. Previously called _Global Opt_.
* _Matrix Mechanism_ - The family of differentially private algorithms that inspired the Parameterized Matrix Mechanism. See  Chao Li, Gerome Miklau, Michael Hay, Andrew Mcgregor, and Vibhor Rastogi. 2015. The matrix mechanism: optimizing linear counting queries under differential privacy. The VLDB Journal 24, 6 (December 2015), 757-781. DOI=http://dx.doi.org/10.1007/s00778-015-0398-x. http://cs.colgate.edu/~mhay/pdfs/li2015optimizing.pdf


## For the Independent block-by-block (IBB) algorithm
* _BBE_ - The Block-by-Block Engine

## For the Fienberg/Kifer Top-Down (FKTD) algorithm
* _assignment_ - The process of assigning records from higher geographies to lower geographies
* _sub-problem_ - Each assignment operation is referred to as a sub-problem. There is 1 subproblem at the root node (e.g. US, or US+PR or a state for single-state runs) geography that assigns to each state, 52 subproblems at the state level, etc. The lowest subproblem is at the block group level of geography. (But note that hybrid mechanisms may incorporate block-level non-assignment problems.)
* _sub-problem inputs_ - The inputs to each sub-problem. The inputs are:
    * A Geography Level, and a Geography identifier
    * Differentially private records assigned to this geounit
    * List of sub-geographies
    * Noisy Measurements for each sub-geography
* _sub-problem engine_ - A python function that solves a sub-problem, typically performing some form of optimization-based post-processing to perform assignment.
* _Child Geounits_ - The geounits at the next lower Geography Level that are contained by a specific Geography. Also referred as to as _Child Nodes_.
* _FKE_ - The Fienberg/Kifer Engine.

## Database Reconstruction and Reidentification Project 
* HDF --- The original 2010 HDF
* reconHDF --- The reconstructed HDF


## Other words
* _DAS Framework_ - A framework that the Census Bureau has developed for writing disclosure avoidance systems. The Framework provides a unified logging facility and provisions for running multiple disclosure avoidance experiments. The independent block-by-block implementation for the 2018 end-to-end test is written directly using the DAS Framework.
* _Top-Down Framework_ (TDF) - A framework, built on top of the DAS Framework, for writing top-down algorithms. 
* _Apache Spark_ - The big data framework we are using.
* _SparkSQL_ - An extension to Spark that allows processing of SQL queries.
* _SQL_ - The Structured Query Language, a standard way of representing queries on databases.
* _Gurobi_ - A commercial optimizer, named after the three founders of the company. Gurobi includes implementations of a number of optimization algorithms, the most important of which for the DAS are its linearly constrained barrier/convex optimizer, its primal/dual linear programming simplex algorithms, and its mixed-integer linear programming branch-and-cut solver. Gurobi is typically orders of magnitude faster and somewhat more memory efficient than open-source alternatives, although for general mixed-integer linear programs problem variance is common and makes this advantage less predictable.
* _Gurobi Tokens_ - Implement a particular version of the Gurobi licensing model suitable for use in enterprise settings where rights to use of the optimizer must often be moved from machine to machine. The DAS team currently has access to 1,800 Gurobi Tokens made available through an agreement with the Mojo team, as distributed from the Mojo server in the research Virtual Private Cloud; an unknown number of tokens (or some other license type) may also be available through the TI team, and 1 additional Mojo token is available from research2. Each Gurobi optimizer instance requires 1 token per process actively invoking the Gurobi solver. Thus, we can run 1,800 Gurobi instances simultaneously (or 1,801 by also using the research2 license); each instance may use arbitrarily many cores (but the improvement this will garner is problem and algorithm dependent).
* _GLPK_ - The GNU Linear Programming Kit, an open source mixed-integer and continuous linear programming optimizer. It contains implementations of the branch-and-cut and simplex algorithms available in Gurobi.


# Words to avoid 
We do not use the following words:
* _adjustment_ - "population adjustment" was the term that was used in the 1990s for the proposed count changes to the 2000 census based on statistical modeling to adjust for the undercount. In (Department of Commerce v. United States House (98-404))(https://www.law.cornell.edu/supremecourt/text/525/326), the US Supreme Court ruled that the 2000 Census had to be based on an actual count, and could not be statistically adjusted. 
* _synthetic people_ - because we are not synthesizing people, we are synthesizing records.
* _color_ - We do not use the word "color" to refer to race.


<-- Ignore what follows
 LocalWords:  DAS Gurobi
     eval: (visual-line-mode t)
 End:
-->
