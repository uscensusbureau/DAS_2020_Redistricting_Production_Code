# Design 2.0

This document describes a proposal for a parallelized Disclosure
Avoidance System for the 2018 end-to-end test and the 2020
Decennial Census. This is a second-generation design that is intended to take
into account what was learned from previous design based on Python
and Apache Spark.

Note: the vocabulary in this file needs to be unified with the vocabulary in [the glossary](glossary.md).

## Design Goals

Simple design and implementation; understandable and maintainable by
multiple members of the team. (Even if all members of the team
couldn't create it.)

Highly factored, modular design, to allow for pluggable
implementations and extensive unit tests.

Greater control over use of Gurobi, particularly token check-out and check-in and model construction workflow.

Leverage Spark for what it's good for (parallelized data operations), avoid it for what it is bad at (network analysis). 

Scheduling of worker nodes. Make sure that all of the worker nodes are fully utilized, and that we can increase the performance of the system by adding more workers. 

Fault tolerance. If a worker fails, no work is lost. 

Data placement - allow for data to be located directly on each node, or in a common store. (Ideally large constant data is replicated on every node.)

Logging/collecting. Fine level control over logs, including:

* Progress logs.
* Error detection logs.
* Log analysis, so that errors are handled properly.

Viewing job statistics to identify stragglers.  

Run with or without Amazon. It would be nice to be able to run on a reduced environment. 

Best practices for differential privacy is to separate noisy measurement from the post-processing. So we implement a two step process:

* Step 1:
What we need are first to generate noisy measurements for every geolevel. This needs to be separate and small (in terms of code) so it can be verified.
This also needs to do an automated check of sensitivity - check that the sensitivity is correct for the set of measurements taken over all geounits in each geolevel, and check that the geounits do indeed form a tree with the appropriate number of levels (levels is max number of nodes encountered on any path from root to leaf). Sensitivity = number of levels times the maximum sensitivity for the set of measurements taken at each geolevel.

* Step 2:
At this point all of the exact, noninvariant measurements have been garbage collected. Only the noisy measurements and invariants are passed to the optimizer, which does top-down optimization runs, first creating US level people, then state, etc.

There needs to be some flexibility for defining new data structures that producers can produce and consumers can consume.

## Design Question #1: Allocation of records to lower geounits in the top-down algorithm

**PHIL: What was the design question here?**

## Design Question #2 Calculation and storage of Noisy Measurements
The naïve implementation of the top-down algorithm passes both *Exact Measurements* and the from high geounits to lower geounits and the *Noisy Person Histogram*. The problem with this approach is that it does not afford a strong separation between the real data and the protected data. To create this separation, it is desirable to compute the *Exact Measurements* first, and then the perform the allocation of the records in the *Noisy US Person Histogram*. **PHIL: in our current code, this separation is stronger even than this; the exact measurements are discarded and replaced with noisy measurements for every geounit in every geolevel before the geoassignment portion of the code even begins. I think it's desirable to maintain that strong separation, as it will help reviewers of the code not have to worry about the geoassignment code possibly affecting the algorithm's privacy guarantee.**

**Calculating the Noisy Measurements**. To calculate the noisy measurements, a set of statistical functions must be run over the microdata for every *Geounit.* It may be possible to compute the statistics efficiently for a higher geography by combining the values for a lower geography, but this would require a mathematical proof for each statistic: it would be safer to simply recalculate it. The calculation of these statistics can be done efficiently with Apache Spark: this is the sort of computation at which Spark excels. **PHIL: All of our exact measurements, invariants, and the data necessary for our derived constraints can be derived by summing statistics over lower geounits to derive the analogous statistics for higher geounits. Every statistic we currently care about respects the relevant superlinearity property. This is not true for the noisy measurements, though; those have to be generated independently for each geounit in each geolevel.**

Once the noisy measurements are created, they need to be stored somewhere. In a pure Spark implementation, they can be stored in a dataframe, which will cause them to be recalculated as necessary (for example, if there is a memory exhaustion or a node failure).

## Open Question #3: Household Formation
How do we represent household queries? For example, how to we represent a block that has four people living in two households, on which has a pair of of married women, the other of which has a pair of married men? **PHIL: There are many ways to do this, but we haven't settled on an ideal way. I have an extensive Overleaf document that carefully works through the attributes we would need to represent household tabulations. Because the scale of the naturally constructed histograms becomes enormous, we were putting off addressing the issue of household query and histogram representation until we had successfully dealt with PL94 and possibly the larger SF-1 demographics histogram.**


## Data Types and Structures

Below are the data types and structures that will be used in the design. Since we are expecting to implement this in Python, each of these will be implemented as a Python class. We will *not* simply pass around dictionaries or lists, because those cannot be type-checked.  

In this section, it is envisioned that each of the names in bold will become a Python class. "attr:" specifies a python attribute that the class will have.

*String Constants* --- Throughout this specification (and in the implementation), we make liberal use of string constants. Instead of having quoted strings in the source code, it is envisioned that there will be a python module called `constants.py` that will be imported into every module and that will contain all string constants. By having all string constants in a single module, we will avoid typos in quoted strings that might be difficult to track down.

~~~~
SEX='sex'
MALE='M'
FEMALE='F'

AGE='age'
RACE='race'
ETHNICITY='ethnicity'
HISPANIC='H'
NOT_HISPANIC='N'
RELATIONSHIP='relationship'

US='US'
STATE='state'
COUNTY='county'
TRACT='tract'
BLOCKGROUP='blockgroup'
BLOCK='block'
CONGRESSIONALDISTRICT='congressional district'
UPPERLEGISLATIVECHAMBER='upper state legislative chamber'
LOWERLEGISLATIVECHAMBER='lower state legislative chamber'
~~~~

**Geolevel** --- Describes which geolevel we are working at. The *Geolevel* class allows us to create implementations that can implement operations at an arbitrary geolevel.

    * attr: name. One of [COUNTRY, STATE, COUNTY, TRACT, BLOCKGROUP, BLOCK, CONGRESSIONALDISTRICT, UPPERLEGISLATIVECHAMBER, LOWERLEGISLATIVECHAMBER]

    * Data Source: Python configuration file 

    * method: `.higher()`

    * method: `.lower()`

**Geounit** --- an identifier that uniquely maps to a Geounit. A Geounit might be the State of Maryland or a specific block.

    * attr: geocode: String --- 1-to-16 character Census geocode for the referenced place.

    * attr: geolevel: Geolevel --- The geolevel of the geocode. It is likely possible to derive the geography from the geocode. 

    * attr: np: Numpy Multiarray --- Exact measurements for individual cells of the histogram associated with this geounit. Set to None when noisy measurements are set.

    * attr: dp: Numpy Multiarray --- Noisy measurements for this geounit. If this is not None, then np is None.

    * attr: cons: Dictionary --- Constraints information for this geounit.

    * attr: invars: Dictionary --- Invariants information for this geounit.

    * attr: congDistGeocode: String --- unique identifier for the congressional district in which this geounit lies. Only non-None for block-level geounits.

    * attr: parentGeocode: String --- substring of geocode that corresponds to this node's parent, or None if this geounit is a congressional district, upper legislative chamber district, or lower legislative chamber district.

    * method: repr --- Representation method so that geounits can be reasonably pretty printed.

    * method: init --- Constructor. Initializes values of most attributes.

    * method: setParentGeocode --- Computes parent geocode from full geocode.

    * method setCongDistGeocode --- Sets congressional district geocode from passed input argument.

**Set of All Geounits** --- The list of all geounits at all geolevels. This will most likely be used in testing. In production, it will be read from the CEF.**PHIL: Is this intended to be a list or a set or something else? The language seems to waffle back and forth. Also, why the uncertainty about whether all geounits will be used in testing, given that we've already used all geounits in testing and intend to continue doing so?**

    * Size: 1 country + 52 state-like regions + 3007 counties + 66,438 census tracts + 211,827 block groups + 8,269,131 blocks = 8,550,456 specific geounits.**PHIL: This calculation is missing the number of congressional districts and upper/lower state legislative chamber districts. Not sure how many there are of each.**

    * Reference: https://www.census.gov/geo/maps-data/data/tallies/tabgeo2k.html

    * Source: The list of geounits will be created from a function or location specified in the configuration file. **PHIL: Is there a reason we want to specify the set of geounits in the config file? With our current workflow we infer the set of (occupied) geounits from the provided microdata, which would work fine for the CEF as well. Are we expecting someone will pass us a separate list of the geounits as input? How do we get the list of geounits into the config file in the first place?**


**Person Attribute** ---  Describes a specific attribute. The *Person Attribute* class allows us to create implementations that can execute on arbitrary attributes.

  * attr: name. One of [AGE, SEX, RACE, ETHNICITY, RELATIONSHIP]

**Specific Attribute** --- This is a specific value for an attribute. 

  * attr: name  : Person Attribute
  * attr: value : Integer

Example: `(name=AGE, value=18)`

**Possible Attributes** - set of all possible attributes. Includes:

* Possible Ages: (AGE,0) .. (AGE,115)

* Possible Sexes: (SEX,MALE)  (SEX,FEMALE) 

* Possible Races: (RACE, cenrace_variable)

* Possible Ethnicities: (ETHNICITY, HISPANIC) (ETHNICITY, NOT_HISPANIC)

* Possible Relationships: (RELATIONSHIP,1) .. (RELATIONSHIP,17)

**PersonHistogram** - A multi-dimensional histogram of person counts by attribute. 

    * attr: hist: Numpy Multiarray --- A description of the records allocated to the geounit. **PHIL: the exact records or the post-processed, formally private microdata?**
    * attr: geounit: Geounit --- The geographical location described by the histogram.**PHIL: This class is somewhat confusing. Would it not be more natural to attach the PersonHistogram as an attribute of the Geounit rather than the Geounit as an attribute of the PersonHistogram? It seems odd to think of a PersonHistogram as containing anything other than the histogram itself. That is essentially what we do now; we have a geounitNode class that is essentially identical to the geounit class in this document, and it includes in particular an attribute for the exact data histogram, the dp data histogram, and the final synthetic/postprocessed data histogram.**

In the operation of the top-down algorithm, there are two US level *Person Histograms:* the *Real US Histogram* and the *Noisy US Histogram*.

**Measurement** - The name of something that is measured

* slot Name:  e.g. "Male-Female Ratio" **PHIL: What does 'slot' refer to?**

* slot SQL:   e.g. "(select count(1) from TABLE where SEX='M') / (select count(1) from TABLE where SEX='F')

* slot func:  e.g. a function that computes the measurement on a numpy multiarray

* slot STATUS: EXACT or NOISY **PHIL: Add another level for postprocessed?**

**MeasurementsForGeounit** - A specific set of measurements for a histogram and a geounit

* slot Measurements - a set of measurements

* slot geounit: Geounit - the geography for these measurements

* slot STATUS: Exact or Noisy (determined by examining the set of Measurements). **PHIL: Add another level for postprocessed?**

**Task**

**MeasurementTask**

**OptimizerTask** - Describes what the optimizer is supposed to do. 
Describes what the optimizer will be called to do. If InputGeography==OutputGeography, then this is performing the block-by-block algorithm. 

* attr: InputGeounit: Geounit

* attr: OutputGeounits: 1 or more Geounits

The data object will respect the following constraints:

* constraint 1: if InputGeounit != OutputGeounit, then the output geounit must be at the next lower geolevel if we are using the top-down mechanism.

* constraint 2: The geolevel of all the geounits in OutputGeounits set must be the same.

* constraint 3: The geounits for all of the elements in OutputGeounits must be distinct


## Functional Elements
This section describes the programming modules that are used to create the 2.0 system.

**Main** - starts up system

**Geounits Generator** 

* Input1: Census Edited File Geounits Table
* Input2: Requested *Geolevel*
* Input3: Restrictions on the *Geounit* objects that are generated. For example, must have a specific county.
* Output1: A set of *Geounit* objects. 

Possible implementation 1: All of the geolevels are stored in a file and it is linearly scanned from beginning to end. 

Possible implementation 2: The CEF can be transformed into a read-only SQLite3 database and the desired geolevels are generated by an SQL query.

Possible implementation 3: The geolevels are directly constructed via RDD methods from the geocodes present in the CEF microdata.

**Histogram Generator**
* Input1: A *Geounit*
* Input2: Records from the Census Edited File Person Table that contain the *Geounit*
* Output1: a *PersonHistogram*

**Measurement Generator** - Reads all raw data and generates measurements.

* Input3: *Geounit*
* Input1: Records from the Census Edited File Person Table that contain the *Geounit*
* Output1: *Geounit*
* Output1: Exact Measurement for the *Geounit*

  * Storage Option 1: Stored in a key/value database where the key is the geounit. **PHIL: This is how the current code works, but using geocode as a key in (key,value) pairs where this instead refers to geounit.**
  * Storage Option 2: Pass the measurements through the OptimizerTasks and have transformation from Measurements to Noisy Measurements be performed prior to the optimization being performed. This is easier to build, but harder to validate the privacy protection. 

**Measurement Noiser** - Applies noise to a set of measurements using a specified differentially private mechanism.

* Input1: A set of Exact Measurements, for a geounit.

* Input2: A set of Invariants, the measurements that may not receive noise.

* Output1: A set of Noisy Measurements, for the same geolevel.

* Output2: The set of Invariants.

* Output3: The measurement for each Invariant. **PHIL: What is the distinction between an Invariant and the measurement for an Invariant?**

 
**Work Queue**  - List of all outstanding OptimizerTasks

* This can be implemented with Amazon's Simple Queue Service (SQS).

* It can alternatively be implemented as a single directory that files, where each file is an OptimizerTask. **PHIL: A single directory that files? What does that mean?**

* To provide for reliability, a Worker can lock an OptimizerTask so that no other workers can obtain it, and then if the worker crashes, the lock is released. (This is the basic functionality provided by SQS. If we create a simulator, it will need to be implemented locally.)

**Worker** - The worker gets an *OptimizerTask* from the *Work Queue*, performs the work, stores the results (possibly creating a new OptimizerTask), and then removes the work from the work queue.

* **Open Questions for the Worker**:
   * Should the worker transform the Measurement into the Noisy Measurement? **PHIL: No. The application of noise to the exact measurements should be done for every geounit in every geolevel in a step completely separate from and prior to any of the geoassignment that occurs inside the workers. That is how the current code is written, and it is the level of separation we want between these two tasks in order to make privacy arguments about the code straightforward.**

# Services required
The following services are required:




# Implementing our algorithms with this design

## Block-by-Block Algorithm

1. Main module starts up
2. The *Histogram Generator* is used to create a *PersonHistogram* for each block.
3. The *Geolevels Generator* is used to create a set of all geounits for the BLOCK level.
4. The *Measurement Generator* is used to create a set of measurements for each block.
5. The *Measurement Noiser* is used to transform every block measurement into a noisy measurement.
5. An *OptimizerTask* is created for each BLOCK. The task has as input the Geounit, the PersonHistogram for the block, and the NoisyMeasurement for each block. 
6. *Workers* are run until the WorkQueue is empty.
7. The MDF is created by combining the output of the Workers. 

The block-by-block Worker does the following with each Optimizer Task:

## Top-down Algorithm
1. Main module starts up
2. The *Histogram Generator* is used to create a *PersonHistogram* for the country. 
3. The *Geolevels Generator* is used to create the Geounit for the country.
4. The *Measurement Generator* is used to create the Measurement for the country. 
5. A single OptimizerTask is created and put on the WorkQueue. 
6. The WorkQueue runs.

**PHIL: There is a missing step here. Before an optimizer can run to perform geoassignment of DP people from US to states (steps 5-6 above), an additional optimization step must first be carried out to estimate from the DP all-US measurements a consistent set of DP microdata at the national level that respects all constraints.**

The top-down Worker does the following with each Optimizer Task:

1. Read the *OptimizerTask*, which contains a *Geounit*, a *PersonHistogram* and a *Measurement*.
2. Run the *Measurement Noiser* to transform the measurements into noisy measurements. 
3. Run the *Geolevels Generator* to get a list of all of the *Geounits* that are contained within this *Geolevel*. For example, get the list of counties for a specific state. These are the *Child Geounits*.
4. Run the *Measurement Generator* to create a measurement for each of the *Child Geounits*. **PHIL: this should not be interleaved with the geoassignment optimization step like this. This is what we most recently refactored our code to avoid doing.**
5. Run the Optimizer to create a *Person Histogram* for each *Child Geounit*.
6. Write each *Person Histogram* to the output store.
6. If the *Child Geography* is not the terminal geography, create an *OptimizerTask* for each child geounit and put each one on the *Work Queue*.

 

# Implementation Plan

