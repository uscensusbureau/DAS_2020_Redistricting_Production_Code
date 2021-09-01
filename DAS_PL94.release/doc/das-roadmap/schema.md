This file presents a SQL schema for the 2020 Census Edited File (CEF)
and sample queries on that file to be optimized by the 2020 Disclosure
Avoidance System.

Where possible, variable names are consistent with the US Census
Metadata Registry (MDR).

## Tables

The PersonData table is taken from the MDF specification.
`count` is 1 if each row represents a person, and >1 if this is a
histogram representation. Note that any SQL statement that assumes
histogram will work on a person representation.


Here are the data tables from the 1940 Census:
```
CREATE TABLE H (
   RECTYPE VARCHAR(1) -- Record type,
   YEAR VARCHAR(4) -- Census year,
   DATANUM VARCHAR(2) -- Data set number,
   SERIAL VARCHAR(8) -- Household serial number,
   HHWT VARCHAR(10) -- Household weight,
   STATEFIP VARCHAR(2) -- State (FIPS code),
   COUNTY VARCHAR(4) -- County,
   GQ VARCHAR(1) -- Group quarters status,
   GQTYPE VARCHAR(1) -- Group quarters type [general version],
   GQTYPED VARCHAR(3) -- Group quarters type [detailed version],
   OWNERSHP VARCHAR(1) -- Ownership of dwelling (tenure) [general version],
   OWNERSHPD VARCHAR(2) -- Ownership of dwelling (tenure) [detailed version],
   ENUMDIST VARCHAR(4) -- Enumeration district
);
CREATE TABLE P (
   RECTYPE VARCHAR(1) -- Record type,
   YEAR VARCHAR(4) -- Census year,
   DATANUM VARCHAR(2) -- Data set number,
   SERIAL VARCHAR(8) -- Household serial number,
   PERNUM VARCHAR(4) -- Person number in sample unit,
   PERWT VARCHAR(10) -- Person weight,
   SLWT VARCHAR(10) -- Sample-line weight,
   RELATE VARCHAR(2) -- Relationship to household head [general version],
   RELATED VARCHAR(4) -- Relationship to household head [detailed version],
   SEX VARCHAR(1) -- Sex,
   AGE VARCHAR(3) -- Age,
   RACE VARCHAR(1) -- Race [general version],
   RACED VARCHAR(3) -- Race [detailed version],
   HISPAN VARCHAR(1) -- Hispanic origin [general version],
   HISPAND VARCHAR(3) -- Hispanic origin [detailed version]
);
```

Here is an approximation of the current PersonData table:

```
create table PersonData (
    TABBLKST CHAR(2),   -- 2018 Tabulation State (FIPS Code)
    TABBLKCOU CHAR(3),  -- 2018 Tabulation County (FIPS)
    TABTRACTCE CHAR(6), -- 2018 Tabulation Census Tract
    TABBLKGRPCE CHAR(1),-- 2018 Census Block Group
    TABBLK      CHAR(4),-- 2018 Block Number
    QREL   CHAR(2),       -- Relationship
    QSEX   CHAR(1),       
    QAGE INTEGER(3),
    CENRACE CHAR(3), 
    HISP    CHAR(1),     -- Hispanic
    count   INTEGER      -- used for representing histograms
);
```

Let's say you have two blocks. Block #1 has 4 white men that are age
10, 15, 20 and 25. Block #2 has 4 white men aged 20 and 4 block men
aged 50, 51, 52 and 53.  The database would look like this:

```
TABBLKST TABBLKCOU  TABTRACTCE TABBLKGRPCE TABBLK QREL QSEX QAGE CENRACE HISP count
   10       100        3001         1        1     1    1    10     01    0     1
   10       100        3001         1        1     1    1    15     01    0     1
   10       100        3001         1        1     1    1    20     01    0     1
   10       100        3001         1        1     1    1    25     01    0     1
   10       100        3001         1        2     1    1    20     01    0     4
   10       100        3001         1        2     1    1    50     02    0     1
   10       100        3001         1        2     1    1    51     02    0     1
   10       100        3001         1        2     1    1    52     02    0     1
   10       100        3001         1        2     1    1    53     02    0     1
```


## Sample queries

Calculate the total population of all blocks, by block:

   SELECT TABBLKST,TABBLKCOU,TABTRACTCE,TABBLKGRPCE,TABBLK,SUM(count)
   from PersonData 
   GROUP BY TABBLKST,TABBLKCOU,TABTRACTCE,TABBLKGRPCE,TABBLK;

Note: You can replace `SUM(count)` with `COUNT(*)` if you know that
you will only have records in the database that are person-level
records, and not histogram records. In practice, we will try to avoid this.

Calculate voting age population of all blocks, by block:

   SELECT TABBLKST,TABBLKCOU,TABTRACTCE,TABBLKGRPCE,TABBLK,SUM(count)
   from PersonData 
   WHERE QAGE>=18
   GROUP BY TABBLKST,TABBLKCOU,TABTRACTCE,TABBLKGRPCE,TABBLK;


For state demographers, calculate the age pyramid (number of men and
women at each age) by county for all counties:

   SELECT TABBLKST,TABBLKCOU,QAGE,QSEX,SUM(count)
   from PersonData 
   GROUP BY TABBLKST,TABBLKCOU,QAGE,QSEX
   ORDER BY 1,2,3,4,5;        -- Ordering is not required but it's friendly

   

## Constraint Language
Constraints (invariants) are currently represented by taking slices on a NumPy histogram.

Constraints can be described as a pair of SQL statements that must
return the same values, where the first SQL statement operates on the
CEF and the second SQL statement operates on the MDF.  

This SQL statement counts the number of people in the US:

    SELECT COUNT(*) FROM PersonData;

SQL allows the specification of a database by prefixing the table with the database name and a period. 
Therefore,  the constraint that no people can be added or deleted could be described with this:

    (SELECT COUNT(*) FROM CEF.PersonData) == (SELECT COUNT(*) FROM MDF.PersonData);

The constraint that the count for each state cannot be changed can be represented as:

    (SELECT TABBLKST,COUNT(*) FROM CEF.PersonData GROUP BY TABBLKST) == 
    (SELECT TABBLKST,COUNT(*) FROM MDF.PersonData GROUP BY TABBLKST);

The constraint that the count for each block cannot be changed can be represented as:

    (SELECT TABBLKST,TABBLKCOU,TABTRACTCE,TABBLKGRPCE,TABBLK,COUNT(*) FROM 
            CEF.PersonData GROUP BY TABBLKST,TABBLKCOU,TABTRACTCE,TABBLKGRPCE,TABBLK) == 
    (SELECT TABBLKST,TABBLKCOU,TABTRACTCE,TABBLKGRPCE,TABBLK,COUNT(*) FROM
            MDF.PersonData GROUP BY TABBLKST,TABBLKCOU,TABTRACTCE,TABBLKGRPCE,TABBLK)

From these constraints, we could readily generate the specific slices required in the NumPy histogram. Our logic that does this does not have to be general, but can be crafted to the specific SQL statements that are developing. The advantage of using SQL is that we can also run the statements on a database that holds the entire CEF and MDF and double-check our work.

