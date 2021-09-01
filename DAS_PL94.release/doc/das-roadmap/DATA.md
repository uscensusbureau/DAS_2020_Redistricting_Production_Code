# Getting Data

The following public datasets are available for use with the disclosure avoidance system under development.

* <https://github.com/labordynamicsinstitute/SynUSpopulation> - Synthetic US Population created by William Sexton. Simply a copy of the American Community Survey public use microdata samples, with each household replicated _n_ times according to its weight. 
* Old Census Data (see below)

## Getting Old Census Data

Because data from the US Census is released to the public after 72
years, data from the 1940 Census can be used without concern for
confidentiality. This makes the data excellent for testing the 2020
Disclosure Avoidance System.  The data is available from
[IPUMS](https://ipums.org), part of the Minnesota Population Center at
the University of Minnesota. 

IPUMS has a large collection of curated, linked datasets that can be
freely downloaded. Because these datasets are large and quite
detailed, the IPUMS website allows you to specify the specific
variables that you wish to download, and allows you to generate and download a specific set of linked data. This flexibility is not needed for
testing the 2020 DAS and the IPUMS interface can cause confusion for
those who are not familiar with it. For this reason, we have developed
the following step-by-step procedure for using IPUMS. Please provide
us with any questions that you may have. 

Census data is available for the years 1850, 1880, 1900, 1910, 1920, 1930 and 1940. The creation of the data sets was supported by the NIH, NSF, Ancestry, Stat/Transfer and the University of Minnesota. The license agreement with Ancestry prohibits IPUMS from distributing the names of the individuals in these data sets. However, that doesn't matter for our purposes.

The data that we are interested is part of the *IPUMS USA* collection. Downloads from IPUMS are free, but you need to create an account. Please note that it is prohibited to redistribute the IPUMS USA data; instead.
To access the collection, follow these steps:

1. Go to <https://usa.ipums.org/usa/> and click Login.
2. Click __Create an account__ to create your account. Enter your information, including a valid email address. The approval process is not instantaneous and does require a valid "general research statement.""
3. You will receive an email confirmation. Click on the link.
4. Once your account is approved, go back to <https://usa.ipums.org/usa> and  log in.
5. Click on the **SELECT DATA** menu option. You will now see this interface:
![ipums interface](images/ipums1.png)
6. Click the button that says **SELECT SAMPLES**. 
7. Select the tab that says **USA SAMPLES** and uncheck every box. You can probably do this by unchecking the box **Default sample from each year**.
8. Select the tab that says **USA FULL COUNT**, select the year you want (e.g. 1920 or 1940) and click **SUBMIT SAMPLE SELECTIONS**:
![ipums interface - USA Full Count](images/ipums2.png)
8. Your **DATA CART** will now indicate that you have **0 VARIABLES** and **1 SAMPLE** selected. It is now necessary to select _which* of the variables from the Census sample you wish to work with. We require both Household variables and Person variables. Select **HOUSEHOLD** underneath the banner **SELECT HARMONIZED VARIABLES**:
![ipums interface - USA Full Count](images/ipums3.png)
9. The Household variables are divided into the categories Technical; Geographic; Group Quarters; Economic Characteristic; Dwelling Characteristic; Appliances, Mechanical, Other; Household Composition; Historical Oversample; Historical Technical; and 1970 Neighborhood. We want variables from many of these categories, so we will go through them one-by-one.  
10. Select **TECHNICAL** underneath the **HOUSEHOLD** pulldown menu and you will see the interface below.  A box with a checkmark (e.g. ☑) indicates that the variable is selected; a circle with a plus sign in it (⊕) is a button that selects a variable. Select the following variables: **YEAR**, which we don't strictly need but which is useful for error checking. It's also preselected and cannot be unselected. SERIAL is preselected; it is the linkage variable between households and people. You can unselect NUMPREC, SUBSAMP, HHWT, NUMPERHH and HHTYPE, as we won't be using them:
![ipums interface - fully expanded](images/ipums4.png)
10. Select the **GEOGRAPHIC** pulldown from the **HOUSEHOLD** menu and select the following variables: STATEFIP, COUNTYF. 
11. Select the **GROUP QUARTERS** pulldown from the **HOUSEHOLDS** menu and select the following variables: GQ, GQTYPE.
12. Select the **ECONOMIC CHARACTERISTIC** pulldown from the **HOUSEHOLDS** menu and select the following variables: OWNERSHP.
14. Select the **HISTORICAL TECHNICAL** pulldown from the **HOUSEHOLDS** menu and select the following variables: ENUMDIST. (Note: we will use Enumeration district as an analog for Census Block Groups. We will combine them to synthesize Census Tracts and split them to synthesize blocks.)
15. Now we are going to add the **PERSON** variables. Select **TECHNICAL** from the **PERSON** pull-down and add the following variables: PERNUM. You can uncheck PERWT and SLWT.
16. Select **DEMOGRAPHIC** from the **PERSON** pull-down and add the following variables: RELATE, SEX, AGE
17. Select **RACE, ETHNICITY AND NATIVITY** from the **PERSON** pull-down and add the following variables: RACE, HISPAN.
18. Here are all the variables we selected:
<table>
<tr><th>Page</th><th><Variables></th></tr>
<tr><td>HOUSEHOLD/TECHNICAL</td><td>YEAR</td>
<tr><td>HOUSEHOLD/GEOGRAPHIC</td><td>STATEFIP, COUNTY</td>
<tr><td>HOUSEHOLD/GROUP QUARTERS</td><td>GQ, GQTYPE</td>
<tr><td>HOUSEHOLD/ECONOMIC CHARACTERISTICS</td><td>OWNERSHIP</td>
<tr><td>HOUSEHOLD/HISTORIC TECHNICAL</td><td>ENUMDIST</td>
<tr><td>PERSON/TECHNICAL</td><td>PERNUM</td></tr>
<tr><td>PERSON/DEMOGRAPHIC</td><td>RELATE, SEX, AGE</td></tr>
<tr><td>PERSON/RACE, ETHNICITY, AND NATIVITY</td><td>RACE, HISPAN</td></tr>
</table>
18. At this point you should have 16 variables selected in 1 sample. Click **VIEW CART** and you will see that there are two file types selected, a Household File and a Person file, and the distribution of variables in each file. Note that there is no linkage variable. Instead, you will be downloading a single file that contains a Household record followed by the Persons in the household, and then another Household record:
![ipums interface - cart](images/ipums5.png)
19. Click **CREATE DATA EXTRACT**:
![ipums interface - fully expanded](images/ipums6.png)
20. Click **Change** on the line that says **DATA FORMAT** and specify that you want a `Fixed-width text (.dat) file` and that you want the data Structure to be `Hierarchical`:
![ipums interface - cart](images/ipums-data-structure.png)
20. We do not need the data quality flags or to select specific cases. Give this extract a name, such as "1940 Census hierarchical extract" and click **SUBMIT EXTRACT**. You will be asked to agree to an additional terms.
![ipums interface - cart](images/ipums-structure.png)
21. The IPUMS USA server is now creating your extract. They typically take a few hours to be created:
![ipums interface - cart](images/ipums6.png)
22. Click **SAS** to download the SAS file that will decode this dataset. You can use the `census_etl` tool, currently under development, to read this SAS file and the downloaded dataset to transform the downloaded dataset into two CSV files, an SQLite3 database, or an Apache Parquet file.

Once you have downloaded the file, you can create a Person table and a Household table using the `ipums_etl.py` program located in the https://github.com/dpcomp-org/census-etl repository. Assuming the files that you download are usa_0001.sas.txt and usa_0001.dat.gz, use the following command:

    $ python3 ipums_etl.py usa_00001.sas.txt usa_00001.dat.gz
    
