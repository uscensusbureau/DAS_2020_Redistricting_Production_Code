# Goal: To determine feasibility of running TopDown to produce microdata that supports tabulation of the fully specified DHC.

Output should match full 2020 MDF specs.

All variables subject to change.

## DHC-P schema requirements:
* RELGQ (42)
    * 0 = Householder living alone
    * 1= Householder not living alone
    * 2 = Opposite-sex spouse
    * 3 = Opposite-sex unmarried partner
    * 4 = Same-sex spouse (Use allocation flag to determine)
    * 5 = Same-sex unmarried partner
    * 6 = Biological son or daughter
    * 7 = Adopted son or daughter
    * 8 =Stepson or stepdaughter
    * 9 = Brother or sister
    * 10 = Father or mother
    * 11 = Grandchild
    * 12 = Parent-in-law
    * 13 = Son-in-law or daughter-in-law
    * 14 = Other relative
    * 15 = Roommate or housemate
    * 16 = Foster child (If roomer/boarder and age<21)
    * 17 = Other nonrelative (also include if roomer/boarder and age>=21)
    * 18 = GQ 101 Federal detention centers
    * 19 = GQ 102 Federal prisons
    * 20 = GQ 103 State prisons
    * 21 = GQ 104 Local jails and other municipal confinements
    * 22 = GQ 105 Correctional residential facilities
    * 23 = GQ 106 Military disciplinary barracks
    * 24 = GQ 201 Group homes for juveniles
    * 25 = GQ 202 Residential treatment centers for juveniles
    * 26 = GQ 203 Correctional facilities intended for juveniles
    * 27 = GQ 301 Nursing facilities
    * 28 = GQ 401 Mental hospitals
    * 29 = GQ 402 Hospitals with patients who have no usual home elsewhere
    * 30 = GQ 403 In-patient hospice facilities
    * 31 = GQ 404 Military treatment facilities
    * 32 = GQ 405 Residential schools for people with disabilities
    * 33 = GQ 501 College/university student housing
    * 34 = GQ 601 Military quarters
    * 35 = GQ 602 Military ships
    * 36 = GQ 701 Emergency and transitional shelters
    * 37 = GQ 801 Group homes intended for adults
    * 38 = GQ 802 Residential treatment centers for adults
    * 39 = GQ 900 Maritime/merchant vessels
    * 40 = GQ 901 Workers' group living quarters and job corps centers
    * 41 = GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)

* SEX (2)
    * 0=Male
    * 1=Female

* AGE (116)
    * 0, 1, 2, ..., 115

* HISP (2)
    * 0=Not Hispanic
    * 1=Hispanic

* CENRACE (63) [Standard cenrace var]

Histogram size: 1,227,744

## DHC-H schema requirements:
* HHSEX (2)
    * 0=Male
    * 1=Female 

* HHAGE (9)
    * 0=15-24
    * 1=25-34
    * 2=35-44
    * 3=45-54
    * 4=55-59
    * 5=60-64
    * 6=65-74
    * 7=75-84
    * 8=85+

*HHHISP (2)
    * 0=Not Hispanic
    * 1=Hispanic

* HHRACE (7)
    * 0=White alone
    * 1=Black alone
    * 2=Asian alone
    * 3=AIAN alone
    * 4=NHPI alone
    * 5=SOR alone
    * 6=Two or more races

* Elderly (4)
    * 0=No one over 60 in the household
    * 1=At least one person 60 or older, no one over 65.
    * 2=At least one person 65 or older, no one over 75
    * 3=At least one person 75 or older

* Tenure (4)
    * Owned with a mortgage
    * Owned free and clear
    * Rented
    * Occupied without payment of rent

### HHTYPE
Need to setup some notation and context first. HHTYPE is a flattening of the old hhtype with size, multigenerational, and two other variables from the MDF spec (PAC and P18). There are 427 values! 

For context, there are 3 distinct uses of "child" in 2020 that I'm aware of:

"own child under 18" (__oc__) means biological, adopted, or step.
"child under 18 excluding householder, spouse, unmarried partner" (__cxhhsup__) means anyone (except a householder, spouse, unmarried partner) under 18 living in a household.
"child under 18" means anyone (including householder, spouse, unmarried partner) (__c__) under 18 living in a household.

The "under 18" is sometimes split up into the categories of "under 6 yrs only", "between 6 and 17 yrs", or "in both ranges". The use of __oc__, __cxhhsup__, __c__ always refers to under 18. That is saying "no __oc__" indicates that no own children under 18 are present. There could be own children 18 or older present. Likewise saying "__oc__ under 6 yrs only" indicates that the only own children under 18 are under 6 yrs. There could also be own children 18 or older. 

Also, note that the definitions nest so an "own child" is a "child excluding householder, spouse, unmarried partner" is a "child".

In particular, if a household has "__oc__ under 6 yrs only" then the household can't have "__cxhhsup__ between 6 and 17 only". The household could either have "__cxhhsup__ under 6 only" or "__cxhhsup__ in both ranges".  These two possibilities will be defined with the following shorthand "__oc__ u6 only, no __cxhhsup__ 6to17" and "__oc__ u6 only, __cxhhsup__ both"  


* HHTYPE (522)
    * 0 = Size 3, not multig, married opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 1 = Size 4, not multig, married opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 2 = Size 5, not multig, married opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 3 = Size 6, not multig, married opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 4 = Size 7+, not multig, married opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 5 = Size 4, multig, married opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 6 = Size 5, multig, married opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 7 = Size 6, multig, married opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 8 = Size 7+, multig, married opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17

    * 9 = Size 4, not multig, married opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 10 = Size 5, not multig, married opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 11 = Size 6, not multig, married opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 12 = Size 7+, not multig, married opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 13 = Size 4, multig, married opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 14 = Size 5, multig, married opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 15 = Size 6, multig, married opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 16 = Size 7+, multig, married opposite-sex with __oc__ u6 only, __cxhhsup__ both

    * 17 = Size 3, not multig, married opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 18 = Size 4, not multig, married opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 19 = Size 5, not multig, married opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 20 = Size 6, not multig, married opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 21 = Size 7+, not multig, married opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 22 = Size 4, multig, married opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 23 = Size 5, multig, married opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 24 = Size 6, multig, married opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 25 = Size 7+, multig, married opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6

    * 26 = Size 4, not multig, married opposite-sex with __oc__ 6to17 only, __cxhhsup__ both
    * 27 = Size 5, not multig, married opposite-sex with __oc__ 6to17 only, __cxhhsup__ both
    * 28 = Size 6, not multig, married opposite-sex with __oc__ 6to17 only, __cxhhsup__ both
    * 29 = Size 7+, not multig, married opposite-sex with __oc__ 6to17 only, __cxhhsup__ both
    * 30 = Size 4, multig, married opposite-sex with __oc__ 6to17 only, __cxhhsup__ both
    * 31 =Size 5, multig, married opposite-sex with __oc__ 6to17 only, __cxhhsup__ both
    * 32 = Size 6, multig, married opposite-sex with __oc__ 6to17 only, __cxhhsup__ both
    * 33 = Size 7+, multig, married opposite-sex with __oc__ 6to17 only, __cxhhsup__ both

    * 34 = Size 4, not multig, married opposite-sex with __oc__ both
    * 35 = Size 5, not multig, married opposite-sex with __oc__ both
    * 36 = Size 6, not multig, married opposite-sex with __oc__ both
    * 37 = Size 7+, not multig, married opposite-sex with __oc__ both
    * 38 = Size 5, multig, married opposite-sex with __oc__ both
    * 39 = Size 6, multig, married opposite-sex with __oc__ both
    * 40 = Size 7+, multig, married opposite-sex with __oc__ both

    * 41 = Size 3, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ u6 only
    * 42 = Size 4, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ u6 only
    * 43 = Size 5, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ u6 only
    * 44 = Size 6, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ u6 only 
    * 45 = Size 7+, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ u6 only

    
    * 46 = Size 4, multig, married opposite-sex with no __oc__, but has __cxhhsup__ u6 only
    * 47 = Size 5, multig, married opposite-sex with no __oc__, but has __cxhhsup__ u6 only
    * 48 = Size 6, multig, married opposite-sex with no __oc__, but has __cxhhsup__ u6 only 
    * 49 = Size 7+, multig, married opposite-sex with no __oc__, but has __cxhhsup__ u6 only

    * 50 = Size 3, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ 6to17 only
    * 51 = Size 4, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ 6to17 only
    * 52 = Size 5, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ 6to17 only
    * 53 = Size 6, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ 6to17 only 
    * 54 = Size 7+, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ 6to17 only

    * 55 = Size 4, multig, married opposite-sex with no __oc__, but has __cxhhsup__ 6to17 only
    * 56 = Size 5, multig, married opposite-sex with no __oc__, but has __cxhhsup__ 6to17 only
    * 57 = Size 6, multig, married opposite-sex with no __oc__, but has __cxhhsup__ 6to17 only 
    * 58 = Size 7+, multig, married opposite-sex with no __oc__, but has __cxhhsup__ 6to17 only

    * 59 = Size 4, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ both
    * 60 = Size 5, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ both
    * 61 = Size 6, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ both 
    * 62 = Size 7+, not multig, married opposite-sex with no __oc__, but has __cxhhsup__ both

    * 63 = Size 5, multig, married opposite-sex with no __oc__, but has __cxhhsup__ both
    * 64 = Size 6, multig, married opposite-sex with no __oc__, but has __cxhhsup__ both 
    * 65 = Size 7+, multig, married opposite-sex with no __oc__, but has __cxhhsup__ both

    * 66 = Size 2, not multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__
    * 67 = Size 3, not multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__
    * 68 = Size 4, not multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__
    * 69 = Size 5, not multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__
    * 70 = Size 6, not multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__
    * 71 = Size 7+, not multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__

    * 72 = Size 4, multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__
    * 73 = Size 5,  multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__
    * 74 = Size 6,  multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__
    * 75 = Size 7+, multig, married opposite-sex with no __oc__, no __cxhhsup__, no __c__

    * 76 = Size 2, not multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__
    * 77 = Size 3, not multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__
    * 78 = Size 4, not multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__
    * 79 = Size 5, not multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__
    * 80 = Size 6, not multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__
    * 81 = Size 7+, not multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__

    * 82 = Size 4, multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__
    * 83 = Size 5, multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__
    * 84 = Size 6, multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__
    * 85 = Size 7+, multig, married opposite-sex with no __oc__, no __cxhhsup__, but has __c__

    * 86 - 171  = Repeat for married same-sex

    * 172 = Size 3, not multig, cohabiting opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 173 = Size 4, not multig, cohabiting opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 174 = Size 5, not multig, cohabiting opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 175 = Size 6, not multig, cohabiting opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 176 = Size 7+, not multig, cohabiting opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 177 = Size 4, multig, cohabiting opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 178 = Size 5, multig, cohabiting opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 179 = Size 6, multig, cohabiting opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17
    * 180 = Size 7+, multig, cohabiting opposite-sex with __oc__ u6 only, no __cxhhsup__ 6to17

    * 181 = Size 4, not multig, cohabiting opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 182 = Size 5, not multig, cohabiting opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 183 = Size 6, not multig, cohabiting opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 184 = Size 7+, not multig, cohabiting opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 185 = Size 4, multig, cohabiting opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 186 = Size 5, multig, cohabiting opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 187 = Size 6, multig, cohabiting opposite-sex with __oc__ u6 only, __cxhhsup__ both
    * 188 = Size 7+, multig, cohabiting opposite-sex with __oc__ u6 only, __cxhhsup__ both

    * 189 = Size 3, not multig, cohabiting opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 190 = Size 4, not multig, cohabiting opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 191 = Size 5, not multig, cohabiting opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 192 = Size 6, not multig, cohabiting opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 193 = Size 7+, not multig, cohabiting opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 194 = Size 4, multig, cohabiting opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 195 = Size 5, multig, cohabiting opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 196 = Size 6, multig, cohabiting opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6
    * 197 = Size 7+, multig, cohabiting opposite-sex with __oc__ 6to17 only, no __cxhhsup__ u6

    * 198 = Size 4, not multig, cohabiting opposite-sex with __oc__ 6to17 only,  __cxhhsup__ both
    * 199 = Size 5, not multig, cohabiting opposite-sex with __oc__ 6to17 only,  __cxhhsup__ both
    * 200 = Size 6, not multig, cohabiting opposite-sex with __oc__ 6to17 only,  __cxhhsup__ both
    * 201 = Size 7+, not multig, cohabiting opposite-sex with __oc__ 6to17 only,  __cxhhsup__ both
    * 202 = Size 4, multig, cohabiting opposite-sex with __oc__ 6to17 only,  __cxhhsup__ both
    * 203 = Size 5, multig, cohabiting opposite-sex with __oc__ 6to17 only,  __cxhhsup__ both
    * 204 = Size 6, multig, cohabiting opposite-sex with __oc__ 6to17 only,  __cxhhsup__ both
    * 205 = Size 7+, multig, cohabiting opposite-sex with __oc__ 6to17 only,  __cxhhsup__ both

    * 206 = Size 4, not multig, cohabiting opposite-sex with __oc__ both
    * 207 = Size 5, not multig,  cohabiting opposite-sex with __oc__ both
    * 208 = Size 6, not multig, cohabiting opposite-sex with __oc__ both
    * 209 = Size 7+, not multig, cohabiting opposite-sex with __oc__ both
    * 210 = Size 5, multig, cohabiting opposite-sex with __oc__ both
    * 211 = Size 6, multig, cohabiting opposite-sex with __oc__ both
    * 212 = Size 7+, multig, cohabiting opposite-sex with __oc__ both

    * 213 = Size 3, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 214 = Size 4, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 215 = Size 5, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 216 = Size 6, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 217 = Size 7+, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ u6 only

    * 218 = Size 4, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 219 = Size 5, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 220 = Size 6, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 221 = Size 7+, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ u6 only

    * 222 = Size 3, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 223 = Size 4, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 224 = Size 5, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 225 = Size 6, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 226 = Size 7+, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ 6to17

    * 227 = Size 4, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 228 = Size 5, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 229 = Size 6, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 230 = Size 7+, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ 6to17

    * 231 = Size 4, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ both
    * 232 = Size 5, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ both
    * 233 = Size 6, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ both
    * 234 = Size 7+, not multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ both

    * 235 = Size 5,  multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ both
    * 236 = Size 6, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ both
    * 237 = Size 7+, multig, cohabiting opposite-sex with relatives, no __oc__, but has __cxhhsup__ both

    * 238 = Size 3, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, no __c__
    * 239 = Size 4, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, no __c__
    * 240 = Size 5, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, no __c__
    * 241 = Size 6, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, no __c__
    * 242 = Size 7+, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, no __c__

    * 243 = Size 4, multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, no __c__
    * 244 = Size 5, multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, no __c__
    * 245 = Size 6, multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, no __c__
    * 246 = Size 7+, multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, no __c__

    * 247 = Size 3, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 248 = Size 4, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 249 = Size 5, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 250 = Size 6, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 251 = Size 7+, not multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, but has __c__

    * 252 = Size 4, multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 253 = Size 5, multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 254 = Size 6, multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 255 = Size 7+, multig, cohabiting opposite-sex with relatives, no __oc__, no __cxhhsup__, but has __c__

    * 256 = Size 3, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ u6 only
    * 257 = Size 4, not multig, cohabiting opposite-sex with  no relatives, no __oc__, but has __cxhhsup__ u6 only
    * 258 = Size 5, not multig, cohabiting opposite-sex with  no relatives, no __oc__, but has __cxhhsup__ u6 only
    * 259 = Size 6, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ u6 only
    * 260 = Size 7+, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ u6 only

    * 261 = Size 3, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ 6to17
    * 262 = Size 4, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ 6to17
    * 263 = Size 5, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ 6to17
    * 264 = Size 6, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ 6to17
    * 265 = Size 7+, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ 6to17

    * 266 = Size 4, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ both
    * 267 = Size 5, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ both
    * 268 = Size 6, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ both
    * 269 = Size 7+, not multig, cohabiting opposite-sex with no relatives, no __oc__, but has __cxhhsup__ both

    * 270 = Size 2, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 271 = Size 3, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 272 = Size 4, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 273 = Size 5, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 274 = Size 6, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 275 = Size 7+, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, no __c__

    * 276 = Size 2, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 277 = Size 3, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 278 = Size 4, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 279 = Size 5, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 280 = Size 6, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 281 = Size 7+, not multig, cohabiting opposite-sex with no relatives, no __oc__, no __cxhhsup__, but has __c__

    * 282 - 391 = Repeat for cohabiting same-sex

    * 392 = Size 1, not multig, no spouse or partner, living alone, no __c__
    * 393 = Size 1, not multig, no spouse or partner, living alone, __c__

    * 394 = Size 2, not multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 395 = Size 3, not multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 396 = Size 4, not multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 397 = Size 5, not multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 398 = Size 6, not multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 399 = Size 7+, not multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 400 = Size 3, multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 401 = Size 4, multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 402 = Size 5, multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 403 = Size 6, multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17
    * 404 = Size 7+, multig, no spouse or partner, with __oc__ u6 only, no __cxhhsup__ 6to17

    * 405 = Size 3, not multig, no spouse or partner, with __oc__ u6 only, __cxhhsup__ both
    * 406 = Size 4, not multig, no spouse or partner, with __oc__ u6 only, __cxhhsup__ both
    * 407 = Size 5, not multig, no spouse or partner, with __oc__ u6 only, __cxhhsup__ both
    * 408 = Size 6, not multig, no spouse or partner, with __oc__ u6 only, __cxhhsup__ both
    * 409 = Size 7+, not multig, no spouse or partner, with __oc__ u6 only, __cxhhsup__ both
    * 410 = Size 3, multig, no spouse or partner, with __oc__ u6 only, __cxhhsup__ both
    * 411 = Size 4, multig, no spouse or partner, with __oc__ u6 only, __cxhhsup__ both
    * 412 = Size 5, multig, no spouse or partner, with__oc__ u6 only, __cxhhsup__ both
    * 413 = Size 6, multig, no spouse or partner, with __oc__ u6 only, __cxhhsup__ both
    * 414 = Size 7+, multig, no spouse or partner, with __oc__ u6 only, __cxhhsup__ both

    * 415 = Size 2, not multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 416 = Size 3, not multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 417 = Size 4, not multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 418 = Size 5, not multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 419 = Size 6, not multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 420 = Size 7+, not multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 421 = Size 3, multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 422 = Size 4, multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 423 = Size 5, multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 424 = Size 6, multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6
    * 425 = Size 7+, multig, no spouse or partner, with __oc__ 6to17 only, no __cxhhsup__ u6

    * 426 = Size 3, not multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both
    * 427 = Size 4, not multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both
    * 428 = Size 5, not multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both
    * 429 = Size 6, not multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both
    * 430 = Size 7+, not multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both
    * 431 = Size 3, multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both
    * 432 = Size 4, multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both
    * 433 = Size 5, multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both
    * 434 = Size 6, multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both
    * 435 = Size 7+, multig, no spouse or partner, with __oc__ 6to17 only, __cxhhsup__ both

    * 436 = Size 3, not multig, no spouse or partner, with __oc__ both
    * 437 = Size 4, not multig, no spouse or partner, with __oc__ both
    * 438 = Size 5, not multig, no spouse or partner, with __oc__ both
    * 439 = Size 6, not multig, no spouse or partner, with __oc__ both
    * 440 = Size 7+, not multig, no spouse or partner, with __oc__ both
    * 441 = Size 4, multig, no spouse or partner, with __oc__ both
    * 442 = Size 5, multig, no spouse or partner, with __oc__ both
    * 443 = Size 6, multig, no spouse or partner, with __oc__ both
    * 444 = Size 7+, multig, no spouse or partner, with __oc__ both

    * 445 = Size 2, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 446 = Size 3, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 447 = Size 4, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 448 = Size 5, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 449 = Size 6, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 450 = Size 7+, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only

    * 451 = Size 3, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 452 = Size 4, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 453 = Size 5, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 454 = Size 6, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only
    * 455 = Size 7+, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ u6 only

    * 456 = Size 2, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 457 = Size 3, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 458 = Size 4, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 459 = Size 5, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 460 = Size 6, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 461 = Size 7+, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17

    * 462 = Size 3, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 463 = Size 4, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 464 = Size 5, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 465 = Size 6, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17
    * 466 = Size 7+, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ 6to17

    * 467 = Size 3, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ both
    * 468 = Size 4, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ both
    * 469 = Size 5, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ both
    * 470 = Size 6, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ both
    * 471 = Size 7+, not multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ both

    * 472 = Size 4, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ both
    * 473 = Size 5, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ both
    * 474 = Size 6, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ both
    * 475 = Size 7+, multig, no spouse or partner with relatives, no __oc__, but has __cxhhsup__ both

    * 476 = Size 2, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__
    * 477 = Size 3, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__
    * 478 = Size 4, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__
    * 479 = Size 5, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__
    * 480 = Size 6, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__
    * 481 = Size 7+, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__

    * 482 = Size 3, multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__
    * 483 = Size 4, multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__
    * 484 = Size 5, multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__
    * 485 = Size 6, multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__
    * 486 = Size 7+, multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, no __c__

    * 487 = Size 2, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 488 = Size 3, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 489 = Size 4, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 490 = Size 5, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 491 = Size 6, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, but has __c__
    * 492 = Size 7+, not multig, no spouse or partner with relatives, no __oc__, no __cxhhsup__, but has __c__

    Note: the child under 18 must be the householder. Therefore the household can't have an own child over 18 so it can't be multig.

    * 493 = Size 2, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ u6 only
    * 494 = Size 3, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ u6 only
    * 495 = Size 4, not multig, no spouse or partner with  no relatives, no __oc__, but has __cxhhsup__ u6 only
    * 496 = Size 5, not multig, no spouse or partner with  no relatives, no __oc__, but has __cxhhsup__ u6 only
    * 497 = Size 6, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ u6 only
    * 498 = Size 7+, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ u6 only

    * 499 = Size 2, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ 6to17
    * 500 = Size 3, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ 6to17
    * 501 = Size 4, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ 6to17
    * 502 = Size 5, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ 6to17
    * 503 = Size 6, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ 6to17
    * 504 = Size 7+, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ 6to17

    * 505 = Size 3, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ both
    * 506 = Size 4, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ both
    * 507 = Size 5, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ both
    * 508 = Size 6, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ both
    * 509 = Size 7+, not multig, no spouse or partner with no relatives, no __oc__, but has __cxhhsup__ both

    * 510 = Size 2, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 511 = Size 3, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 512 = Size 4, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 513 = Size 5, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 514 = Size 6, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, no __c__
    * 515 = Size 7+, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, no __c__

    * 516 = Size 2, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 517 = Size 3, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 518 = Size 4, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 519 = Size 5, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 520 = Size 6, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, but has __c__
    * 521 = Size 7+, not multig, no spouse or partner with no relatives, no __oc__, no __cxhhsup__, but has __c__
    


    






    


    





