# TODO for 1940

* Prechecks

 - [ ] Confirm 1940 parameter settings with John A

 - [ ] Confirm basic counts on histogram vars

Notes: GQTYPEs 1,5 do not occur. Since GQTYPEs are invariant, these will be treated as structural zeros and removed from consideration. Valid GQTYPE values are 0,2,3,4,6,7,8,9. The value 9 (Other non-institutional GQ and unknown) is most common and probably represents households. Households and GQ are treated more or less uniformly so don't need to make this assumption. Will stick with 0 (non-group quarters households). Update: "GQTYPE" combined with "GQ" gives evidences that many of these are in fact households. Consider remapping them.

 - [ ] Check for vacant GQs.

No vacant units of any kind found in output_H.csv. Need to check if Simson is filtering those out or whatnot. 


HISPAN recode 0 if 0 else 1.

AGE recode 0 if <18 else 1.

GQTYPE recode shift numbering down. shift back for writer.

RACE recode shift down 1. Shift back for writer.

All other histogram vars should behave as expected.

 - [ ] Ask about voting age and structural zeros.

Notes: Sticking with 18 for va. Hearing no objections, proceeding with no structural zeros on GQ occupants.

* Programming tasks
 - [ ] 1940 Reader
    
 - [ ] 1940 InvariantsCreator

 - [ ] 1940 ConstraintsCreator

 - [ ] 1940 DPQueries

 - [ ] 1940 Writer

 - [ ] 1940 Config

 - [ ] Testing. Completed bug free run.