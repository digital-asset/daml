# Release of Daml DAML_VERSION

## Bugfixes
 
- Fix bug 23-029
    The bug happens for Daml models that throw a Daml exception in the body of 
    an Exercise that is caught outside of the exercise and the remainder of 
    the  transaction includes a projection for an new informee. Daml models 
    that do not catch exceptions are unaffected.

## What’s New
