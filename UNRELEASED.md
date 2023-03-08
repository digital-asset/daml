# Release of Daml DAML_VERSION

## What’s New

Use one of the following topics to capture your release notes. Duplicate this file if necessary.
Add [links to the documentation too](https://docs.daml.com/DAML_VERSION/about.html).

You text does not need to be perfect. Leave good information here such that we can build release notes from your hints.
Of course, perfect write-ups are very welcome.

You need to update this file whenever you commit something of significance. Reviewers need to check your PR 
and ensure that the release notes and documentation has been included as part of the PR.

### Restricted name warnings
Attempting to use the names `this`, `self` or `arg` in template, interface or exception fields will often result in confusing errors, mismatches with the underlying desugared code.  
We now throw an error (or warning) early in those cases on the field name itself to make this more clear.

*Note: Exception as well as templates without any choices did not previously throw errors for both `self` and `arg`. While using these names is discouraged, we only throw a warning here to avoid a breaking change. We may promote this to an error in future.*