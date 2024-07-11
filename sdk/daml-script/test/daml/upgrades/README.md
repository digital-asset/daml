# Writing upgrades tests here
This suite allows for small, self contained runtime upgrades tests written with daml-script.  
It provides a mechanism for generating upgraded dars within the daml test file using comments.

## Generated packages
Each test module can have one or more `PACKAGE` definitions in the following format:
```
{- PACKAGE
name: <the-package-name>
versions: <number of versions to create as an int, will generate 1.0.0, 2.0.0, ..., n.0.0>
-}
```

as well as one or more `MODULE` definitions in the following format:
```
{- MODULE
package: <a-package-name>
contents: |
  module <Name.Of.The.Module> where
  <write daml code here>
-}
```

NOTES:
  * The `name` of a `PACKAGE` must be unique across all upgrades tests, so choose something similar to your test file name.
  * If the `package` of a `MODULE` definition does not match any `PACKAGE` definition, it will be ignored.
  * The module name is extracted from the `contents`, so it must contain the line `module <Name.Of.The.Module> where` somewhere.
    * Version comments (see below) are processed before extracting the module name, allowing for module name changes across versions.
  * If multiple `MODULE` definitions use the same module name, the test suite will crash.

The contents of each module can use a version comment syntax to include subsets of code in a specific version.
The syntax for this is `<some code> -- @V <space separated versions>`. For example
```
template Upgraded with
    p: Party
    anotherField: Optional Int -- @V  2
    fieldThatChangesType: Int  -- @V 1
    fieldThatChangesType: Text -- @V  2
  where
    signatory p
```

With these comments, the modules can be imported via modules prefixes. Each module path will be prefixed with `V1` .. `Vn` for importing.  
i.e.
```
import qualified V1.Name.Of.The.Module as V1
import qualified V2.Name.Of.The.Module as V2
```

## UpgradeTestLib
It is expected that all daml script test files import the `UpgradeTestLib` module, which will transitively import daml3-script and various assertion/utility modules.  
This testing module exposes the `tests` function as such  
```
tests : [(Text, Script ())] -> Script ()
```
This is used to define your list of named tests. Each test file will run a top level `main : Script ()` function, so the usual formula is
```
main : Script ()
main = tests
  [ ("some test", someTest)
  , ("some other test", someOtherTest)
  ]

someTest : Script ()
someTest = ...

someOtherTest : Script ()
someOtherTest = ...
```

Given upgrades is work in progress, we also expose `broken : (Text, Script ()) -> (Text, Script ())`, which can be used to wrap the test cases in the `main` definition. This will tag the test with `(BROKEN)` and ensure it fails.
```
main : Script ()
main = tests
  [ ("some test", someTest)
  , broken ("oops", someOtherTest)
  ]
```

Finally, we provide some utilities for unvetting dars in tests.
```
withUnvettedDar : Text -> Script a -> Script a
withUnvettedDarOnParticipant : Text -> ParticipantName -> Script a -> Script a
participant0 : ParticipantName
participant1 : ParticipantName
```
These allow running a computation with a dar unvetted, and handle re-vetting the dar afterwards, even in the case of failure. The first `Text` field is the dar names discussed in the Generated packages section, i.e. `my-package-1.0.0`.
Avoid using the daml3-script internal vetting primitives, and use these functions instead.
