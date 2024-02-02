# Release of Daml DAML_VERSION
Use one of the following topics to capture your release notes. Duplicate this file if necessary.
Add [links to the documentation too](https://docs.daml.com/DAML_VERSION/about.html).

You text does not need to be perfect. Leave good information here such that we can build release notes from your hints.
Of course, perfect write-ups are very welcome.

You need to update this file whenever you commit something of significance. Reviewers need to check your PR
and ensure that the release notes and documentation has been included as part of the PR.

## Bugfixes

## What’s New

# Release of Daml 2.8.1

## Bugfixes

## What’s New

# Release of Daml 2.9.0

## Bugfixes

## What’s New

# Release of Daml 3.0.0

## Bugfixes

## What’s New

* The type class `HasField` (methods `getField` and `setField`) has been split
  in two: `GetField` and `SetField`, each one with the correspondingly named
  method. `HasField` is now defined as a type synonym for the conjunction of
  `GetField` and `SetField`. This means that functions with `HasField`
  constraints will continue to work without any changes.

  No action should be necessary for migrating the compiler-generated instances
  for record types. For user-defined instances of the `HasField` class,
  migration is a matter of replacing one instance declaration with two, for
  example,

  ```daml
  instance HasField "field_name" RecType FieldType where
    getField = <getFieldImpl>
    setField = <setFieldImpl>
  ```

  would have to be rewritten as

  ```daml
  instance GetField "field_name" RecType FieldType where
    getField = <getFieldImpl>

  instance SetField "field_name" RecType FieldType where
    setField = <setFieldImpl>
  ```

* User-defined instances of the class `GetField` enable the use of dot-syntax
  for field access, i.e. `rec.field`

* User-defined instances of the class `SetField` enable the use of with-syntax
  for field update, i.e. `rec with field = newValue`
