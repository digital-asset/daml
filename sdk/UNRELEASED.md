# Release of Daml DAML_VERSION

## Bugfixes

## What’s New

# Release of Daml 2.7.0

## Bugfixes
- Fixed the daml-script binary not using TLS and access token settings correctly when using the `--all` flag.

## What’s New

Use one of the following topics to capture your release notes. Duplicate this file if necessary.
Add [links to the documentation too](https://docs.daml.com/DAML_VERSION/about.html).

You text does not need to be perfect. Leave good information here such that we can build release notes from your hints.
Of course, perfect write-ups are very welcome.

You need to update this file whenever you commit something of significance. Reviewers need to check your PR
and ensure that the release notes and documentation has been included as part of the PR.

### Minor Script warning
As part of the ongoing efforts to improve Scripts and the testing story around them, we've added a small warning for a test case that non-obviously doesn't run.
Consider the following test:
```hs
myTest = script do
    ...
    error "Got here, and shouldn't have"
```
This test will not run, as it is implicitly polymorphic. Take a look at the type of [`error`](https://docs.daml.com/daml/stdlib/Prelude.html#function-ghc-err-error-7998), it returns `a`, and as such, so does `myTest`. We cannot "run" this without making `a` a concrete type, so this cannot be run.
We provide a warning in this case so a user may rectify this issue either by providing an explicit type signature, or by bounding the value of `a` in the call to `script`, via `script @()`.

### Restricted name warnings
Attempting to use the names `this`, `self` or `arg` in template, interface or exception fields will often result in confusing errors, mismatches with the underlying desugared code.
We now throw an error (or warning) early in those cases on the field name itself to make this more clear.

*Note: Exception as well as templates without any choices did not previously throw errors for both `self` and `arg`. While using these names is discouraged, we only throw a warning here to avoid a breaking change. We may promote this to an error in future.*

### Dynamic Exercise
As part of extending the language to support evolving template definitions, we've added a new function `dynamicExercise : HasDynamicExercise t c r => ContractId t -> c -> Update r`, only available when targetting Daml LF version `1.dev`. Currently, it operates just like `exercise`, but at a later stage it will instead use the most recent definition of template `t` choice `c` vetter by all stakeholders.

### `daml script --ide-ledger`
In an effort to unify some of our internal and external tools, we now support the `--ide-ledger` option in `daml script`, allowing a user to directly invoke scripts within a `dar` file on their local machine, without a separate ledger running. Note the difference here with `daml test` being that `daml script` will not attempt to recompile or read the source code directly. This option cannot be used with `--ledger-host`, `--participant-config` or `--json-api`.

# Release of Daml 2.8.0

## Deprecation of template-local definitions

The syntax for `let` bindings in `template` definitions will be deprecated in
favor of plain top-level bindings. If the deprecated syntax is used then the
following warning will be shown during compilation or in the IDE:

```
  Template-local binding syntax ("template-let") is deprecated,
  it will be removed in a future version of Daml.
  Instead, use plain top level definitions, taking parameters
  for the contract fields or body ("this") if necessary.
```

For more information, see [Reference: Templates: Template-local Definitions (Deprecated)](https://docs.daml.com/2.8.0/daml/reference/templates.html#template-local-definitions-deprecated)

## Removal of deprecated 'controller..can' syntax

The 'controller..can' syntax for defining template choices, deprecated since
Daml 2.0, has now been completely removed. Projects that use this syntax are no
longer accepted. Those choices should instead be defined using 'choice-first'
syntax.

Note that, as a consequence, the warning flags `-Wcontroller-can` and
`-Wnocontroller-can` are not accepted anymore.

See [Deprecation of `controller`-first syntax: Migrating](https://docs.daml.com/2.7.0/daml/reference/choices.html#migrating)
for more information on how to adapt existing projects.

## What’s New
### Daml-script json support for --all
The daml-script binary runner has been refactored to be more consistent across using `--script-name` and `--all`.  
As such, now `--all` will work when using `--json-api`.  

### Daml-script --upload-dar flag
The daml-script binary now allows you to specify if you want the Dar containing your scripts to be uploaded to the ledger before execution.  
**NOTE - DEPRECATION:** The previously implicit uploading behaviour of automatically uploading when using `--all` is now deprecated with a warning.  
If you need to upload, you should explicitly write `--upload-dar=yes`. If you don't, you can either ignore the warning until it is dropped in daml3
(at which point the default behaviour will change to never upload), or you can explicitly write `--upload-dar=no`, which will continue to work after daml3.

# Release of Daml 2.9.0

# Release of Daml 3.0.0
