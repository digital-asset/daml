**NOTE** To start contributing to the project, first please read
[BUILD.md](BUILD.md), as it contains the basic information about our
build.

# Design Documentation

Design documents should be written for any non-trival new feature or change.
Please store them in the [Team Design Document folder in Google Drive](https://drive.google.com/drive/u/0/folders/1oqfZtpZzZRQtZoFI75KgWQNxQ4TNtfj7).

Include the JIRA ticket and an informative short description in the name of the document.
Use the Design Document template as a base, but free feel to remove sections not useful to your design.

# Development Environment Setup

## Importing into IntelliJ

**Important** Before importing the project, you need to change the
heap size setting for sbt in IntelliJ. Go to `Settings > Build,
Execution, Deployment > Build Tools > sbt` and change the `Maximum
heap size, MB` to `4096`.

Then import the project as an sbt project, using the normal IntelliJ
project import dialog.

**Note**: During the import, in the settings dialog check the `Use sbt
shell for build and import` checkbox.

That's all. It should take 5-10 few minutes to import the project.
Everything else should work as usual.

### Compiler Flags

`javac` must use the `-parameters` flag due to our usage of the
[Jackson
parameter-names](https://github.com/FasterXML/jackson-modules-java8/tree/master/parameter-names)
module.

The default maven build is already set to use this. However it must be
set manually for IntelliJ.

This can be set in IntelliJ by going to
Preferences->Build,Execution,Deployment->Compiler->Java Compiler and
setting `-parameters` in the `Additional command line parameters`
field.

### Notes on the performance of importing and refreshing the sbt project in IntelliJ

As reported by many, importing the ledger sbt project into
IntelliJ takes a lot of time, which makes switching branches really
painful.

If you experience this issue, please go the `File -> Settings` menu,
and under `Build, Execution Deployment -> Build tools -> sbt` uncheck
the `Download Library sources` and `Download sbt sources` checkboxes.


## Useful sbt commands

During development it is useful to have the sbt shell open. Unlike
maven, sbt has an interactive shell.

The ledger sbt build has a few command aliases, which
implements functionality by calling many tasks:

### packageAll

`sbt packageAll` will create all artifacts for the project. Some
testing scripts (not part of the build, but under `script/test` expect
the output of this command to be in place.

### verify

This runs all checks executed in the first step of the CI build, which
includes checking code being well formatted, running unit- and
integration tests.

### fmt

`sbt fmt` will run the code formatter for all scala and sbt files.

### testWithCoverage

Runs tests with code coverage turned on.

### Some other useful commands in an example session:

    sbt:ledger> clean
    sbt:ledger> test:compile                     // compiles everything
    sbt:ledger> compile                          // compiles non-test code
    sbt:ledger> test                             // runs all tests (unit and integration tests)
    sbt:ledger> viper / test                     // runs all the tests in viper
    sbt:ledger> viper / testOnly com.da.SomeTest // Runs a single test

# Coding Guidelines

These guidelines are what we expect to do by default when writing new code.
Exceptions can and should be made where it makes sense - particularly for integrating with external or legacy code, or where these suggested practices would be sub-optimal.

## ScalaDoc

 * Typically, place ScalaDoc on all public class and method declarations. 
 * Describe any error cases or preconditions the caller should be aware of (unless clearly documented in a [precondition](#precondition) or error type [error type](#error-handling).
     * If a class uses a set of functions with similar error cases it is fine to document them once at class level.
 * Parameter documentation can be ommitted unless expected values are non-obvious through naming or type alone.
 * Describe the side effects in the method if there are any.

## Error Handling

Prefer using `Either[Err, Result]` return types over throwing our own exceptions for code that returns known and specific errors.
We anticipate that Error types will be a `sealed trait` and `case class` hierarchy suitable for the unit of functionality.
In contrast we do not expect to define a shared/common hierarchy for errors such as null pointers or runtime issues.

If you have a method that deliberately throws an exception, prefer naming it with "try" prefix, e.g. `tryDoSomething`.

However as Exceptions can still occur at Runtime, a top level caller should still catch and appropriately handle them.

When defining asynchronous behavior still prefer using this approach with a `Future[Either[Err, Result]]` and treat Failed futures the same way as unexpected Runtime exceptions.

## Preconditions

1. Use of [`require(requirement, message)`](https://www.scala-lang.org/api/2.12.x/scala/Predef$.html#require(requirement:Boolean,message:=%3EAny):Unit) is strongly encouraged for parameters on public APIs that can not be obviously inferred from the type and can be easily verified. Please provide a descriptive `message` with all checks.
  * Examples to use `require`: collections requiring a certain size, a number being above 0
  * Examples to avoid `require`: 
    * Checking values are non-null. Checks for every parameter will add significant noise to the code, and callers can be expected to know they should provide instances.
    * Complex data structures should not be validated by every method using them. For instance it is difficult to describe a "valid" transaction succinctly and repeated runtime checks may become costly.
2. The preconditions of a method should be documented in its ScalaDoc.

## [Scalaz](https://github.com/scalaz/scalaz) Usage

Generally we prefer idiomatic core Scala and usage of its standard library over Scalaz alternatives. Particularly for core APIs used throughout ledger. However isolated usages of Scalaz is permitted for:

* Tagged Types for values where type information beyond a primitive type would be desirable (e.g. `ParticipantId` rather than `String`).
* Utility operations that work on top of scala core data structures. A prominent example is: [traverse/sequence](https://github.com/scalaz/scalaz/blob/c284750ea5afaa44a02d3e6aa06b9e9e183b2128/core/src/main/scala/scalaz/syntax/TraverseSyntax.scala#L30). 
We are willing to accept this usage since it isn't pervasive (we don't need to adopt scalaz data structures) and it is a very common operation we would have had to implement ourselves otherwise. 

Where Scalaz provides a data structure that we'd otherwise be duplicating ourselves we may opt to adopt the Scalaz version. Any adoption of data structures into shared APIs should be reviewed by the bulk of the team and decision documented.

Example:
 * Usage of [`StrictTree`](https://github.com/scalaz/scalaz/blob/series/7.3.x/core/src/main/scala/scalaz/StrictTree.scala) in our GSL V2 transaction representation.

If we reach use cases where we find core Scala practices sub-optimal/undesirable and would like to consider alternative Scalaz approaches, create a straw man portraying issues and a proposal for alternatives. Review with the whole team to reach a consensus then document the decision.
It is more important for the codebase to have consistency and that the whole team comfortable owning and maintaining it than optimising for isolated/small cases.

## Usage of basic data types

Whenever possible in scala code we should use data types defined in `com.digitalasset.daml.lf.data` and `com.digitalasset.daml.lf.transaction` packages for basic datatypes of our domain, such as `Party` or `Transaction`.

As we require or need extra indirection for extension of basic types or inclusion in another API we suggest to use a single module to include data definitions and re-export the original daml-lf definitions for simpler imports.

## Deferred work

If you want to indicate work that needs to be done put a `//TODO` comment in the code. The comment should read:

```
// TODO (DEL-nnnn) some description
```

Where `nnnn` is the associated JIRA ticket.

When the todo will definitely be done almost immediately in a following PR using your name is sufficient.

```
// TODO (Padbury) will be improved when we..
```

# Review Guidelines

## General Rules

 * Try to think through how big a pull request/change will be in advance of starting it, and opt to follow our process for either light or heavy PRs.
 * We generally trust each other to be good actors.
   * If you say you'll be doing something in a follow up PR, please do it.
   * If someone submits a conditional approval based on a change, make that change before merging.
 * If you request changes, please be engaged when those changes are made or the discussion around them. Also prefix your requests with severity flags:
   * **FIX**: something you view as a bug due to which you are rejecting the PR
   * **FOLLOWUP**: something you want the author to address but are ok deferring to a follow-up PR or future JIRA ticket
   * **MINOR**: naming, formatting, syntax feedback etc that reviewer will expect to be actioned before merging
   * **QUESTION**: subjective/opinionated comment typically on styling/naming which author should respond to but could merge without actioning (e.g. suggestion of a different name)
 * When the author has addressed requested changes, the reviewer should resolve pertinent conversation threads in GitHub by pressing the `Resolve` button.

We attempt to bucket code reviews into two different categories:

## Light Pull Requests

 * Can be reviewed by any team member with little context
 * Does not include significant code, design or architectural changes

For the submitter it is appropriate to broadcast the review to our Slack channel (typically `#team-ledger-sync`) when it is ready for review.

Anyone who is in a position to review should attempt to pick up. Between us all this should result in a timely review.

## Heavy Pull Requests

 * The proposed changes require significant context to perform a meaningful reviews. For example:
   * Is an implementation of a previously reviewed design or specification, and knowledge of that is required
   * Changes have significant risk if incorrect (e.g. time model properties, critical to our integrity guarantees, vital to our NFR or operational properties)

Ideally engage potential reviewers long before the PR is ready and let them know that these changes are coming with what will be required to perform a meaningful review (particularly any documents or specifications that would be good to read in advance).
In many cases there will be a natural reviewer (likely someone else around the work stream of the change), if not ask the team lead to assign someone in advance of the PR being ready.

Keep in mind it can be more effective to discuss complex topics in person or on Zoom/Slack rather than a back and forth over Github comments. This is encouraged.
