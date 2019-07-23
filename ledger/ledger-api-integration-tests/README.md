# Integration tests for Ledger API based implementations

This module contains various integration tests running against Ledger
API implementations (sandbox, platform).

Due to the time and resource requirements of these tests, they don't
run by the normal `test` task of the build. Instead the test jar and
its dependencies are packaged into a `.tgz` archive, which can be
started using `java -jar ledger-api-integration-tests.jar`, using
scalatest Runner arguments (see
http://www.scalatest.org/user_guide/using_the_runner ). This `.tgz` is
stashed during the normal build, and used later in several,
concurrently run Jenkins stages.

## Scenario based semantic tests

The scenario based semantic tests are running scenarios found in DAML
libraries against the sandbox and the platform.

Testing behavior for scenario based semantic tests. The usage pattern is as follows:

```scala
class SomeTest extends AsyncFlatSpec
  with TestExecutionSequencerFactory
  with AkkaBeforeAndAfterAll
  with SemanticTestBehavior {

val scenariosToExclude = Map(LedgerBackend.Sandbox => (scenarioName) => true,
                             LedgerBackend.Platform => Set("Test.scenario"))

config(ModuleId("<groupId>", "<artifactId>", "<version>", "dar"), "committer", scenariosToExclude)
  .run
  .unsafePerformSync
  .fold(e => sys.error(s"$e"), identity)
  .toConfigurationVector(Set(LedgerBackend.Platform))
  .foreach(semanticTestBehavior)
}
```

Since dynamic runtime configuration of scalatest tests are a bit
messy, the configuration is static for each test suite. Although
multiple configurations can be executed in each suite, a Jenkins stage
can only run one or more test suites. It is not possible to run only
parts of a suite. That is why there are two suite classes, one for
sandbox and one for platform for each DAML library.

The config function returns a `scalaz.concurrent.Task`, which loads
the configuration. The implementation loads the DAML package,
discovers all the scenarios, including the necessary stakeholders for
each scenario, inspects which scenarios are manipulating time, and
based on these data creates a configuration.

After running the task, the `toConfigurationVector` creates a list of
configuration vectors. Each element in this list represents one run of
either sandbox or platform, for a given participant configuration, and
a list of scenarios to execute.

For every element of this list one should call `semanticTestBehavior`,
which starts the configured backend, set up with the configured
participants, and executes the configured scenarios (to be precise,
the SemanticTester creates a list of commands and expected responses
based on the content of the scenarios, and executes these commands on
the backend).

## Writing integration tests

The integration tests should work with any conformant DAML ledger.
This includes not only our sandbox, but also other ledger implementations.

The following are important considerations for writing integration tests. 


### Exclusive access

Don't assume you have exclusive access to the ledger.
Other tests or applications may be running concurrently.

* Don't make assumptions on how ledger offsets evolve.
  Other applications may modify the ledger in between your commands.
* Make sure command and workflow IDs are globally unique,
  and not shared between test runs.
  * See `TestIdsGenerator.testCommandId` and `TestIdsGenerator.testWorkflowId`
* Make sure party names are globally unique,
  and not shared between test runs.
  * See `TestIdsGenerator.testPartyName`
* Thanks to our privacy model, using unique party names and workflow IDs
  will essentially create a private sub-ledger and make sure none of your actions interfere with the rest of the ledger.

### Previous ledger content

Don't make assumptions on the previous content of the ledger.
The ledger may be completely empty, or it may be a long running ledger with millions of ledger entries.

* Don't start streaming from genesis.
  Instead, at the start of the test, read the ledger begin,
  and start all streaming from there.
* Make sure all required DAML packages are uploaded
  and all required parties are allocated before the test runs.
  Note that the packages may or may not have been previously uploaded by other applications.
  * If possible, upload packages and allocate parties at the beginning of the test using the admin API.
    In the future, this setup step will be handled by the test tool itself.
* Be mindful when using commands that return unfiltered results.
  For example, `ListKnownPartiesRequest` could return a huge number of entries.
