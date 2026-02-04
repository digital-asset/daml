// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.docs

import better.files.*
import better.files.Dsl.{ln_s, mkdirs, rm}
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  HeadlessConsole,
  InstanceReference,
  SplitBufferedProcessLogger,
}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.docs.DocsGenerationSynchronization.{
  cleanUpDarsSimlink,
  createDarsSimlink,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{SuppressingLogger, TracedLogger}
import com.digitalasset.canton.resource.{DbMigrations, DbStorage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.circe.syntax.*
import monocle.macros.syntax.lens.*
import org.flywaydb.core.Flyway

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.blocking
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.sys.process.Process
import scala.util.{Failure, Success, Using}

/** Sphinx documentation generator
  *
  * This class is used in our documentation build to test the documented commands (as an integration
  * test) and insert their output into the manual. Have a look at the getting started guide.
  *
  * Effectively, the file is organised by snippets. A snippet is defined as
  *
  * {{{
  * .. snippet:: snippet_name
  * }}}
  *
  * Each distinct snippet_name will be executed in an isolated test environment! Under the snippet
  * directive there are three types of actions that can run:
  *
  * {{{
  * .. snippet:: snippet_name
  *     .. success(output=10): participant1.parties.list("Bob")
  *     .. assert:: RES.nonEmpty
  *     .. failure:: participant1.parties.enable("Bob")
  *     .. hidden:: val synchronizerId = sequencer1.synchronizer_id
  * }}}
  *
  * The `success` command will run the command and capture the output (optionally truncated to the
  * number of lines you want).
  *
  * The `assert` command will be silently asserting the previous result. You always get the variable
  * RES which refers to the previous result. So the type of the assert command always needs to be a
  * boolean.
  *
  * The `failure` command is used for commands where we expect a failure. Here, we also capture the
  * output.
  *
  * The `hidden` command is used for running additional (boilerplate) console commands that are
  * needed to make the following snippets work, but are likely distracting for what the current
  * topic wants to explain. But be mindful, double-check that a `hidden` command is actually not
  * required for the documented snippet to work. For example, avoid adding
  * {{{
  *   .. hidden:: import com.digitalasset.canton.something
  * }}}
  * to make the documentation snippets work. This import will not appear in the documentation, and
  * when users copy-paste the documented snippet into the console, they face a compilation error.
  *
  * In each RST file, you can have several snippets. They are mapped to "IsolatedEnvironments".
  *
  * To ensure source RST file and target JSON output file paths are as expected by the tooling,
  * implement your "snippet generator" by extending from
  * [[com.digitalasset.canton.integration.tests.docs.SnippetGenerator]].
  *
  * Future improvements / known limitations:
  *   - compilation failures are not properly included as output
  *   - an RST file can only have one topology configuration file.
  */
abstract class SphinxDocumentationGenerator(
    source: File,
    target: File,
    config: File,
    moreConfig: File*
) extends CommunityIntegrationTest
    with IsolatedEnvironments {

  override lazy val environmentDefinition: EnvironmentDefinition = makeEnvironment

  /** if true, we will reuse our static node identities with pre-generated keys */
  def useStaticIdentity: Boolean = true

  protected def makeEnvironment: EnvironmentDefinition = {
    val fullConfig = config +: moreConfig
    val definition = EnvironmentDefinition
      .fromFiles(fullConfig*)
      .clearConfigTransforms()
      .addConfigTransforms(
        ConfigTransforms.uniqueH2DatabaseNames,
        ConfigTransforms.globallyUniquePorts,
      )
      .addConfigTransforms(ConfigTransforms.optSetProtocolVersion*)
      .focus(_.testingConfig.participantsWithoutLapiVerification)
      /* Participant nodes run as part of the documentation tests have testing flag disabled.
      As a result, the verification is done over the store and not via the state inspection.
      However, if the participant is backed by in-memory store, the check logs an error, which
      makes the CI fail.
      Since scenarios are relatively simple, the check is disabled.
       */
      .replace(Set("participant1", "participant2", "participant3", "participant4"))

    if (useStaticIdentity) {
      ReuseCryptoKeys.transformEnvironmentDefinition(definition)
    } else {
      definition
    }
  }

  private val snippetResults = new mutable.ListBuffer[Seq[SnippetStepResult]]()

  private def fetchTestScenarios: Seq[SnippetScenario] = {
    logger.debug(s"Loading script from file $source")

    SnippetScenario.parse(source.lines.toSeq)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    storeResults()
  }

  private def storeResults(): Unit = {
    val dir = target.parent
    if (!dir.exists)
      mkdirs(dir)

    def toRstCodeBlock(acc: String, stepResult: SnippetStepResult) = {
      val (indent, cmd, out) = stepResult.truncatedOutput
      val atSign = if (stepResult.prependAtSign) "@ " else ""
      val language = stepResult.language
      val sb = new mutable.StringBuilder
      if (cmd.nonEmpty) {
        sb.append(s".. code-block:: $language\n\n$indent$atSign$cmd\n")
      }
      val stepOutputLines = if (out.isEmpty) Array.empty[String] else out.split("\n")
      stepOutputLines.foreach(line => sb.append(s"$indent    $line\n"))
      if (stepOutputLines.isEmpty) sb.append("\n")
      acc + sb.toString()
    }

    target.write(
      snippetResults
        .map {
          _.foldLeft("") { (acc, stepResult) =>
            toRstCodeBlock(acc, stepResult)
          }
        }
        .asJson
        .spaces2
    )
  }

  protected def runAtScenarioStart(scenarioName: String)(implicit
      env: TestConsoleEnvironment
  ): Unit = ()

  "documentation snippets" should {
    fetchTestScenarios.foreach { scenario =>
      scenario.name onlyRunWith ProtocolVersion.latest in { implicit env =>
        runAtScenarioStart(scenario.name)
        clue(s"Running scenario ${scenario.name}") {
          val result = Using(new HeadlessConsole(env, logger = logger)) { console =>
            val res = new ScenarioRunner(console, scenario)(logger, loggerFactory).run()

            snippetResults.appendAll(res)
          }

          result match {
            case Failure(exception) =>
              fail(exception) // we want the test to fail if the command fails
            case Success(_) => succeed
          }
        }
      }
    }
  }
}

abstract class SnippetGenerator(
    source: File,
    config: File,
    additionalConfig: File*
) extends SphinxDocumentationGenerator(
      source,
      SnippetGenerator.deriveTargetFile(source),
      config,
      additionalConfig*
    )

private object SnippetGenerator {

  private val SourcePrefix = File("docs-open/src/sphinx/")
  private val SourceExtension = ".rst"
  private val TargetPrefix = File("docs-open/target/snippet_json_data/")
  private val TargetExtension = ".json"

  def deriveTargetFile(source: File): File = {

    require(
      source.path.startsWith(SourcePrefix.path),
      s"Source path for derivation must start with '$SourcePrefix'. Found: '$source'",
    )
    require(
      source.extension().contains(SourceExtension),
      s"Source file for derivation '$source' must have a '$SourceExtension' extension.",
    )

    TargetPrefix.path
      .resolve(SourcePrefix.relativize(source).normalize())
      .toString
      .stripSuffix(SourceExtension)
      .concat(TargetExtension)
      .toFile
  }
}

object ScenarioRunner {
  // Regex to capture shell commands which output is stored in a variable
  private val varCaptureRegex = """^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*\$\(([\s\S]*?)\)\s*$""".r
}

class ScenarioRunner(console: HeadlessConsole, scenario: SnippetScenario)(
    logger: TracedLogger,
    loggerFactory: SuppressingLogger,
)(implicit env: TestConsoleEnvironment, traceContext: TraceContext) {
  import org.scalatest.Assertions.*
  import org.scalatest.EitherValues.*

  // Records the output of shell commands as the scenario plays out
  private val shellOutputs = mutable.Map.empty[String, String]
  // Shared temp directory for shell commands that can be run from anywhere but need a persisting directory
  // For instance because they write files to the directory
  private val shellTemporaryDir = File.newTemporaryDirectory(prefix = scenario.name)

  def run(): Seq[Seq[SnippetStepResult]] = {
    val counter = new AtomicInteger(0)
    console.init().value

    setupCapture(console)

    scenario.steps.map(snippet =>
      snippet.map {
        case step: SnippetStep.Success =>
          val idx = counter.incrementAndGet()
          runSuccessStep(step, idx)

        case step: SnippetStep.Failure =>
          runFailureStep(step)

        case step: SnippetStep.Assert =>
          val idx = counter.get()
          runAssertStep(step, idx)

        case step: SnippetStep.Hidden =>
          runHiddenStep(step)

        case step: SnippetStep.Shell =>
          runShellStep(step)
      }
    )
  }

  private def runSuccessStep(step: SnippetStep.Success, idx: Int) = {
    val cmd = step.cmd

    def run(command: String): Unit = console.runLine(command) match {
      case Left(value) =>
        fail(
          s"scenario ${scenario.name} failed at $step with $value on command $command"
        )
      case Right(_) => ()
    }

    logger.debug(s"Running success $cmd of line ${step.line}")
    env.testConsoleOutput.startRecording()

    val res = s"RES$idx"
    val identO = SnippetStep.cmdStartsWithVal.findFirstMatchIn(cmd) match {
      case Some(value) =>
        // run command first
        run(cmd)
        // now, grab the value
        val ident = value.group(1)
        run(s"val $res = $ident")
        Some(ident)
      case None if cmd.trim.startsWith("import") =>
        run(cmd)
        None
      case None =>
        run(s"val $res = " + cmd)
        Some(res.toLowerCase)
    }
    // pretty print the output
    identO.foreach(ident => run(s"capture($res, \"$ident\")"))
    val output = env.testConsoleOutput.stopRecording()

    logger.debug(s"Captured $output")
    SnippetStepResult(step, output)
  }

  private def runFailureStep(step: SnippetStep.Failure): SnippetStepResult = {
    val cmd = step.cmd

    logger.debug(s"Running failure $cmd of line ${step.line}")
    // capture errors and warnings
    loggerFactory.suppressWarningsAndErrors {
      console.runLine(cmd) match {
        case Left(err) =>
          logger.debug(s"Running failure $cmd ended with $err")
        case Right(_) =>
          fail(s"scenario ${scenario.name} should fail at $step but succeeded!")
      }
    }
    val errorsAndWarnings = loggerFactory.fetchRecordedLogEntries
    logger.debug(s"Observed $errorsAndWarnings")
    SnippetStepResult(
      step,
      errorsAndWarnings.map(x => s"${x.level} ${stripTestName(x.loggerName)} - ${x.message}"),
    )
  }

  private def runAssertStep(step: SnippetStep.Assert, idx: Int) = {
    val cmd = step.cmd

    logger.debug(s"Running assert $cmd of line ${step.line}")
    val res = s"RES$idx"
    console.runLine(s"assert(${cmd.replace("RES", res)})") match {
      case Left(value) =>
        fail(s"scenario ${scenario.name} assertion failed at $step: $value")
      case Right(_) => ()
    }
    SnippetStepResult(step, Seq())
  }

  private def runHiddenStep(step: SnippetStep.Hidden) = {
    val cmd = step.cmd

    logger.debug(s"Running hidden $cmd of line ${step.line}")
    console.runLine(cmd) match {
      case Left(value) =>
        fail(s"scenario ${scenario.name} failed at $step: $value")
      case Right(_) => ()
    }
    SnippetStepResult(step, Seq())
  }

  private def runShellStep(step: SnippetStep.Shell) = {
    val cmd = step.cmd
    val cwd = step.cwd match {
      case Some("CANTON") => File.currentWorkingDirectory
      case Some(path) => File(path)
      case None => shellTemporaryDir
    }

    val processLogger = new SplitBufferedProcessLogger {
      override def out(s: => String): Unit = {
        logger.info(s)
        super.out(s)
      }
      override def err(s: => String): Unit = {
        logger.error(s)
        super.err(s)
      }
    }

    // Capture the variable the command output is assigned to, if any
    val (varCaptureO, actualCommand) = cmd match {
      case ScenarioRunner.varCaptureRegex(variable, cmd) =>
        (Some(variable.trim), cmd.trim)
      case _ =>
        (None, cmd)
    }

    // Strip all indentation on multiline commands
    val trimmedCommand = actualCommand.lines().map(_.stripIndent()).toList.asScala.mkString("\n")

    logger.debug(s"Running shell $trimmedCommand of line ${step.line}")
    val run = Process(
      Seq("/bin/sh", "-c", trimmedCommand),
      cwd = cwd.toJava,
      extraEnv = shellOutputs.toSeq *,
    ).run(processLogger)

    val exitCode = run.exitValue()

    val output = processLogger.output()

    if (exitCode != 0) {
      fail(
        s"Shell command $trimmedCommand exited with $exitCode code:\n Stdout: $output Stderr: ${processLogger
            .error()} \n Env:\n${shellOutputs.map { case (k, v) => s"$k=$v" }.mkString("\n")}"
      )
    }

    // If the result is captured in a variable, record it so we can add it to the environment for next commands
    varCaptureO.foreach { varName =>
      shellOutputs.put(varName, output.stripLineEnd)
    }
    SnippetStepResult(step, Seq())
  }

  private def setupCapture(console: HeadlessConsole): Unit =
    // setup a capture method that runs pretty printing on any pretty printable canton class
    console.runLine("""
val myPrinter = new pprint.PPrinter(additionalHandlers = { case p: com.digitalasset.canton.logging.pretty.PrettyPrinting => import com.digitalasset.canton.logging.pretty.Pretty._; p.toTree }, defaultHeight = 100)
def capture[T: pprint.TPrint](value: => T, ident: String)(implicit classTagT: scala.reflect.ClassTag[T] = null): Unit = {
  val tp = implicitly[pprint.TPrint[T]].render(pprint.TPrintColors.BlackWhite)
  val captured = value
  if(!captured.isInstanceOf[Unit]) {
    val pp = myPrinter.apply(value).plainText
    consoleEnvironment.consoleOutput.info(s"${ident}: $tp = $pp")
  }
}
""")

  private def stripTestName(str: String): String =
    // we don't want the test name in the logger name
    str.replace(":" + getClass.getSimpleName, "")
}

class PocDocumentationIntegrationTest
    extends SphinxDocumentationGenerator(
      File("community/app/src/test/resources/docs/poc.txt"),
      File("community/app/target/poc.txt.result"),
      File(
        "community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"
      ),
    ) {}

class ExternalSigningTopologyTransactionIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/sdk/tutorials/app-dev/external_signing_topology_transaction.rst"),
      File(
        "community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"
      ),
    )

class ExternalSigningOnboardingIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/sdk/tutorials/app-dev/external_signing_onboarding.rst"),
      File(
        "community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"
      ),
    )

class ExternalSigningOnboardingLapiIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/sdk/tutorials/app-dev/external_signing_onboarding_lapi.rst"),
      File(
        "community/app/src/test/resources/examples/08-interactive-submission/interactive-submission.conf"
      ),
    ) {

  override def makeEnvironment: EnvironmentDefinition =
    super.makeEnvironment
      .addConfigTransform(
        _.focus(_.parameters.portsFile).replace(Some("external_party_onboarding.json"))
      )

  override protected def runAtScenarioStart(scenarioName: String)(implicit
      env: TestConsoleEnvironment
  ): Unit =
    env.environment.writePortsFile()
  override def afterAll(): Unit = {
    super.afterAll()
    File("external_party_onboarding.json").delete(swallowIOExceptions = true)
  }
}

class ExternalSigningSubmissionIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/sdk/tutorials/app-dev/external_signing_submission.rst"),
      File(
        "community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"
      ),
    )

class ExternalMultiHostedPartyOnboardingDocsIntegrationTest
    extends SnippetGenerator(
      File(
        "docs-open/src/sphinx/sdk/tutorials/app-dev/external_signing_onboarding_multihosted.rst"
      ),
      File(
        "community/app/src/test/resources/examples/08-interactive-submission/interactive-submission.conf"
      ),
    ) {
  override def makeEnvironment: EnvironmentDefinition =
    super.makeEnvironment
      .addConfigTransform(
        _.focus(_.parameters.portsFile).replace(Some("external_party_onboarding_multi_hosted.json"))
      )
  override protected def runAtScenarioStart(scenarioName: String)(implicit
      env: TestConsoleEnvironment
  ): Unit =
    env.environment.writePortsFile()
  override def afterAll(): Unit = {
    super.afterAll()
    File("external_party_onboarding_multi_hosted.json").delete(swallowIOExceptions = true)
  }
}

class OfflineRootNamespaceIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/secure/keys/namespace_key.rst"),
      File(
        "community/app/src/test/resources/manual-init-example.conf"
      ),
    ) {
  override final def useStaticIdentity: Boolean = false

  override lazy val environmentDefinition: EnvironmentDefinition = {
    val environment = makeEnvironment
    // Needed otherwise the LAPI store integrity checker complains
    environment.copy(testingConfig =
      environment.testingConfig.copy(participantsWithoutLapiVerification = Set("participant1"))
    )
  }
}

class GettingStartedDocumentationIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/tutorials/getting_started.rst"),
      File(
        "community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"
      ),
    ) {

  override protected def beforeAll(): Unit = {
    // we need the dar directory here
    createDarsSimlink()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    cleanUpDarsSimlink()
  }
}

class PackageDarManagementDocumentationIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/operate/packages/packages.rst"),
      File(
        "community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"
      ),
      File(
        "community/app/src/test/resources/examples/07-repair/enable-preview-commands.conf"
      ),
    ) {

  override protected def beforeAll(): Unit = {
    // we need the dar directory here
    createDarsSimlink()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    cleanUpDarsSimlink()
  }
}

class SynchronizerInstallationManual
    extends SnippetGenerator(
      source = File("docs-open/src/sphinx/synchronizer/howtos/operate/bootstrap.rst"),
      config = File(
        "community/app/src/test/resources/examples/02-multiple-sequencers-and-mediators/multiple-sequencers-and-mediators.conf"
      ),
    ) {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val tmpPath = File("tmp/synchronizer-bootstrapping-files")

  override protected def beforeAll(): Unit = {
    if (tmpPath.exists) {
      tmpPath.delete()
    }
    mkdirs(tmpPath)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    if (tmpPath.exists) {
      tmpPath.delete()
    }
    super.afterAll()
  }
}

/* Synchronization is needed because integration tests run in parallel and 3 tests depend on the same DARs
 */
private object DocsGenerationSynchronization {
  private val finished: AtomicInteger = new AtomicInteger(0)
  private val expectedCalls: Int = 3
  private val darsSymLink = File("dars")
  private val darsDir = File("community/common/target/scala-2.13/classes")

  def createDarsSimlink(): Unit =
    blocking {
      DocsGenerationSynchronization.synchronized {
        if (!darsSymLink.exists) {
          ln_s(darsSymLink, darsDir)
        }
      }
    }

  def cleanUpDarsSimlink(): Unit =
    blocking {
      DocsGenerationSynchronization.synchronized {
        if (finished.incrementAndGet() >= expectedCalls) {
          rm(darsSymLink)
        }
      }
    }
}

class UpgradingDocumentationIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/upgrade/index.rst"),
      File(
        "community/app/src/test/resources/upgrade-example-topology.conf"
      ),
    ) {

  // this test does not like static identities
  override final def useStaticIdentity: Boolean = false

  override def makeEnvironment: EnvironmentDefinition =
    super.makeEnvironment.withManualStart
      .withSetup { implicit env =>
        import env.*

        initializeEmptyFlywayDatabase(env)

        sequencers.local.start()
        mediators.local.start()

        NetworkBootstrapper(
          Seq(
            NetworkTopologyDescription(
              "testdomain",
              synchronizerOwners = Seq(sequencer1),
              synchronizerThreshold = PositiveInt.one,
              sequencers = Seq(sequencer1),
              mediators = Seq(mediator1),
            ),
            NetworkTopologyDescription(
              "olddomain",
              synchronizerOwners = Seq(sequencer2),
              synchronizerThreshold = PositiveInt.one,
              sequencers = Seq(sequencer2),
              mediators = Seq(mediator2),
            ),
            NetworkTopologyDescription(
              "newdomain",
              synchronizerOwners = Seq(sequencer3),
              synchronizerThreshold = PositiveInt.one,
              sequencers = Seq(sequencer3),
              mediators = Seq(mediator3),
            ),
          )
        )(env).bootstrap()
      }

  private val fakeMigrationPath = File("tmp/fake-migration")

  registerPlugin(new UsePostgres(loggerFactory))

  private def initializeEmptyFlywayDatabase(
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val dbConfig = env.environment.config.participants("participant").storage match {
      case config: DbConfig.Postgres => config
      case _ => sys.error("expected postgres config")
    }
    val ctx = CloseContext(env)
    val database = DbStorage
      .createDatabase(
        dbConfig,
        dbConfig.numReadConnectionsCanton(
          forParticipant = false,
          withWriteConnectionPool = false,
          withMainConnection = false,
        ),
        forMigration = true,
        scheduler = None,
      )(env.environment.loggerFactory)(ctx)
      .value
      .onShutdown(Left("Should not shutdown"))
      .getOrElse(sys.error("failed to create db"))

    val flyway = Flyway.configure
      .locations("filesystem:" + fakeMigrationPath.toString())
      .dataSource(DbMigrations.createDataSource(database.source))
      .cleanOnValidationError(false)
      .baselineOnMigrate(false)
      .lockRetryCount(60)
      .load()
    flyway.migrate().success shouldBe true
    database.close()
  }

  override protected def beforeAll(): Unit = {
    if (!fakeMigrationPath.exists()) {
      fakeMigrationPath.createDirectories()
      File(
        "community/common/src/main/resources/db/migration/canton/postgres/stable/V1_1__initial.sql"
      ).copyToDirectory(fakeMigrationPath)
    }
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    fakeMigrationPath.delete()
    super.afterAll()
  }
}

class IdentityManagementCookbook
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/operate/identity_management.rst"),
      File("community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"),
      File("community/app/src/test/resources/documentation-snippets/preview-and-repair.conf"),
    ) {

  registerPlugin(new UsePostgres(loggerFactory))

  private val testFiles =
    Seq(File("alice.acs.gz"), File("partyDelegation.bin"), File("rootCert.bin"))

  override protected def afterAll(): Unit = {
    testFiles.foreach(f =>
      if (f.exists) {
        f.delete()
      }
    )
    super.afterAll()
  }
}

class ExternalPartiesDocumentationIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/operate/parties/external_parties.rst"),
      File("community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"),
    )

class PartyReplicationDocumentationIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/operate/parties/party_replication.rst"),
      File("community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"),
      File(
        "community/app/src/test/resources/documentation-snippets/simple-topology-party-replication-extension.conf"
      ),
    ) {

  registerPlugin(new UsePostgres(loggerFactory))

  private val partyReplicationAcsFilename = "party_replication.alice.acs.gz"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    createDarsSimlink()
    val file = File(partyReplicationAcsFilename)
    file.delete(swallowIOExceptions = true)
    file.deleteOnExit(swallowIOExceptions = true)
  }

  override protected def afterAll(): Unit = {
    File(partyReplicationAcsFilename).delete(swallowIOExceptions = true)
    cleanUpDarsSimlink()
    super.afterAll()
  }
}

class SequencerConnectivityDocumentationIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/operate/synchronizers/connectivity.rst"),
      File(
        "community/app/src/test/resources/sequencer-connectivity-documentation.conf"
      ),
    ) {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val target = File("./tls")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    target.delete(swallowIOExceptions = true)
    target.createDirectories()
    target.deleteOnExit(swallowIOExceptions = true)
    val base = "community/app/src/test/resources/tls/"
    (Seq("crt", "key", "pem").map(ending => s"sequencer3-127.0.0.1.$ending") :+ "root-ca.crt")
      .filterNot(name => (target / name).exists)
      .foreach(name => File(base + name).copyToDirectory(target))
  }

  override protected def afterAll(): Unit = {
    target.delete(swallowIOExceptions = true)
    super.afterAll()
  }
}

class DecentralizedPartyIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/operate/parties/decentralized_parties.rst"),
      File("community/app/src/test/resources/decentralized-parties-doc.conf"),
    )

class MediatorInspectionAdminApiIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/synchronizer/howtos/observe/mediator_inspection.rst"),
      File("community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"),
    )

class KeyManagementDocumentationIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/secure/keys/key_management.rst"),
      File("community/app/src/test/resources/key-management-documentation.conf"),
    ) {

  override def makeEnvironment: EnvironmentDefinition = super.makeEnvironment
    .withNetworkBootstrap { implicit env =>
      import env.*

      new NetworkBootstrapper(
        NetworkTopologyDescription(
          "mySynchronizer",
          Seq[InstanceReference](sequencer1, mediator1),
          PositiveInt.one,
          Seq(sequencer1),
          Seq(mediator1),
        )
      )
    }

}

class KeyRestrictionsDocumentationIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/secure/keys/key_restrictions.rst"),
      File("community/app/src/test/resources/key-management-documentation.conf"),
    )

class SequencerNodeHealthSnippetGeneratorTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/synchronizer/howtos/observe/sequencer_health.rst"),
      File(
        "community/app/src/test/resources/sequencer-mediator-health-documentation.conf"
      ),
    ) {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory
    )
  )
}

class MediatorNodeHealthSnippetGeneratorTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/synchronizer/howtos/observe/mediator_health.rst"),
      File(
        "community/app/src/test/resources/sequencer-mediator-health-documentation.conf"
      ),
    ) {
  registerPlugin(new UsePostgres(loggerFactory))
}

class OperateTrafficSnippetGeneratorTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/synchronizer/howtos/operate/traffic.rst"),
      File(
        "community/app/src/test/resources/sequencer-mediator-health-documentation.conf"
      ),
    ) {

  override def makeEnvironment: EnvironmentDefinition =
    super.makeEnvironment
      .withSetup { implicit env =>
        import env.*

        bootstrap.synchronizer(
          synchronizerName = "da",
          sequencers = Seq(sequencer1, sequencer2, sequencer3),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters =
            StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        )
        mediator1.health.wait_for_initialized()
        participant1.synchronizers.connect_local(sequencer1, daName)
        participant2.synchronizers.connect_local(sequencer1, daName)
        participant3.synchronizers.connect_local(sequencer1, daName)
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class HowtosHealthIntegrationTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/observe/health.rst"),
      File("community/app/src/test/resources/examples/01-simple-topology/simple-topology.conf"),
      File("community/app/src/test/resources/documentation-snippets/log-slow-futures.conf"),
      File("community/app/src/test/resources/documentation-snippets/deadlock-detection.conf"),
      File("community/app/src/test/resources/documentation-snippets/exit-on-fatal-failures.conf"),
    ) {

  private val heathDumpOutputDir = File.currentWorkingDirectory
  private val healthDumpFilePattern = "^local-\\d+$".r

  private def healthDumpFilesToDelete: Iterator[File] =
    heathDumpOutputDir.list.filter(f => healthDumpFilePattern.matches(f.name))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    healthDumpFilesToDelete.foreach { f =>
      f.delete(swallowIOExceptions = true)
    }
  }

  override protected def afterAll(): Unit = {
    healthDumpFilesToDelete.foreach { f =>
      f.delete(swallowIOExceptions = true)
    }
    super.afterAll()
  }
}

class PartyManagmentSnippetGeneratorTest
    extends SnippetGenerator(
      File("docs-open/src/sphinx/participant/howtos/operate/parties/parties.rst"),
      File(
        "community/app/src/test/resources/examples/02-multiple-sequencers-and-mediators/multiple-sequencers-and-mediators.conf"
      ),
    ) {

  registerPlugin(new UsePostgres(loggerFactory))
}
