// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.*
import com.daml.ledger.api.v2.interactive.interactive_submission_service.PreparedTransaction
import com.digitalasset.canton.ConsoleScriptRunner
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.StorageConfig.Memory
import com.digitalasset.canton.config.{CantonCommunityConfig, DbConfig}
import com.digitalasset.canton.crypto.InteractiveSubmission
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  IsolatedCommunityEnvironments,
}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ExampleIntegrationTest.*
import com.digitalasset.canton.integration.{
  CommunityConfigTransforms,
  CommunityEnvironmentDefinition,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}
import com.digitalasset.canton.platform.apiserver.execution.CommandExecutionResult
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionEncoder
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{HexString, ResourceUtil}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.ImmArray
import monocle.macros.syntax.lens.*
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.util.UUID
import scala.concurrent.blocking
import scala.sys.process.{Process, ProcessLogger}

abstract class ExampleIntegrationTest(configPaths: File*)
    extends CommunityIntegrationTest
    with IsolatedCommunityEnvironments
    with HasConsoleScriptRunner {

  protected def additionalConfigTransform: Seq[CantonCommunityConfig => CantonCommunityConfig] =
    Seq.empty

  override lazy val environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition
      .fromFiles(configPaths*)
      .addConfigTransforms(
        // lets not share databases
        CommunityConfigTransforms.uniqueH2DatabaseNames,
        _.focus(_.monitoring.tracing.propagation).replace(TracingConfig.Propagation.Enabled),
        CommunityConfigTransforms.updateAllParticipantConfigs { case (_, config) =>
          // to make sure that the picked up time for the snapshot is the most recent one
          config
            .focus(_.parameters.reassignmentTimeProofFreshnessProportion)
            .replace(NonNegativeInt.zero)
        },
        CommunityConfigTransforms.uniquePorts,
      )
      .addConfigTransforms(additionalConfigTransform*)
}

trait HasConsoleScriptRunner { this: NamedLogging =>
  import org.scalatest.EitherValues.*
  def runScript(scriptPath: File)(implicit env: Environment): Unit =
    ConsoleScriptRunner.run(env, scriptPath.toJava, logger = logger).value.discard
}

object ExampleIntegrationTest {
  lazy val examplesPath: File = "community" / "app" / "src" / "pack" / "examples"
  lazy val simpleTopology: File = examplesPath / "01-simple-topology"
  lazy val referenceConfiguration: File = "community" / "app" / "src" / "pack" / "config"
  lazy val composabilityConfiguration: File = examplesPath / "05-composability"
  lazy val repairConfiguration: File = examplesPath / "07-repair"
  lazy val interactiveSubmissionV1Folder: File =
    examplesPath / "08-interactive-submission/v1"
  lazy val advancedConfTestEnv: File =
    "community" / "app" / "src" / "test" / "resources" / "advancedConfDef.env"

  def ensureSystemProperties(kvs: (String, String)*): Unit = blocking(synchronized {
    kvs.foreach { case (key, value) =>
      Option(System.getProperty(key)) match {
        case Some(oldValue) =>
          require(
            oldValue == value,
            show"Trying to set incompatible system properties for ${key.singleQuoted}. Old: ${oldValue.doubleQuoted}, new: ${value.doubleQuoted}.",
          )
        case None =>
          System.setProperty(key, value)
      }
    }
  })
}

sealed abstract class SimplePingExampleIntegrationTest
    extends ExampleIntegrationTest(simpleTopology / "simple-topology.conf") {

  "run simple-ping.canton successfully" in { implicit env =>
    import env.*
    val port = sequencer1.sequencerConnection.endpoints.head.port.unwrap.toString
    ensureSystemProperties(("canton-examples.da-port", port))
    runScript(simpleTopology / "simple-ping.canton")(environment)
  }
}

final class SimplePingExampleReferenceIntegrationTestDefault
    extends SimplePingExampleIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

sealed abstract class InteractiveSubmissionDemoExampleIntegrationTest
    extends ExampleIntegrationTest(
      interactiveSubmissionV1Folder / "interactive-submission.conf"
    )
    with ScalaCheckPropertyChecks {

  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.platform.apiserver.services.command.interactive.InteractiveSubmissionGenerators.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val portsFiles =
    (interactiveSubmissionV1Folder / "canton_ports.json").deleteOnExit()
  override protected def additionalConfigTransform
      : Seq[CantonCommunityConfig => CantonCommunityConfig] = Seq(
    _.focus(_.parameters.portsFile).replace(Some(portsFiles.pathAsString))
  )
  private val pythonVenv = s"${interactiveSubmissionV1Folder.pathAsString}/.venv"
  private var pythonEnv: Seq[(String, String)] = Seq.empty
  private val processLogger = new ProcessLogger {
    override def out(s: => String): Unit = logger.info(s"python script: $s")
    override def err(s: => String): Unit = logger.error(s"python script: $s")
    override def buffer[T](f: => T): T = f
  }

  private def runAndAssertCommandSuccess(process: scala.sys.process.ProcessBuilder) = assert(
    process.!(processLogger) == 0
  )

  override def beforeAll(): Unit = {
    // Create virtual env to make sure we run the demo with the exact package versions we want
    runAndAssertCommandSuccess(
      Process(Seq("python", "-m", "venv", ".venv"), interactiveSubmissionV1Folder.toJava)
    )
    // Install dependencies
    runAndAssertCommandSuccess(
      Process(
        Seq(s"$pythonVenv/bin/pip", "install", "-r", "requirements.txt"),
        interactiveSubmissionV1Folder.toJava,
      )
    )
    // This allows to be agnostic of the python version as the folder contains the python minor version (e.g: python3.10)
    val packagesFolder = File(s"$pythonVenv/lib").children.next() / "site-packages"
    // Create a PYTHONPATH env var pointing to the venv to use in priority those packages
    pythonEnv = Seq(
      "PYTHONPATH" -> s"${packagesFolder.pathAsString}${sys.env.get("PYTHONPATH").map(":" + _).getOrElse("")}"
    )
    // Run the setup - set the PYTHONPATH to ensure we use the packages in the venv
    runAndAssertCommandSuccess(
      Process(Seq("./setup.sh"), cwd = interactiveSubmissionV1Folder.toJava, extraEnv = pythonEnv*)
    )
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // Delete the temp files created by the test
    List(
      File("participant_id"),
      File("synchronizer_id"),
      File(pythonVenv),
      interactiveSubmissionV1Folder / "com",
      interactiveSubmissionV1Folder / "google",
      interactiveSubmissionV1Folder / "scalapb",
    ).foreach(_.delete(swallowIOExceptions = true))
  }

  "run the interactive submission demo" in { implicit env =>
    import env.*
    runScript(interactiveSubmissionV1Folder / "bootstrap.canton")(environment)

    env.environment.writePortsFile()

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "python",
          (interactiveSubmissionV1Folder / "interactive_submission.py").pathAsString,
          "--synchronizer-id",
          sequencer1.synchronizer_id.toProtoPrimitive,
          "--participant-id",
          participant1.id.uid.toProtoPrimitive,
          "run-demo",
        ),
        cwd = interactiveSubmissionV1Folder.toJava,
        extraEnv = pythonEnv *,
      )
    )
  }

  def hashFromExamplePythonImplementation(preparedTransaction: PreparedTransaction): String = {
    val tempFile = File.newTemporaryFile(prefix = "prepared_transaction_proto").deleteOnExit()
    ResourceUtil.withResource(tempFile.newFileOutputStream()) { fos =>
      preparedTransaction.writeTo(fos)
    }
    Process(
      Seq(
        "python",
        "transaction_util.py",
        "--hash",
        tempFile.pathAsString,
      ),
      cwd = interactiveSubmissionV1Folder.toJava,
      extraEnv = pythonEnv *,
    ).!!.stripLineEnd
  }

  def buildV1Hash(
      commandExecutionResult: CommandExecutionResult,
      transactionUUID: UUID,
      mediatorGroup: PositiveInt,
      synchronizerId: SynchronizerId,
      hashTracer: HashTracer,
  ) =
    InteractiveSubmission.computeVersionedHash(
      HashingSchemeVersion.V1,
      commandExecutionResult.transaction,
      InteractiveSubmission.TransactionMetadataForHashing.createFromDisclosedContracts(
        commandExecutionResult.submitterInfo.actAs.toSet,
        commandExecutionResult.submitterInfo.commandId,
        transactionUUID,
        mediatorGroup.value,
        synchronizerId,
        Option.when(commandExecutionResult.dependsOnLedgerTime)(
          commandExecutionResult.transactionMeta.ledgerEffectiveTime
        ),
        commandExecutionResult.transactionMeta.submissionTime,
        commandExecutionResult.processedDisclosedContracts,
      ),
      commandExecutionResult.transactionMeta.optNodeSeeds
        .getOrElse(ImmArray.empty)
        .toList
        .toMap,
      testedProtocolVersion,
      hashTracer,
    )

  "produce hash consistent with canton implementation" in { implicit env =>
    import env.*
    forAll {
      (
          commandExecutionResult: CommandExecutionResult,
          synchronizerId: SynchronizerId,
          transactionUUID: UUID,
          mediatorGroup: PositiveInt,
      ) =>
        val hashTracer = HashTracer.StringHashTracer(traceSubNodes = true)
        val expectedHash = buildV1Hash(
          commandExecutionResult,
          transactionUUID,
          mediatorGroup,
          synchronizerId,
          hashTracer,
        )

        val result = for {
          encoded <- encoder.serializeCommandExecutionResult(
            commandExecutionResult,
            synchronizerId,
            transactionUUID,
            mediatorGroup.value,
          )
        } yield {

          val pythonHash = hashFromExamplePythonImplementation(encoded)
          val hashEqual = pythonHash == HexString.toHexString(expectedHash.value.unwrap)
          if (!hashEqual) {
            // helpful for debugging, only printed if the test fails
            logger.debug(hashTracer.result)
          }
          assert(hashEqual)
          succeed
        }

        timeouts.default.await_("Encoding")(result)
    }
  }
}

final class InteractiveSubmissionDemoExampleIntegrationTestInMemory
    extends InteractiveSubmissionDemoExampleIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[Memory](loggerFactory))
}

sealed abstract class RepairExampleIntegrationTest
    extends ExampleIntegrationTest(
      referenceConfiguration / "storage" / "h2.conf",
      repairConfiguration / "synchronizer-repair-lost.conf",
      repairConfiguration / "synchronizer-repair-new.conf",
      repairConfiguration / "participant1.conf",
      repairConfiguration / "participant2.conf",
      repairConfiguration / "enable-preview-commands.conf",
    ) {
  "deploy repair user-manual topology and initialize" in { implicit env =>
    ExampleIntegrationTest.ensureSystemProperties("canton-examples.dar-path" -> CantonExamplesPath)
    runScript(repairConfiguration / "synchronizer-repair-init.canton")(env.environment)
  }
}

final class RepairExampleReferenceIntegrationTestDefault extends RepairExampleIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}
