// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.*
import com.daml.ledger.api.v2.interactive.interactive_submission_service.PreparedTransaction
import com.digitalasset.canton.ConsoleScriptRunner
import com.digitalasset.canton.config.CantonCommunityConfig
import com.digitalasset.canton.config.CommunityDbConfig.H2
import com.digitalasset.canton.config.CommunityStorageConfig.Memory
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.InteractiveSubmission
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  IsolatedCommunityEnvironments,
}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ExampleIntegrationTest.{
  ensureSystemProperties,
  interactiveSubmissionV1Configuration,
  referenceConfiguration,
  repairConfiguration,
  simpleTopology,
}
import com.digitalasset.canton.integration.{
  CommunityConfigTransforms,
  CommunityEnvironmentDefinition,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}
import com.digitalasset.canton.platform.apiserver.execution.CommandExecutionResult
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionEncoder
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{HexString, ResourceUtil}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.ImmArray
import monocle.macros.syntax.lens.*
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.io.{BufferedReader, InputStreamReader}
import java.util.UUID
import scala.concurrent.{Future, blocking}

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
  lazy val interactiveSubmissionV1Configuration: File =
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
  registerPlugin(new UseCommunityReferenceBlockSequencer[H2](loggerFactory))
}

sealed abstract class InteractiveSubmissionDemoExampleIntegrationTest
    extends ExampleIntegrationTest(
      interactiveSubmissionV1Configuration / "interactive-submission.conf"
    )
    with ScalaCheckPropertyChecks {

  import com.digitalasset.canton.platform.apiserver.services.command.interactive.InteractiveSubmissionGenerators.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val portsFiles =
    (interactiveSubmissionV1Configuration / "canton_ports.json").deleteOnExit()
  override protected def additionalConfigTransform
      : Seq[CantonCommunityConfig => CantonCommunityConfig] = Seq(
    _.focus(_.parameters.portsFile).replace(Some(portsFiles.pathAsString))
  )

  override def beforeAll(): Unit = {
    assert(
      new ProcessBuilder("./setup.sh")
        .directory(interactiveSubmissionV1Configuration.toJava)
        .start()
        .waitFor() == 0
    )
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // Delete the temp files created by the bootstrap script
    File("participant_id").delete(swallowIOExceptions = true)
    File("domain_id").delete(swallowIOExceptions = true)
  }

  "run the interactive submission demo" in { implicit env =>
    import env.*
    runScript(interactiveSubmissionV1Configuration / "bootstrap.canton")(environment)

    env.environment.writePortsFile()

    val pb = new ProcessBuilder(
      "python3",
      (interactiveSubmissionV1Configuration / "interactive_submission.py").pathAsString,
      "--domain-id",
      sequencer1.domain_id.toProtoPrimitive,
      "--participant-id",
      participant1.id.uid.toProtoPrimitive,
      "run-demo",
    )
      .directory(interactiveSubmissionV1Configuration.toJava)

    // Redirect everything to stdout so we can debug the error if the test fails
    pb.redirectErrorStream(true)
    val process = pb.start()

    // Log the process stdout to the canton log file
    val reader = new BufferedReader(new InputStreamReader(process.getInputStream()))
    val outputToLogF = Future {
      Iterator
        .continually(Option(reader.readLine()))
        .takeWhile(_.isDefined)
        .foreach(_.foreach(logger.info(_)))
    }
    process.waitFor() shouldBe 0
    outputToLogF.futureValue
  }

  def hashFromExamplePythonImplementation(preparedTransaction: PreparedTransaction): String = {
    import scala.sys.process.*

    val tempFile = File.newTemporaryFile(prefix = "prepared_transaction_proto").deleteOnExit()
    ResourceUtil.withResource(tempFile.newFileOutputStream()) { fos =>
      preparedTransaction.writeTo(fos)
    }
    Seq(
      "python3",
      (interactiveSubmissionV1Configuration / "transaction_util.py").pathAsString,
      "--hash",
      tempFile.pathAsString,
    ).!!.stripLineEnd
  }

  def buildV1Hash(
      commandExecutionResult: CommandExecutionResult,
      transactionUUID: UUID,
      mediatorGroup: PositiveInt,
      domainId: DomainId,
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
        domainId,
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
          domainId: DomainId,
          transactionUUID: UUID,
          mediatorGroup: PositiveInt,
      ) =>
        val hashTracer = HashTracer.StringHashTracer(traceSubNodes = true)
        val hashV1 = buildV1Hash(
          commandExecutionResult,
          transactionUUID,
          mediatorGroup,
          domainId,
          hashTracer,
        )

        val result = for {
          encoded <- encoder.serializeCommandExecutionResult(
            commandExecutionResult,
            domainId,
            transactionUUID,
            mediatorGroup.value,
          )
        } yield {

          val referenceHash = hashFromExamplePythonImplementation(encoded)
          val hashEqual = referenceHash == HexString.toHexString(hashV1.value.unwrap)
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
      repairConfiguration / "domain-repair-lost.conf",
      repairConfiguration / "domain-repair-new.conf",
      repairConfiguration / "participant1.conf",
      repairConfiguration / "participant2.conf",
      repairConfiguration / "enable-preview-commands.conf",
    ) {
  "deploy repair user-manual topology and initialize" in { implicit env =>
    ExampleIntegrationTest.ensureSystemProperties("canton-examples.dar-path" -> CantonExamplesPath)
    runScript(repairConfiguration / "domain-repair-init.canton")(env.environment)
  }
}

final class RepairExampleReferenceIntegrationTestDefault extends RepairExampleIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[H2](loggerFactory))
}
