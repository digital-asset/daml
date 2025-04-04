// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import better.files.File
import com.daml.ledger.api.v2.interactive.interactive_submission_service.PreparedTransaction
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.InteractiveSubmission
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.interactiveSubmissionV1Folder
import com.digitalasset.canton.integration.{CommunityIntegrationTest, ConfigTransform}
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.TransactionData
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionEncoder
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.{ConcurrentBufferedLogger, HexString, ResourceUtil}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.ImmArray
import monocle.macros.syntax.lens.*
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.util.UUID
import scala.sys.process.Process

sealed abstract class InteractiveSubmissionDemoExampleIntegrationTest
    extends ExampleIntegrationTest(
      interactiveSubmissionV1Folder / "interactive-submission.conf"
    )
    with CommunityIntegrationTest
    with ScalaCheckPropertyChecks {

  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.platform.apiserver.services.command.interactive.InteractiveSubmissionGenerators.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val portsFiles =
    (interactiveSubmissionV1Folder / "canton_ports.json").deleteOnExit()
  override protected def additionalConfigTransform: Seq[ConfigTransform] = Seq(
    _.focus(_.parameters.portsFile).replace(Option(portsFiles.pathAsString))
  )
  private def mkProcessLogger(logErrors: Boolean = true) = new ConcurrentBufferedLogger {
    override def out(s: => String): Unit = {
      logger.info(s)
      super.out(s)
    }
    override def err(s: => String): Unit = {
      if (logErrors) logger.error(s)
      super.err(s)
    }
  }
  private val processLogger = mkProcessLogger()

  private def runAndAssertCommandSuccess(
      pb: scala.sys.process.ProcessBuilder,
      processLogger: ConcurrentBufferedLogger,
  ): Unit = {
    val exitCode = pb.!(processLogger)
    if (exitCode != 0) {
      fail(s"Command failed:\n\n ${processLogger.output()}")
    }
  }

  private def runAndAssertCommandFailure(
      pb: scala.sys.process.ProcessBuilder,
      processLogger: ConcurrentBufferedLogger,
      expectedFailure: String,
  ): Unit = {
    val exitCode = pb.!(processLogger)
    if (exitCode == 0) {
      fail(s"Expected command failure but it succeeded")
    } else {
      processLogger.output() should include(expectedFailure)
    }
  }

  override def beforeAll(): Unit = {
    runAndAssertCommandSuccess(
      Process(Seq("./setup.sh"), cwd = interactiveSubmissionV1Folder.toJava),
      processLogger,
    )
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // Delete the temp files created by the test
    List(
      File("participant_id"),
      File("synchronizer_id"),
      interactiveSubmissionV1Folder / "com",
      interactiveSubmissionV1Folder / "google",
      interactiveSubmissionV1Folder / "scalapb",
    ).foreach(_.delete(swallowIOExceptions = true))
  }

  private def setupTest(implicit env: FixtureParam): Unit = {
    import env.environment
    runScript(interactiveSubmissionV1Folder / "bootstrap.canton")(environment)
    environment.writePortsFile()
  }

  "run the interactive submission demo" in { implicit env =>
    import env.*
    setupTest

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
          "-a", // Automatically accept all transactions (by default the script stops to ask users to explicitly confirm)
        ),
        cwd = interactiveSubmissionV1Folder.toJava,
      ),
      processLogger,
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
    ).!!.stripLineEnd
  }

  def buildV1Hash(
      preparedTransactionData: TransactionData,
      transactionUUID: UUID,
      mediatorGroup: PositiveInt,
      synchronizerId: SynchronizerId,
      hashTracer: HashTracer,
  ) =
    InteractiveSubmission.computeVersionedHash(
      HashingSchemeVersion.V1,
      preparedTransactionData.transaction,
      InteractiveSubmission.TransactionMetadataForHashing.create(
        preparedTransactionData.submitterInfo.actAs.toSet,
        preparedTransactionData.submitterInfo.commandId,
        transactionUUID,
        mediatorGroup.value,
        synchronizerId,
        Option.when(preparedTransactionData.dependsOnLedgerTime)(
          preparedTransactionData.transactionMeta.ledgerEffectiveTime
        ),
        preparedTransactionData.transactionMeta.submissionTime,
        preparedTransactionData.inputContracts,
      ),
      preparedTransactionData.transactionMeta.optNodeSeeds
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
          preparedTransactionData: TransactionData,
          synchronizerId: SynchronizerId,
          transactionUUID: UUID,
          mediatorGroup: PositiveInt,
      ) =>
        val hashTracer = HashTracer.StringHashTracer(traceSubNodes = true)
        val expectedHash = buildV1Hash(
          preparedTransactionData,
          transactionUUID,
          mediatorGroup,
          synchronizerId,
          hashTracer,
        )

        val result = for {
          encoded <- encoder.serializeCommandInterpretationResult(
            preparedTransactionData,
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

  "run the interactive topology bash demo" in { implicit env =>
    import env.*
    setupTest

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "./interactive_topology_example.sh",
          participant1.config.adminApi.address + ":" + participant1.config.adminApi.port.unwrap.toString,
          sequencer1.synchronizer_id.toProtoPrimitive,
        ),
        cwd = interactiveSubmissionV1Folder.toJava,
      ),
      processLogger,
    )
  }

  "do error handling in bash" in { implicit env =>
    import env.*
    runScript(interactiveSubmissionV1Folder / "bootstrap.canton")(environment)

    env.environment.writePortsFile()

    runAndAssertCommandFailure(
      Process(
        Seq(
          "./interactive_topology_example.sh",
          participant1.config.adminApi.address + ":" + participant1.config.adminApi.port.unwrap.toString,
          "invalid_Store",
        ),
        cwd = interactiveSubmissionV1Folder.toJava,
      ),
      processLogger,
      "Invalid unique identifier `invalid_Store` with missing namespace",
    )
  }

  "run the interactive topology python demo" in { implicit env =>
    import env.*
    setupTest

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "python",
          (interactiveSubmissionV1Folder / "interactive_topology_example.py").pathAsString,
          "--synchronizer-id",
          sequencer1.synchronizer_id.toProtoPrimitive,
          "run-demo",
        ),
        cwd = interactiveSubmissionV1Folder.toJava,
      ),
      processLogger,
    )
  }

  "do error handling in python" in { implicit env =>
    setupTest

    runAndAssertCommandFailure(
      Process(
        Seq(
          "python",
          (interactiveSubmissionV1Folder / "interactive_topology_example.py").pathAsString,
          "--synchronizer-id",
          "invalid_Store",
          "run-demo",
        ),
        cwd = interactiveSubmissionV1Folder.toJava,
      ),
      mkProcessLogger(logErrors = false),
      "Invalid unique identifier `invalid_Store` with missing namespace",
    )
  }
}

final class InteractiveSubmissionDemoExampleIntegrationTestH2
    extends InteractiveSubmissionDemoExampleIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}
