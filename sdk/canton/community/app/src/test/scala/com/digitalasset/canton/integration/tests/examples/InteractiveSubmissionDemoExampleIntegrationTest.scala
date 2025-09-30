// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import better.files.File
import com.daml.ledger.api.v2.interactive.interactive_submission_service.PreparedTransaction
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.interactiveSubmissionFolder
import com.digitalasset.canton.integration.{CommunityIntegrationTest, ConfigTransform}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.command.interactive.GeneratorsInteractiveSubmission
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.{
  PrepareTransactionData,
  PreparedTransactionEncoder,
}
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.util.{ConcurrentBufferedLogger, HexString, ResourceUtil}
import com.digitalasset.canton.version.{CommonGenerators, HashingSchemeVersion}
import monocle.macros.syntax.lens.*
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.sys.process.Process

sealed abstract class InteractiveSubmissionDemoExampleIntegrationTest
    extends ExampleIntegrationTest(
      interactiveSubmissionFolder / "interactive-submission.conf"
    )
    with CommunityIntegrationTest
    with ScalaCheckPropertyChecks {

  private lazy val generators = new CommonGenerators(testedProtocolVersion)
  private lazy val generatorsInteractiveSubmission =
    new GeneratorsInteractiveSubmission(generators.lf, generators.topology)

  import generatorsInteractiveSubmission.*

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val portsFiles =
    (interactiveSubmissionFolder / "canton_ports.json").deleteOnExit()
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

  override def beforeAll(): Unit = {
    runAndAssertCommandSuccess(
      Process(Seq("./setup.sh"), cwd = interactiveSubmissionFolder.toJava),
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
      interactiveSubmissionFolder / "com",
      interactiveSubmissionFolder / "google",
      interactiveSubmissionFolder / "scalapb",
      File("canton_ports.json"),
      File("private_key.der"),
      File("public_key.der"),
    ).foreach(_.delete(swallowIOExceptions = true))
  }

  private def setupTest(implicit env: FixtureParam): Unit = {
    import env.environment
    runScript(interactiveSubmissionFolder / "bootstrap.canton")(environment)
    environment.writePortsFile()
  }

  "run the interactive submission demo" in { implicit env =>
    import env.*
    setupTest

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "python",
          (interactiveSubmissionFolder / "interactive_submission.py").pathAsString,
          "--synchronizer-id",
          sequencer1.synchronizer_id.toProtoPrimitive,
          "--participant-id",
          participant1.id.uid.toProtoPrimitive,
          "run-demo",
          "-a", // Automatically accept all transactions (by default the script stops to ask users to explicitly confirm)
        ),
        cwd = interactiveSubmissionFolder.toJava,
      ),
      processLogger,
    )
  }

  "run the multi-hosted external party onboarding demo" in { implicit env =>
    import env.*
    runScript(interactiveSubmissionFolder / "multi-hosted.canton")(environment)
    environment.writePortsFile()

    val privateKeyFile = File.newTemporaryFile()

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "python",
          (interactiveSubmissionFolder / "external_party_onboarding_multi_hosting.py").pathAsString,
          "--party-name",
          "alice",
          "--private-key-file",
          privateKeyFile.pathAsString,
          "--threshold",
          "2",
          "--admin-endpoint",
          s"${participant1.config.adminApi.address}:${participant1.config.adminApi.port.unwrap}",
          s"${participant2.config.adminApi.address}:${participant2.config.adminApi.port.unwrap}",
          "--synchronizer-id",
          sequencer1.synchronizer_id.toProtoPrimitive,
          "-a", // Automatically accept all transactions (by default the script stops to ask users to explicitly confirm)
          "onboard",
        ),
        cwd = interactiveSubmissionFolder.toJava,
      ),
      processLogger,
    )

    val alice = eventually() {
      participant1.parties.list().find(_.party.toProtoPrimitive.contains("alice")).value.party
    }

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "python",
          (interactiveSubmissionFolder / "external_party_onboarding_multi_hosting.py").pathAsString,
          "--private-key-file",
          privateKeyFile.pathAsString,
          "--threshold",
          "2",
          "--admin-endpoint",
          s"${participant3.config.adminApi.address}:${participant3.config.adminApi.port.unwrap}",
          "--synchronizer-id",
          sequencer1.synchronizer_id.toProtoPrimitive,
          "-a", // Automatically accept all transactions (by default the script stops to ask users to explicitly confirm)
          "update",
          "--party-id",
          alice.toProtoPrimitive,
          "--participant-permission",
          "confirmation",
        ),
        cwd = interactiveSubmissionFolder.toJava,
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
        "daml_transaction_util.py",
        "--hash",
        tempFile.pathAsString,
      ),
      cwd = interactiveSubmissionFolder.toJava,
    ).!!.stripLineEnd
  }

  def buildV2Hash(
      preparedTransactionData: PrepareTransactionData,
      hashTracer: HashTracer,
  ) =
    preparedTransactionData.computeHash(HashingSchemeVersion.V2, testedProtocolVersion, hashTracer)

  "produce hash consistent with canton implementation" in { implicit env =>
    import env.*
    forAll {
      (
        preparedTransactionData: PrepareTransactionData,
      ) =>
        val hashTracer = HashTracer.StringHashTracer(traceSubNodes = true)
        val expectedHash = buildV2Hash(
          preparedTransactionData,
          hashTracer,
        )

        val result = for {
          encoded <- encoder.encode(
            preparedTransactionData
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
        cwd = interactiveSubmissionFolder.toJava,
      ),
      processLogger,
    )
  }

  "do error handling in bash" in { implicit env =>
    import env.*
    runScript(interactiveSubmissionFolder / "bootstrap.canton")(environment)

    env.environment.writePortsFile()

    runAndAssertCommandFailure(
      Process(
        Seq(
          "./interactive_topology_example.sh",
          participant1.config.adminApi.address + ":" + participant1.config.adminApi.port.unwrap.toString,
          "invalid_Store",
        ),
        cwd = interactiveSubmissionFolder.toJava,
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
          (interactiveSubmissionFolder / "interactive_topology_example.py").pathAsString,
          "--synchronizer-id",
          sequencer1.synchronizer_id.toProtoPrimitive,
          "run-demo",
        ),
        cwd = interactiveSubmissionFolder.toJava,
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
          (interactiveSubmissionFolder / "interactive_topology_example.py").pathAsString,
          "--synchronizer-id",
          "invalid_Store",
          "run-demo",
        ),
        cwd = interactiveSubmissionFolder.toJava,
      ),
      mkProcessLogger(logErrors = false),
      "Invalid unique identifier `invalid_Store` with missing namespace",
    )
  }

  "run external party onboarding via ledger api" in { implicit env =>
    setupTest
    runAndAssertCommandSuccess(
      Process(
        Seq(
          "./external_party_onboarding.sh"
        ),
        cwd = interactiveSubmissionFolder.toJava,
      ),
      processLogger,
    )

  }

}

final class InteractiveSubmissionDemoExampleIntegrationTestH2
    extends InteractiveSubmissionDemoExampleIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}
