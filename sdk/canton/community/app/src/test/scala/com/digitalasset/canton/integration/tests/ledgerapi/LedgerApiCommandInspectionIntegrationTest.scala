// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.ledger.api.v2.admin.command_inspection_service.CommandState
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.damltests.java.damldebug as T
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.UnknownInformees
import com.digitalasset.canton.examples.java as E
import com.digitalasset.canton.integration.util.UpdateFormatHelpers.getUpdateFormat
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Interpreter
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}
import com.digitalasset.canton.topology.PartyId
import monocle.macros.syntax.lens.*

import scala.collection.concurrent.TrieMap
import scala.concurrent.Promise
import scala.jdk.CollectionConverters.*

class LedgerApiCommandInspectionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory)
      )
      .withSetup { env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)
        participant1.parties.enable("Alice")
      }

  private val parties = new TrieMap[String, PartyId]()
  private def party(name: String)(implicit env: TestConsoleEnvironment): PartyId =
    parties.getOrElseUpdate(name, env.participant1.parties.find(name))

  private def submitAndFail(
      commandId: String,
      cmd: Seq[com.daml.ledger.javaapi.data.Command],
      code: ErrorCode,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val alice = party("Alice")
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        cmd,
        commandId = commandId,
      ),
      _.shouldBeCantonErrorCode(code),
    )
    val failed = participant1.ledger_api.commands.failed(commandId = commandId)
    logger.debug(s"Look at that nice rejection information: $failed")
    failed should have length (1)
    failed.headOption.value.completion.status
      .valueOrFail("no status")
      .message should include(code.id)

  }

  "ledger_api.commands" should {

    "succeed with empty results" in { env =>
      import env.*
      participant1.ledger_api.commands.failed() shouldBe empty
      participant1.ledger_api.commands.status() shouldBe empty
    }

    "fail when invalid command id is used" in { env =>
      import env.*
      loggerFactory.assertLogs(
        a[CommandFailure] shouldBe thrownBy(
          participant1.ledger_api.commands.status("$#@@@@")
        ),
        _.commandFailureMessage should include regex
          s"GrpcClientError: INVALID_ARGUMENT/INVALID_FIELD\\(8,.*\\): The submitted command has a field with invalid value: Invalid field command_id",
      )
    }

    "report failures for invalid commands" when {
      "failing on invalid command" in { implicit env =>
        submitAndFail(
          "invalidcommand",
          (new T.DebugTest(party("Alice").toLf)
            .create()
            .commands()
            .overridePackageId(T.DebugTest.PACKAGE_ID)
            .asScala
            .toSeq),
          NotFound.Package,
        )
      }
      "failing on interpretation error" in { implicit env =>
        val alice = party("Alice")
        submitAndFail(
          "interpretationfailure",
          (new E.iou.Iou(
            alice.toLf,
            alice.toLf,
            new E.iou.Amount(new java.math.BigDecimal(-1), "CHF"),
            Seq.empty.asJava,
          ).create().commands().asScala.toSeq),
          Interpreter.UnhandledException,
        )
      }
      "failing on routing error" in { implicit env =>
        val alice = party("Alice")
        submitAndFail(
          "routingfailure",
          (new E.iou.Iou(
            alice.toLf,
            "notavalid::1220party",
            new E.iou.Amount(new java.math.BigDecimal(1), "CHF"),
            Seq.empty.asJava,
          ).create().commands().asScala.toSeq),
          UnknownInformees,
        )
      }

    }

    "report failures for contention" when {
      "failing on contract-contention" in { implicit env =>
        import env.*
        val alice = party("Alice")
        val commandId = "contentionfailure"
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          (new E.iou.Dummy(alice.toLf).create().commands().asScala.toSeq),
        )
        val cid = participant1.ledger_api.javaapi.state.acs.await(E.iou.Dummy.COMPANION)(alice)

        val have = Promise[Unit]() // to notify that we are holding
        val sequencer = getProgrammableSequencer(sequencer1.name)
        val participant1id = participant1.id // Do not inline as this is a gRPC call
        sequencer.setPolicy_("delay all messages from participant1") { submissionRequest =>
          if (submissionRequest.sender == participant1id && submissionRequest.isConfirmationRequest)
            if (!have.isCompleted) {
              have.trySuccess(())
              // hold until we see the next one
              SendDecision.OnHoldUntil(_.isConfirmationRequest)
            } else {
              SendDecision.Process
            }
          else SendDecision.Process
        }
        def submit(cmdId: String) =
          participant1.ledger_api.javaapi.commands.submit_async(
            Seq(alice),
            (cid.id.exerciseArchive().commands().asScala.toSeq),
            commandId = cmdId,
          )
        submit(commandId)
        have.future.futureValue
        // check that we have a pending command
        val pending = participant1.ledger_api.commands
          .status(commandId, state = CommandState.COMMAND_STATE_PENDING)
          .headOption
          .valueOrFail("where is my pending?")
        pending.completion.commandId shouldBe commandId

        // submit the second command
        submit("willnotfail")

        // now wait until the first one failed
        eventually() {
          participant1.ledger_api.commands
            .failed(commandId = commandId) should not be empty
        }
        logger.debug(
          s"Look at how nice the error info is prepared here ${participant1.ledger_api.commands
              .failed(commandId = commandId)
              .headOption
              .valueOrFail("should be there")}"
        )

        // figure out which transaction spend the contract
        val event = participant1.ledger_api.event_query.by_contract_id(
          cid.id.contractId,
          Seq(alice),
        )
        event.archived should not be empty
        val transactionOffset =
          event.archived
            .flatMap(_.archivedEvent)
            .valueOrFail("have")
            .offset
        // get transaction
        val tx = participant1.ledger_api.updates
          .update_by_offset(transactionOffset, getUpdateFormat(Set(alice)))
          .collect { case tx: UpdateService.TransactionWrapper => tx.transaction }
          .valueOrFail("should have tx")
        // if this succeeds, then we were able to find the right transaction that caused the failure
        tx.commandId shouldBe "willnotfail"

      }
    }

    "report nice stats on success" in { implicit env =>
      import env.*

      val alice = party("Alice")
      val commandId = "good-command"
      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        (new E.iou.Dummy(alice.toLf).create().commands().asScala.toSeq),
        commandId = commandId,
      )
      val completed = participant1.ledger_api.commands
        .status(commandId)
        .headOption
        .valueOrFail("should have")
      logger.info(s"Look at how nice the output is $completed")

    }

  }

}

class LedgerApiCommandInspectionOffIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.enableCommandInspection).replace(false)
        )
      )
      .withSetup { env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)
        participant1.parties.enable("Alice")
      }

  "ledger_api.commands" should {

    "fail when command inspection is turned off" in { env =>
      import env.*
      loggerFactory.assertLogs(
        a[CommandFailure] shouldBe thrownBy(
          participant1.ledger_api.commands.status()
        ),
        _.commandFailureMessage should include regex
          "GrpcServiceUnavailable: UNIMPLEMENTED/Method not found: com.daml.ledger.api.v2.admin.CommandInspectionService/GetCommandStatus",
      )
    }
  }

}
