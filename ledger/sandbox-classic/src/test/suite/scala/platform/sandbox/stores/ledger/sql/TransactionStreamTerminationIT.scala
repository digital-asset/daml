// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql
import java.time.{Duration => JDuration}

import akka.stream.scaladsl.Sink
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.{SuiteResourceManagementAroundAll, MockMessages => M}
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.daml.ledger.api.v1.commands.{Command, CreateCommand}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.ledger.client.services.admin.PartyManagementClient
import com.daml.ledger.client.services.commands.CommandClient
import com.daml.ledger.client.services.transactions.TransactionClient
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.{SandboxFixture, TestCommands}
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

class TransactionStreamTerminationIT
    extends AsyncWordSpec
    with Matchers
    with ScalaFutures
    with TestCommands
    with SandboxFixture
    with SuiteResourceManagementAroundAll {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(scaled(Span(15000, Millis)), scaled(Span(150, Millis)))

  override protected def config: SandboxConfig = super.config.copy(
    // TODO: this class does not use SandboxBackend.H2Database, the jdbc url provided here will have no effect
    jdbcUrl = Some("jdbc:h2:mem:static_time;db_close_delay=-1"),
    timeProviderType = Some(TimeProviderType.Static),
  )

  private def commandClientConfig =
    CommandClientConfiguration(
      maxCommandsInFlight = config.commandConfig.maxCommandsInFlight,
      maxParallelSubmissions = config.maxParallelSubmissions,
      defaultDeduplicationTime = JDuration.ofSeconds(30),
    )

  private val applicationId = "transaction-stream-termination-test"

  private def newTransactionClient(ledgerId: domain.LedgerId) =
    new TransactionClient(ledgerId, TransactionServiceGrpc.stub(channel))

  private def newPartyManagement() =
    new PartyManagementClient(PartyManagementServiceGrpc.stub(channel))

  private def newCommandSubmissionClient(ledgerId: domain.LedgerId) =
    new CommandClient(
      CommandSubmissionServiceGrpc.stub(channel),
      CommandCompletionServiceGrpc.stub(channel),
      ledgerId,
      applicationId,
      commandClientConfig,
    )

  "TransactionService" when {
    "streaming transactions until ledger end with no corresponding ledger entries" should {
      "terminate properly" in {

        val actualLedgerId = ledgerId()
        val begin =
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
        val end = LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))
        val submitRequest: SubmitRequest =
          M.submitRequest.update(
            _.commands.ledgerId := actualLedgerId.unwrap,
            _.commands.applicationId := applicationId,
            _.commands.commands := List(
              Command.of(
                Command.Command.Create(
                  CreateCommand(
                    Some(templateIds.dummy),
                    Some(
                      Record(
                        Some(templateIds.dummy),
                        Seq(
                          RecordField(
                            "operator",
                            Option(
                              Value(Value.Sum.Party(M.submitAndWaitRequest.commands.get.party))
                            ),
                          )
                        ),
                      )
                    ),
                  )
                )
              )
            ),
          )

        val commandClient = newCommandSubmissionClient(actualLedgerId)
        val txClient = newTransactionClient(actualLedgerId)
        val partyManagementClient = newPartyManagement()

        def getLedgerEnd = txClient.getLedgerEnd().map(_.getOffset.value.absolute.get.toLong)
        for {
          endAtStartOfTest <- getLedgerEnd
          // first we create a transaction
          _ <- commandClient.trackSingleCommand(submitRequest)
          endAfterSubmission <- getLedgerEnd
          // next we allocate a party. this causes the ledger end to move without a corresponding ledger entry
          _ <- partyManagementClient.allocateParty(None, None)
          endAfterParty <- getLedgerEnd
          // without the fix, this call will cause the test to run into a timeout
          transactions <- txClient
            .getTransactions(begin, Some(end), M.transactionFilter)
            .runWith(Sink.seq)
        } yield {
          endAtStartOfTest should be < endAfterSubmission
          endAfterSubmission should be < endAfterParty
          transactions should have size 1
        }
      }
    }
  }

}
