// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql
import java.time.{Duration => JDuration}

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.{
  SuiteResourceManagementAroundAll,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest
}
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.ledger.client.services.admin.PartyManagementClient
import com.digitalasset.ledger.client.services.commands.CommandClient
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.{SandboxFixture, TestCommands}
import com.digitalasset.platform.services.time.TimeProviderType
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.{AsyncWordSpec, Matchers}
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
    jdbcUrl = Some("jdbc:h2:mem:static_time;db_close_delay=-1"),
    timeProviderType = Some(TimeProviderType.Static),
  )
  def commandClientConfig =
    CommandClientConfiguration(
      config.commandConfig.maxCommandsInFlight,
      config.commandConfig.maxParallelSubmissions,
      overrideTtl = true,
      ttl = JDuration.ofMillis(2000),
    )
  private val applicationId = "transaction-stream-termination-test"

  private def newTransactionClient(ledgerId: domain.LedgerId) =
    new TransactionClient(ledgerId, TransactionServiceGrpc.stub(channel))

  private def newPartyManagement(ledgerId: domain.LedgerId) =
    new PartyManagementClient(PartyManagementServiceGrpc.stub(channel))

  private def newCommandSubmissionClient(ledgerId: domain.LedgerId) =
    new CommandClient(
      CommandSubmissionServiceGrpc.stub(channel),
      CommandCompletionServiceGrpc.stub(channel),
      ledgerId,
      applicationId,
      commandClientConfig,
      None
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
              Command(Command.Command.Create(CreateCommand(
                Some(templateIds.dummy),
                Some(Record(
                  Some(templateIds.dummy),
                  Seq(RecordField(
                    "operator",
                    Option(Value(Value.Sum.Party(M.submitAndWaitRequest.commands.get.party)))))))
              ))))
          )

        val commandClient = newCommandSubmissionClient(actualLedgerId)
        val txClient = newTransactionClient(actualLedgerId)
        val partyManagementClient = newPartyManagement(actualLedgerId)

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
