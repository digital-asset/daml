// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.command

import java.time
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.testing.utils.{MockMessages, SuiteResourceManagementAroundAll}
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest
}
import com.digitalasset.ledger.api.v1.commands.Command.Command.{Create, Exercise}
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  SetTimeRequest,
  TimeServiceGrpc
}
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionsRequest,
  TransactionServiceGrpc
}
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.Value.Sum.Party
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.ledger.client.services.commands.CommandClient
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.platform.sandbox.services.{SandboxFixture, TestCommands}
import io.grpc.Status
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

final class CommandStaticTimeIT
    extends AsyncWordSpec
    with Matchers
    with TestCommands
    with SandboxFixture
    with ScalaFutures
    with SuiteResourceManagementAroundAll
    with OptionValues {

  private val tenDays: time.Duration = java.time.Duration.ofDays(10L)

  private val ttlSeconds = 20L

  private val newCommandId: () => String = {
    val atomicInteger = new AtomicInteger()
    () =>
      atomicInteger.incrementAndGet().toString
  }

  private lazy val unwrappedLedgerId = ledgerId().unwrap

  private def createCommandClient()(implicit mat: Materializer): Future[CommandClient] =
    StaticTime
      .updatedVia(TimeServiceGrpc.stub(channel), unwrappedLedgerId)
      .recover { case NonFatal(_) => TimeProvider.UTC }(DirectExecutionContext)
      .map(tp =>
        new CommandClient(
          CommandSubmissionServiceGrpc.stub(channel),
          CommandCompletionServiceGrpc.stub(channel),
          ledgerId(),
          MockMessages.applicationId,
          CommandClientConfiguration(
            maxCommandsInFlight = 1,
            maxParallelSubmissions = 1,
            overrideTtl = true,
            ttl = java.time.Duration.ofSeconds(30)),
          None
        ).withTimeProvider(Some(tp)))(DirectExecutionContext)

  private lazy val submitRequest: SubmitRequest =
    MockMessages.submitRequest.update(
      _.commands.ledgerId := unwrappedLedgerId,
      _.commands.commands := List(
        CreateCommand(
          Some(templateIds.dummy),
          Some(
            Record(
              Some(templateIds.dummy),
              Seq(RecordField(
                "operator",
                Option(
                  Value(Value.Sum.Party(MockMessages.submitAndWaitRequest.commands.get.party)))))))
        ).wrap)
    )

  implicit class CommandClientOps(commandClient: CommandClient) {
    def send(request: SubmitRequest): Future[Completion] =
      commandClient
        .withTimeProvider(None)
        .trackSingleCommand(request)
  }

  private def submissionWithTime(time: Instant): SubmitRequest = {
    val let = fromInstant(time)
    val mrt = let.update(_.seconds.modify(_ + ttlSeconds))
    submitRequest
      .update(
        _.commands.ledgerEffectiveTime := let,
        _.commands.maximumRecordTime := mrt,
        _.commands.ledgerId := unwrappedLedgerId,
        _.commands.commandId := newCommandId()
      )
  }

  // format: off
  private def dummyCreateRequest(time: Instant, templateId: Identifier) =
    submissionWithTime(time)
      .update(_.commands.commands := List(
        Command(
          Create(
            CreateCommand(Some(templateId),
              Some(Record(Some(templateId), List(RecordField("operator", Some(Value(Party("party"))))))))))
      ))
  // format: on

  private def dummyExerciseRequest(time: Instant, templateId: Identifier, contractId: String) =
    submissionWithTime(time)
      .update(
        _.commands.commands := List(
          Command(
            Exercise(
              ExerciseCommand(
                Some(templateId),
                contractId,
                "DummyChoice1",
                Some(Value(Sum.Record(Record()))))))
        ))

  private def extractContractId(transaction: Transaction): String = {
    val events = for {
      event <- transaction.events
    } yield event.event

    events.collect {
      case Created(CreatedEvent(contractId, _, _, _, _, _, _, _, _)) => contractId
    }.head
  }

  private def timeSource =
    ClientAdapter.serverStreaming(
      GetTimeRequest(unwrappedLedgerId),
      TimeServiceGrpc.stub(channel).getTime)

  "Command and Time Services" when {

    "ledger effective time is within acceptance window" should {

      "commands should be accepted" in {
        for {
          commandClient <- createCommandClient()
          completion <- commandClient
            .withTimeProvider(None)
            .trackSingleCommand(
              SubmitRequest(
                Some(submitRequest.getCommands
                  .withLedgerId(unwrappedLedgerId)
                  .withCommandId(newCommandId()))))
        } yield {
          completion.status.value should have('code (Status.OK.getCode.value()))
        }
      }

      "ledger effective time is outside of acceptance window (in the future)" should {

        "reject commands with ABORTED" in {
          for {
            commandClient <- createCommandClient()
            getTimeResponse <- timeSource.runWith(Sink.head)
            currentTime = toInstant(getTimeResponse.getCurrentTime)
            completion <- commandClient.send(submissionWithTime(currentTime.plus(tenDays)))
          } yield {
            completion.status.value should have('code (Status.ABORTED.getCode.value()))
            completion.status.value.message shouldEqual "TRANSACTION_OUT_OF_TIME_WINDOW: [" +
              "currentTime:1970-01-01T00:00:00Z, " +
              "ledgerEffectiveTime:1970-01-11T00:00:00Z, " +
              "lowerBound:1970-01-10T23:59:59Z, " +
              "upperBound:1970-01-11T00:00:30Z]"
          }
        }
      }

      "time is changed, and ledger effective time is within the new acceptance window" should {

        "commands should be accepted" in {
          for {
            commandClient <- createCommandClient()
            getTimeResponse <- timeSource.runWith(Sink.head)
            oldTime = getTimeResponse.getCurrentTime
            newTime = toInstant(oldTime).plus(tenDays)
            _ <- TimeServiceGrpc
              .stub(channel)
              .setTime(SetTimeRequest(unwrappedLedgerId, Some(oldTime), Some(fromInstant(newTime))))
            completion <- commandClient.send(submissionWithTime(newTime))
          } yield {
            completion.status.value should have('code (Status.OK.getCode.value()))
          }
        }
      }

      "time is changed, and ledger effective time is outside of the new acceptance window (it's the previous time)" should {

        "reject commands with ABORTED" in {
          for {
            commandClient <- createCommandClient()
            getTimeResponse <- timeSource.runWith(Sink.head)
            oldTime = getTimeResponse.getCurrentTime
            newTime = toInstant(oldTime).plus(tenDays)
            _ <- TimeServiceGrpc
              .stub(channel)
              .setTime(SetTimeRequest(unwrappedLedgerId, Some(oldTime), Some(fromInstant(newTime))))
            completion <- commandClient.send(submissionWithTime(toInstant(oldTime)))
          } yield {
            completion.status.value should have('code (Status.ABORTED.getCode.value()))
            completion.status.value.message shouldEqual "TRANSACTION_OUT_OF_TIME_WINDOW: [" +
              "currentTime:1970-01-21T00:00:00Z, " +
              "ledgerEffectiveTime:1970-01-11T00:00:00Z, " +
              "lowerBound:1970-01-10T23:59:59Z, " +
              "upperBound:1970-01-11T00:00:30Z]"
          }
        }
      }

      "a choice on a contract is exercised before its LET" should {
        "result in a rejection" in {
          for {
            commandClient <- createCommandClient()
            getTimeResponse <- timeSource.runWith(Sink.head)
            currentTime = getTimeResponse.getCurrentTime
            templateId = templateIds.dummy
            txEndOffset <- TransactionServiceGrpc
              .stub(channel)
              .getLedgerEnd(GetLedgerEndRequest(unwrappedLedgerId))
              .map(_.getOffset)
            comp <- commandClient.send(dummyCreateRequest(toInstant(currentTime), templateId))
            _ = comp.getStatus should have('code (0))
            transactions <- ClientAdapter
              .serverStreaming(
                GetTransactionsRequest(
                  unwrappedLedgerId,
                  Some(txEndOffset),
                  None,
                  Some(TransactionFilter(
                    Map(submitRequest.commands.value.party -> Filters.defaultInstance)))),
                TransactionServiceGrpc
                  .stub(channel)
                  .getTransactions
              )
              .map(_.transactions)
              .take(1)
              .takeWithin(3.seconds)
              .runWith(Sink.head)
            contractId = extractContractId(transactions.head)
            timeBefore = toInstant(currentTime).minusSeconds(1)
            completion <- commandClient
              .withTimeProvider(None)
              .trackSingleCommand(dummyExerciseRequest(timeBefore, templateId, contractId))

          } yield {
            // INVALID_ARGUMENT, as this should return the same code as exercising a non-existing contract.
            completion.status.value should have('code (Status.INVALID_ARGUMENT.getCode.value()))
          }
        }
      }
    }
  }

}
