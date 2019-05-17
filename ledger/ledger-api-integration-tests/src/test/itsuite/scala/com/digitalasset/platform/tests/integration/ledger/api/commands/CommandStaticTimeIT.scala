// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import java.time
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.scaladsl.Sink
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.{Create, Exercise}
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.testing.time_service.{GetTimeRequest, SetTimeRequest}
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.Value.Sum.Party
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.ledger.client.services.commands.CommandClient
import com.digitalasset.platform.apitesting.{LedgerContext, TestTemplateIds}
import io.grpc.Status
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scala.concurrent.duration._

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Option2Iterable"
  ))
class CommandStaticTimeIT
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with ScalaFutures
    with SuiteResourceManagementAroundAll
    with MultiLedgerCommandUtils
    with TestTemplateIds
    with OptionValues {

  override def timeLimit: Span = 15.seconds

  private val tenDays: time.Duration = java.time.Duration.ofDays(10L)

  private val ttlSeconds = 20L

  private val newCommandId: () => String = {
    val atomicInteger = new AtomicInteger()
    () =>
      atomicInteger.incrementAndGet().toString
  }

  implicit class CommandClientOps(commandClient: CommandClient) {
    def send(request: SubmitRequest) =
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
        _.commands.ledgerId := config.assertStaticLedgerId,
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

  private def dummyExerciseRequest(time: Instant, templateId: Identifier, contractId: String) =
    submissionWithTime(time)
      .update(_.commands.commands := List(
        Command(Exercise(ExerciseCommand(Some(templateId), contractId, "DummyChoice1", Some(Value(Sum.Record(Record()))))))
      ))

  private def extractContractId(transaction: Transaction): String = {
    val events = for {
      event <- transaction.events
    } yield event.event

    events.collect {
      case Created(CreatedEvent(contractId, _, _, _, _, _)) => contractId
    }.head
  }

  private def timeSource(ctx: LedgerContext) =
    ClientAdapter.serverStreaming(GetTimeRequest(config.assertStaticLedgerId), ctx.timeService.getTime)

  "Command and Time Services" when {

    "ledger effective time is within acceptance window" should {

      "commands should be accepted" in allFixtures { ctx =>
        for {
          commandClient <- ctx.commandClient()
          completion <- commandClient
            .withTimeProvider(None)
            .trackSingleCommand(SubmitRequest(
              Some(submitRequest.getCommands.withLedgerId(config.assertStaticLedgerId).withCommandId(newCommandId()))))
        } yield {
          completion.status.value should have('code (Status.OK.getCode.value()))
        }
      }

      "ledger effective time is outside of acceptance window (in the future)" should {

        "reject commands with ABORTED" in allFixtures { ctx =>
          for {
            commandClient <- ctx.commandClient()
            getTimeResponse <- timeSource(ctx).runWith(Sink.head)
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

        "commands should be accepted" in allFixtures { ctx =>
          for {
            commandClient <- ctx.commandClient()
            getTimeResponse <- timeSource(ctx).runWith(Sink.head)
            oldTime = getTimeResponse.getCurrentTime
            newTime = toInstant(oldTime).plus(tenDays)
            _ <- ctx.timeService.setTime(SetTimeRequest(config.assertStaticLedgerId, Some(oldTime), Some(fromInstant(newTime))))
            completion <- commandClient.send(submissionWithTime(newTime))
          } yield {
            completion.status.value should have('code (Status.OK.getCode.value()))
          }
        }
      }


      "time is changed, and ledger effective time is outside of the new acceptance window (it's the previous time)" should {

        "reject commands with ABORTED" in allFixtures { ctx =>
          for {
            commandClient <- ctx.commandClient()
            getTimeResponse <- timeSource(ctx).runWith(Sink.head)
            oldTime = getTimeResponse.getCurrentTime
            newTime = toInstant(oldTime).plus(tenDays)
            _ <- ctx.timeService.setTime(SetTimeRequest(config.assertStaticLedgerId, Some(oldTime), Some(fromInstant(newTime))))
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
        "result in a rejection" in allFixtures { ctx =>
          for {
            commandClient <- ctx.commandClient()
            getTimeResponse <- timeSource(ctx).runWith(Sink.head)
            currentTime = getTimeResponse.getCurrentTime
            templateId = templateIds.dummy
            txEndOffset <- ctx.transactionClient.getLedgerEnd.map(_.getOffset)
            comp <- commandClient.send(dummyCreateRequest(toInstant(currentTime), templateId))
            _ = comp.getStatus should have('code(0))
            transaction <- ctx.transactionClient.getTransactions(txEndOffset, None, TransactionFilter(Map(submitRequest.commands.value.party -> Filters.defaultInstance)))
              .take(1)
              .takeWithin(3.seconds)
              .runWith(Sink.head)
            contractId = extractContractId(transaction)
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
