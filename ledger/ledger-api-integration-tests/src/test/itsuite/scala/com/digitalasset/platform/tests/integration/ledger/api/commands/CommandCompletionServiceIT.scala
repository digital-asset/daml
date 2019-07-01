// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.MockMessages.{applicationId, party}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_completion_service.{
  Checkpoint,
  CompletionStreamRequest
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.Boundary
import com.digitalasset.ledger.api.v1.transaction_service.GetLedgerEndRequest
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement.{
  CheckpointElement,
  CompletionElement
}
import com.digitalasset.ledger.client.services.commands.{
  CommandCompletionSource,
  CompletionStreamElement
}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.services.time.TimeProviderType.WallClock
import com.digitalasset.util.Ctx
import org.scalatest.{AsyncWordSpec, Matchers}
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CommandCompletionServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll {

  private def completionSource(
      completionService: CommandCompletionService,
      ledgerId: domain.LedgerId,
      applicationId: String,
      parties: Seq[String],
      offset: LedgerOffset): Source[CompletionStreamElement, NotUsed] =
    CommandCompletionSource(
      CompletionStreamRequest(ledgerId.unwrap, applicationId, parties, Some(offset)),
      completionService.completionStream)

  "Commands Completion Service" when {
    "listening for completions" should {

      "handle multi party subscriptions" in allFixtures { ctx =>
        val configuredParties = config.parties.list.toList
        for {
          commandClient <- ctx.commandClient(ctx.ledgerId)
          tracker <- commandClient.trackCommands[String](configuredParties)
          commands = configuredParties.map(p => Ctx(p, ctx.command(p, Nil)))
          result <- Source(commands).via(tracker).runWith(Sink.seq)
        } yield {
          val expected = configuredParties.map(p => (p, 0))
          result.map(ctx => (ctx.value.commandId, ctx.value.getStatus.code)) should contain theSameElementsAs expected
        }
      }

      "emit periodic Checkpoints with RecordTimes" in allFixtures { ctx =>
        val completionService =
          completionSource(
            ctx.commandCompletionService,
            ctx.ledgerId,
            applicationId,
            Seq(party),
            LedgerOffset(Boundary(LEDGER_BEGIN)))

        val recordTimes = completionService.collect {
          case CheckpointElement(Checkpoint(Some(recordTime), _)) => recordTime
        }

        recordTimes
          .batch(Long.MaxValue, ts => List(ts))((list, ts) => ts :: list)
          .throttle(1, 5.seconds)
          .drop(2) // initially we might miss a few for some reason
          .map(_.size)
          .take(2)
          .runWith(Sink.seq)
          .map { noOfHeartBeats =>
            all(noOfHeartBeats) should be >= 2 // should be fine for 1Hz
          }
      }

      "emit checkpoints with exclusive offsets (results for coming back with the same offset will not contain the records)" in allFixtures {
        ctx =>
          val configuredParties = config.parties.list.toList

          def completionsFrom(offset: LedgerOffset) =
            completionSource(
              ctx.commandCompletionService,
              ctx.ledgerId,
              applicationId,
              configuredParties,
              offset)
              .sliding(2, 1)
              .map(_.toList)
              .collect {
                case CompletionElement(completion) :: CheckpointElement(checkpoint) :: Nil =>
                  checkpoint.getOffset -> completion.commandId
              }

          for {
            startingOffsetResponse <- ctx.transactionService.getLedgerEnd(
              GetLedgerEndRequest(ctx.ledgerId.unwrap))
            startingOffset = startingOffsetResponse.getOffset
            commandClient <- ctx.commandClient(ctx.ledgerId)
            commands = configuredParties
              .map(p => ctx.command(p, Nil))
              .zipWithIndex
              .map { case (req, i) => req.update(_.commands.commandId := s"command-id-$i") }
            _ <- Future.sequence(
              commands
                .map(commandClient.submitSingleCommand))

            completions1 <- completionsFrom(startingOffset)
              .take(commands.size.toLong)
              .runWith(Sink.seq)

            completions2 <- completionsFrom(completions1(0)._1)
              .take(commands.size.toLong - 1)
              .runWith(Sink.seq)

            completions3 <- completionsFrom(completions1(1)._1)
              .take(commands.size.toLong - 2)
              .runWith(Sink.seq)
          } yield {
            val commandIds1 = completions1.map(_._2)
            val commandIds2 = completions2.map(_._2)
            val commandIds3 = completions3.map(_._2)

            commands.map(_.getCommands.commandId).foreach { commandId =>
              commandIds1 should contain(commandId)
            }

            commandIds2 should not contain (commandIds1(0))

            commandIds3 should not contain (commandIds1(0))
            commandIds3 should not contain (commandIds1(1))
          }
      }
    }
  }

  override protected def config: Config =
    Config.default.withHeartBeatInterval(1.second).withTimeProvider(WallClock) //10Hz

}
