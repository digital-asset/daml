// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
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
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.Boundary
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement.CheckpointElement
import com.digitalasset.ledger.client.services.commands.{
  CommandCompletionSource,
  CompletionStreamElement
}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.services.time.TimeProviderType.WallClock
import com.digitalasset.util.Ctx
import com.google.rpc.status.Status
import org.scalatest.{AsyncWordSpec, Matchers}

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
      ledgerId: String,
      applicationId: String,
      parties: Seq[String],
      offset: LedgerOffset): Source[CompletionStreamElement, NotUsed] =
    CommandCompletionSource(
      CompletionStreamRequest(ledgerId, applicationId, parties, Some(offset)),
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
          val expected = configuredParties.map(p => Ctx(p, Completion(p, Some(Status(0)))))
          result should contain theSameElementsAs expected
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
            noOfHeartBeats.foreach(_ should be >= 2) // should be fine for 1Hz
            succeed
          }
      }
    }
  }

  override protected def config: Config =
    Config.default.withHeartBeatInterval(1.second).withTimeProvider(WallClock) //10Hz

}
