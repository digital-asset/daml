// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.MockMessages.applicationId
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_completion_service.{
  Checkpoint,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.digitalasset.ledger.api.v1.commands.{
  Command => ProtoCommand,
  CreateCommand => ProtoCreateCommand
}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.Boundary
import com.digitalasset.ledger.api.v1.transaction_service.GetLedgerEndRequest
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement.{
  CheckpointElement,
  CompletionElement
}
import com.digitalasset.ledger.client.services.commands.{
  CommandClient,
  CommandCompletionSource,
  CompletionStreamElement
}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture, TestTemplateIds}
import com.digitalasset.platform.services.time.TimeProviderType.WallClock
import com.digitalasset.util.Ctx
import org.scalatest.{AsyncWordSpec, Matchers}
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.concurrent.duration._
import com.digitalasset.platform.apitesting.TestParties._
import com.digitalasset.platform.testing.SingleItemObserver
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CommandCompletionServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll {

  private[this] val templateIds = new TestTemplateIds(config).templateIds

  private def completionSource(
      completionService: CommandCompletionService,
      ledgerId: domain.LedgerId,
      applicationId: String,
      parties: Seq[String],
      offset: Option[LedgerOffset]): Source[CompletionStreamElement, NotUsed] =
    CommandCompletionSource(
      CompletionStreamRequest(ledgerId.unwrap, applicationId, parties, offset),
      completionService.completionStream)

  "Commands Completion Service" when {
    "listening for completions" should {

      "handle multi party subscriptions" in allFixtures { ctx =>
        val configuredParties = config.parties
        for {
          commandClient <- ctx.commandClient(ctx.ledgerId)
          tracker <- commandClient.trackCommands[String](configuredParties)
          commands = configuredParties.map(p => Ctx(p, ctx.command(p, p, Nil)))
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
            Seq(Alice),
            Some(LedgerOffset(Boundary(LEDGER_BEGIN))))

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
          val configuredParties = config.parties

          def completionsFrom(offset: LedgerOffset) =
            completionSource(
              ctx.commandCompletionService,
              ctx.ledgerId,
              applicationId,
              configuredParties,
              Some(offset))
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
              .map(p => ctx.command(p, p, Nil))
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

      "implicitly tail the stream if no offset is passed" in allFixtures { ctx =>
        // A command to create a Test.Dummy contract (see //ledger/sandbox/src/main/resources/damls/Test.daml)
        val createDummyCommand = ProtoCommand(
          ProtoCommand.Command.Create(
            ProtoCreateCommand(
              templateId = Some(templateIds.dummy),
              createArguments = Some(
                Record(
                  fields = Seq(
                    RecordField("operator", Some(Value(Value.Sum.Party(Alice))))
                  )
                ))
            )))

        def trackDummyCreation(client: CommandClient, context: LedgerContext, id: String) =
          client.trackSingleCommand(context.command(id, Alice, Seq(createDummyCommand)))

        def trackDummyCreations(client: CommandClient, context: LedgerContext, ids: List[String]) =
          Future.sequence(ids.map(trackDummyCreation(client, context, _)))

        // Don't use the `completionSource` to ensure that the RPC lands on the server before anything else happens
        def tailCompletions(
            context: LedgerContext
        ): Future[Completion] = {
          SingleItemObserver
            .find[CompletionStreamResponse] {
              case CompletionStreamResponse(_, Nil) => false
              case _ => true
            }(
              context.commandCompletionService.completionStream(
                CompletionStreamRequest(
                  context.ledgerId.unwrap,
                  applicationId,
                  Seq(Alice),
                  offset = None),
                _))
            .map(_.get)
            .collect {
              case CompletionStreamResponse(_, completion +: _) => completion
            }
        }

        val arbitraryCommandIds = List.tabulate(10)(_.toString)
        val expectedCommandId = "the-one"

        for {
          client <- ctx.commandClient(ctx.ledgerId)
          _ <- trackDummyCreations(client, ctx, arbitraryCommandIds)
          futureCompletion = tailCompletions(ctx) // this action and the following have to run concurrently
          _ = trackDummyCreation(client, ctx, expectedCommandId) // concurrent to previous action
          completion <- futureCompletion
        } yield {
          completion.commandId shouldBe expectedCommandId
        }

      }
    }
  }

  override protected def config: Config =
    Config.default.withHeartBeatInterval(1.second).withTimeProvider(WallClock) //10Hz

}
