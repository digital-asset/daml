// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands

import java.time.{Duration => JDuration}

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Source}
import akka.stream.{DelayOverflowStrategy, FlowShape}
import com.digitalasset.ledger.api.v1.command_submission_service._
import com.digitalasset.ledger.api.v1.completion._
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.client.services.commands.tracker.CommandTracker
import com.digitalasset.util.Ctx
import com.google.protobuf.empty.Empty
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Try

/**
  * Tracks commands and emits results upon their completion or timeout.
  * The outbound data is minimal, users must put whatever else they need into the context objects.
  */
object CommandTrackerFlow {

  private val logger = LoggerFactory.getLogger(getClass)

  final case class Materialized[SubmissionMat, Context](
      submissionMat: SubmissionMat,
      trackingMat: Future[immutable.Map[String, Context]])

  def apply[Context, SubmissionMat](
      commandSubmissionFlow: Flow[
        Ctx[(Context, String), SubmitRequest],
        Ctx[(Context, String), Try[Empty]],
        SubmissionMat],
      createCommandCompletionSource: LedgerOffset => Source[CompletionStreamElement, NotUsed],
      startingOffset: LedgerOffset,
      maxDeduplicationTime: () => JDuration,
      backOffDuration: FiniteDuration = 1.second): Flow[
    Ctx[Context, SubmitRequest],
    Ctx[Context, Completion],
    Materialized[SubmissionMat, Context]] = {

    val trackerExternal = new CommandTracker[Context](maxDeduplicationTime)

    Flow.fromGraph(GraphDSL.create(commandSubmissionFlow, trackerExternal)(Materialized.apply) {
      implicit builder => (submissionFlow, tracker) =>
        import GraphDSL.Implicits._

        val wrapResult = builder.add(Flow[Ctx[(Context, String), Try[Empty]]].map(Left.apply))

        val wrapCompletion = builder.add(Flow[CompletionStreamElement].map(Right.apply))

        val merge = builder.add(
          Merge[Either[Ctx[(Context, String), Try[Empty]], CompletionStreamElement]](2, false))

        val completionSource = builder.add(
          createCommandCompletionSource(startingOffset).recoverWithRetries(
            1, {
              case e =>
                logger.warn(
                  s"Completion Stream failed with an error. Trying to recover in ${backOffDuration} ..",
                  e)
                Source.empty
                delayedEmptySource(backOffDuration)
            }
          )
        )

        // format: OFF
        tracker.submitRequestOut ~> submissionFlow ~> wrapResult ~> merge.in(0)
        tracker.commandResultIn  <~ merge.out
        merge.in(1) <~ wrapCompletion <~ completionSource
        // format: ON

        FlowShape(tracker.submitRequestIn, tracker.resultOut)
    })
  }

  private def delayedEmptySource(
      delay: FiniteDuration): Source[CompletionStreamElement, NotUsed] = {
    Source
      .single(1)
      .delay(delay, DelayOverflowStrategy.backpressure)
      .flatMapConcat(_ => Source.empty[CompletionStreamElement])
  }

}
