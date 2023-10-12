// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import akka.NotUsed
import akka.stream.scaladsl.{Concat, Flow, GraphDSL, Merge, Source}
import akka.stream.{DelayOverflowStrategy, FlowShape, OverflowStrategy}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.digitalasset.canton.ledger.client.services.commands.tracker.{
  CommandTracker,
  TrackedCommandKey,
}
import com.digitalasset.canton.util.Ctx
import com.google.protobuf.empty.Empty
import org.slf4j.LoggerFactory

import java.time.Duration
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

/** Tracks commands and emits results upon their completion or timeout.
  * The outbound data is minimal, users must put whatever else they need into the context objects.
  */
object CommandTrackerFlow {

  private val logger = LoggerFactory.getLogger(getClass)

  final case class Materialized[SubmissionMat, Context](
      submissionMat: SubmissionMat,
      trackingMat: Future[immutable.Map[TrackedCommandKey, Context]],
  )

  def apply[Context, SubmissionMat](
      commandSubmissionFlow: Flow[
        Ctx[(Context, TrackedCommandKey), CommandSubmission],
        Ctx[(Context, TrackedCommandKey), Try[
          Empty
        ]],
        SubmissionMat,
      ],
      createCommandCompletionSource: LedgerOffset => Source[CompletionStreamElement, NotUsed],
      startingOffset: LedgerOffset,
      maximumCommandTimeout: Duration,
      backOffDuration: FiniteDuration = 1.second,
      timeoutDetectionPeriod: FiniteDuration = 1.second,
  ): Flow[Ctx[Context, CommandSubmission], Ctx[
    Context,
    Either[CompletionFailure, CompletionSuccess],
  ], Materialized[
    SubmissionMat,
    Context,
  ]] = {

    val trackerExternal = new CommandTracker[Context](maximumCommandTimeout, timeoutDetectionPeriod)

    Flow.fromGraph(GraphDSL.createGraph(commandSubmissionFlow, trackerExternal)(Materialized.apply) {
      implicit builder => (submissionFlow, tracker) =>
        import GraphDSL.Implicits.*

        val wrapResult =
          builder.add(Flow[Ctx[(Context, TrackedCommandKey), Try[Empty]]].map(Left.apply))

        val wrapCompletion = builder.add(Flow[CompletionStreamElement].map(Right.apply))

        val merge = builder.add(
          Merge[Either[Ctx[(Context, TrackedCommandKey), Try[Empty]], CompletionStreamElement]](
            inputPorts = 2,
            eagerComplete = false,
          )
        )

        val startAt = builder.add(Source.single(startingOffset))
        val concat = builder.add(Concat[LedgerOffset](2))

        val completionFlow = builder.add(
          Flow[LedgerOffset]
            .buffer(1, OverflowStrategy.dropHead) // storing the last offset
            .expand(offset =>
              Iterator.iterate(offset)(identity)
            ) // so we always have an element to fetch
            .flatMapConcat(
              createCommandCompletionSource(_).recoverWithRetries(
                1,
                { case e =>
                  logger.warn(
                    s"Completion Stream failed with an error. Trying to recover in $backOffDuration..."
                  )
                  logger.debug(
                    s"Completion Stream failed with an error. Trying to recover in $backOffDuration...",
                    e,
                  )
                  delayedEmptySource(backOffDuration)
                },
              )
            )
        )

        // format: OFF
        (startAt.out       ~> concat).discard
        (tracker.offsetOut ~> concat).discard
        concat.out        ~> completionFlow.in

        tracker.submitRequestOut ~> submissionFlow ~> wrapResult ~> merge.in(0)
        tracker.commandResultIn  <~ merge.out
        merge.in(1) <~ wrapCompletion <~ completionFlow.out
        // format: ON

        FlowShape(tracker.submitRequestIn, tracker.resultOut)
    })
  }

  private def delayedEmptySource(
      delay: FiniteDuration
  ): Source[CompletionStreamElement, NotUsed] = {
    Source
      .single(1)
      .delay(delay, DelayOverflowStrategy.backpressure)
      .flatMapConcat(_ => Source.empty[CompletionStreamElement])
  }

}
