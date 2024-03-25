// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.foldable.*
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerError,
  ApplicationHandlerException,
  ApplicationHandlerPassive,
}
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.SingletonTraverse
import com.digitalasset.canton.util.SingletonTraverse.syntax.*
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.scaladsl.Flow

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** Converts an [[com.digitalasset.canton.sequencing.ApplicationHandler]] into a Pekko flow. */
class ApplicationHandlerPekko[F[+_], Context](
    handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
    metrics: SequencerClientMetrics,
    override protected val loggerFactory: NamedLoggerFactory,
    killSwitchOfContext: Context => KillSwitch,
)(implicit executionContext: ExecutionContext, Context: SingletonTraverse.Aux[F, Context])
    extends NamedLogging {
  import ApplicationHandlerPekko.*

  /** Calls the `handler` sequentially for each sequenced event,
    * and stops if synchronous processing throws an exception.
    * `Control` elements are passed through.
    *
    * @param asyncParallelism How many asynchronous processing steps are run concurrently.
    */
  def asFlow(
      asyncParallelism: PositiveInt
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Flow[
    F[BoxedEnvelope[PossiblyIgnoredEnvelopeBox, ClosedEnvelope]],
    F[UnlessShutdown[Either[ApplicationHandlerError, Unit]]],
    NotUsed,
  ] = {
    Flow[F[BoxedEnvelope[PossiblyIgnoredEnvelopeBox, ClosedEnvelope]]].contextualize
      .statefulMapAsyncContextualizedUS(KeepGoing: State)(processSynchronously)
      // do not use mapAsyncUS because the asynchronous futures have already been spawned
      // by the synchronous processing
      // and we want to synchronize on all of them upon a shutdown
      // The declared parallelism nevertheless limits the number of asynchronous processing running in parallel
      // via backpressure.
      .mapAsync(asyncParallelism.value) { result =>
        result.traverseSingleton { (context, errorOrSyncResult) =>
          val asyncResult = errorOrSyncResult match {
            case Outcome(errorOrSyncResult) =>
              errorOrSyncResult match {
                case Right(Some(syncResult)) =>
                  processAsyncResult(syncResult, killSwitchOfContext(context))
                case Right(None) => FutureUnlessShutdown.pure(Right(()))
                case Left(error) => FutureUnlessShutdown.pure(Left(error))
              }
            case AbortedDueToShutdown => FutureUnlessShutdown.abortedDueToShutdown
          }
          asyncResult.unwrap
        }
      }
  }

  private[this] def processSynchronously(
      state: State,
      context: Context,
      tracedEventBatch: BoxedEnvelope[PossiblyIgnoredEnvelopeBox, ClosedEnvelope],
  )(implicit closeContext: CloseContext): FutureUnlessShutdown[
    (
        State,
        Either[ApplicationHandlerError, Option[EventBatchSynchronousResult]],
    )
  ] = {

    state match {
      case KeepGoing =>
        tracedEventBatch.traverse(NonEmpty.from) match {
          case Some(eventBatchNE) =>
            handleNextBatch(eventBatchNE, killSwitchOfContext(context))
          case None =>
            FutureUnlessShutdown.pure(KeepGoing -> Right(None))
        }
      case Halt =>
        FutureUnlessShutdown.pure(Halt -> Right(None))
    }
  }

  private def handleNextBatch(
      tracedBatch: Traced[NonEmpty[Seq[PossiblyIgnoredSequencedEvent[ClosedEnvelope]]]],
      killSwitch: KillSwitch,
  )(implicit
      closeContext: CloseContext
  ): FutureUnlessShutdown[
    (State, Either[ApplicationHandlerError, Option[EventBatchSynchronousResult]])
  ] =
    tracedBatch.withTraceContext { implicit batchTraceContext => batch =>
      val lastSc = batch.last1.counter
      val firstEvent = batch.head1
      val firstSc = firstEvent.counter

      metrics.handler.numEvents.inc(batch.size.toLong)(MetricsContext.Empty)
      logger.debug(s"Passing ${batch.size} events to the application handler ${handler.name}.")
      // Measure only the synchronous part of the application handler so that we see how much the application handler
      // contributes to the sequential processing bottleneck.
      val syncResultFF = FutureUnlessShutdown.fromTry(
        Try(
          Timed.future(metrics.handler.applicationHandle, handler(Traced(batch)))
        )
      )

      syncResultFF.flatten.transformIntoSuccess {
        case Success(asyncResultOutcome) =>
          asyncResultOutcome.map(result =>
            KeepGoing -> Right(Some(EventBatchSynchronousResult(firstSc, lastSc, result)))
          )

        case Failure(error) =>
          killSwitch.shutdown()
          handleError(error, firstSc, lastSc, syncProcessing = true)
            .map(failure => Halt -> Left(failure))
      }
    }

  private def processAsyncResult(
      syncResult: EventBatchSynchronousResult,
      killSwitch: KillSwitch,
  )(implicit
      closeContext: CloseContext
  ): FutureUnlessShutdown[Either[ApplicationHandlerError, Unit]] = {
    val EventBatchSynchronousResult(firstSc, lastSc, asyncResult) = syncResult
    implicit val batchTraceContext: TraceContext = syncResult.traceContext
    asyncResult.unwrap.transformIntoSuccess {
      case Success(outcome) =>
        outcome.map(Right.apply)
      case Failure(error) =>
        killSwitch.shutdown()
        handleError(error, firstSc, lastSc, syncProcessing = false).map(failure => Left(failure))
    }
  }

  private def handleError(
      error: Throwable,
      firstSc: SequencerCounter,
      lastSc: SequencerCounter,
      syncProcessing: Boolean,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): UnlessShutdown[ApplicationHandlerError] = {
    val sync = if (syncProcessing) "Synchronous" else "Asynchronous"
    error match {
      case PassiveInstanceException(reason) =>
        logger.warn(s"$sync event processing stopped because instance became passive")
        Outcome(ApplicationHandlerPassive(reason))

      case _ if closeContext.context.isClosing =>
        logger.info(
          s"$sync event processing failed for event batch with sequencer counters $firstSc to $lastSc, most likely due to an ongoing shutdown",
          error,
        )
        AbortedDueToShutdown

      case _ =>
        logger.error(
          s"Synchronous event processing failed for event batch with sequencer counters $firstSc to $lastSc.",
          error,
        )
        Outcome(ApplicationHandlerException(error, firstSc, lastSc))
    }
  }
}

object ApplicationHandlerPekko {

  private[ApplicationHandlerPekko] sealed trait State extends Product with Serializable
  private[ApplicationHandlerPekko] case object Halt extends State
  private[ApplicationHandlerPekko] case object KeepGoing extends State

  private final case class EventBatchSynchronousResult(
      firstSc: SequencerCounter,
      lastSc: SequencerCounter,
      asyncResult: AsyncResult,
  )(implicit val traceContext: TraceContext)
}
