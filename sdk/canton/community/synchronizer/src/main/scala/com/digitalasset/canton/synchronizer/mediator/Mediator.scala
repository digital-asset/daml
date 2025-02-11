// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.{EitherT, NonEmptySeq}
import cats.implicits.toFoldableOps
import cats.instances.future.*
import cats.syntax.bifunctor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.protocol.messages.{
  ProtocolMessage,
  RootHashMessage,
  SerializedRootHashMessagePayload,
}
import com.digitalasset.canton.protocol.{DynamicSynchronizerParametersWithValidity, RequestId}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.RichSequencerClient
import com.digitalasset.canton.sequencing.handlers.DiscardIgnoredEvents
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, OpenEnvelope, SequencedEvent}
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.{SequencedEventStore, SequencerCounterTrackerStore}
import com.digitalasset.canton.synchronizer.mediator.Mediator.PruningError
import com.digitalasset.canton.synchronizer.mediator.store.MediatorState
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.{
  MediatorId,
  SynchronizerId,
  SynchronizerOutboxHandle,
  TopologyManagerStatus,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, FutureUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext

/** Responsible for events processing.
  * Reads mediator confirmation requests and confirmation responses from a sequencer and produces ConfirmationResultMessages.
  * For scaling / high-availability, several instances need to be created.
  */
private[mediator] class Mediator(
    val synchronizerId: SynchronizerId,
    val mediatorId: MediatorId,
    @VisibleForTesting
    val sequencerClient: RichSequencerClient,
    val topologyClient: SynchronizerTopologyClientWithInit,
    private[canton] val syncCrypto: SynchronizerCryptoClient,
    topologyTransactionProcessor: TopologyTransactionProcessor,
    val topologyManagerStatus: TopologyManagerStatus,
    val synchronizerOutboxHandle: SynchronizerOutboxHandle,
    val timeTracker: SynchronizerTimeTracker,
    state: MediatorState,
    private[canton] val sequencerCounterTrackerStore: SequencerCounterTrackerStore,
    sequencedEventStore: SequencedEventStore,
    parameters: CantonNodeParameters,
    protocolVersion: ProtocolVersion,
    clock: Clock,
    metrics: MediatorMetrics,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with FlagCloseableAsync
    with HasCloseContext {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  private val delayLogger =
    new DelayLogger(
      clock,
      logger,
      parameters.delayLoggingThreshold,
      metrics.sequencerClient.handler.delay,
    )

  private val verdictSender =
    VerdictSender(sequencerClient, syncCrypto, mediatorId, protocolVersion, loggerFactory)

  private val processor = new ConfirmationRequestAndResponseProcessor(
    synchronizerId,
    mediatorId,
    verdictSender,
    syncCrypto,
    timeTracker,
    state,
    protocolVersion,
    loggerFactory,
    timeouts,
  )

  private val deduplicator = MediatorEventDeduplicator.create(
    state.deduplicationStore,
    state.finalizedResponseStore,
    verdictSender,
    syncCrypto.ips,
    protocolVersion,
    loggerFactory,
  )

  private val eventsProcessor = new MediatorEventsProcessor(
    topologyTransactionProcessor.createHandler(synchronizerId),
    processor,
    deduplicator,
    loggerFactory,
  )

  val stateInspection: MediatorStateInspection = new MediatorStateInspection(state)

  /** Starts the mediator. NOTE: Must only be called at most once on a mediator instance. */
  private[mediator] def start()(implicit
      initializationTraceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = performUnlessClosingUSF("start") {
    for {

      preheadO <- sequencerCounterTrackerStore.preheadSequencerCounter
      nextTs = preheadO.fold(CantonTimestamp.MinValue)(_.timestamp.immediateSuccessor)
      _ <- state.deduplicationStore.initialize(nextTs)

      _ <-
        sequencerClient.subscribeTracking(
          sequencerCounterTrackerStore,
          DiscardIgnoredEvents(loggerFactory)(handler),
          timeTracker,
          onCleanHandler = onCleanSequencerCounterHandler,
        )

    } yield ()
  }

  private def onCleanSequencerCounterHandler(
      newTracedPrehead: Traced[SequencerCounterCursorPrehead]
  ): Unit = newTracedPrehead.withTraceContext { implicit traceContext => newPrehead =>
    FutureUtil.doNotAwait(
      performUnlessClosingUSF("prune mediator deduplication store")(
        state.deduplicationStore.prune(newPrehead.timestamp)
      ).onShutdown(logger.info("Not pruning the mediator deduplication store due to shutdown")),
      "pruning the mediator deduplication store failed",
    )
  }

  /** Prune all unnecessary data from the mediator state and sequenced events store.
    * Will validate the provided timestamp is before the prehead position of the sequenced events store,
    * meaning that all events up until this point have completed processing and can be safely removed.
    */
  def prune(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, PruningError, Unit] =
    for {
      preHeadCounterO <- EitherT
        .right(sequencerCounterTrackerStore.preheadSequencerCounter)
      preHeadTsO = preHeadCounterO.map(_.timestamp)
      cleanTimestamp <- EitherT
        .fromOption(preHeadTsO, PruningError.NoDataAvailableForPruning)
        .leftWiden[PruningError]
        .mapK(FutureUnlessShutdown.outcomeK)

      _ <- EitherT
        .cond[FutureUnlessShutdown](
          timestamp <= cleanTimestamp,
          (),
          PruningError.CannotPruneAtTimestamp(timestamp, cleanTimestamp),
        )

      synchronizerParametersChanges <- EitherT
        .right(
          topologyClient
            .awaitSnapshot(timestamp)
            .flatMap(snapshot => snapshot.listDynamicSynchronizerParametersChanges())
        )

      _ <- NonEmptySeq.fromSeq(synchronizerParametersChanges) match {
        case Some(synchronizerParametersChangesNes) =>
          prune(
            pruneAt = timestamp,
            cleanTimestamp = cleanTimestamp,
            synchronizerParametersChanges = synchronizerParametersChangesNes,
          )

        case None =>
          logger.info(
            s"No synchronizer parameters found for pruning at $timestamp. This is likely due to $timestamp being before synchronizer bootstrapping. Will not prune."
          )
          EitherT.pure[FutureUnlessShutdown, PruningError](())
      }

    } yield ()

  private def prune(
      pruneAt: CantonTimestamp,
      cleanTimestamp: CantonTimestamp,
      synchronizerParametersChanges: NonEmptySeq[DynamicSynchronizerParametersWithValidity],
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, PruningError, Unit] = {
    val latestSafePruningTsO = Mediator.latestSafePruningTsBefore(
      synchronizerParametersChanges,
      cleanTimestamp,
    )

    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown] {
        latestSafePruningTsO
          .toRight(PruningError.MissingSynchronizerParametersForValidPruningTsComputation(pruneAt))
          .flatMap { latestSafePruningTs =>
            Either.cond[PruningError, Unit](
              pruneAt <= latestSafePruningTs,
              (),
              PruningError.CannotPruneAtTimestamp(pruneAt, latestSafePruningTs),
            )
          }
      }

      _ = logger.debug(show"Pruning finalized responses up to [$pruneAt]")
      _ <- EitherT.right(state.prune(pruneAt))
      _ = logger.debug(show"Pruning sequenced event up to [$pruneAt]")
      _ <- EitherT.right(sequencedEventStore.prune(pruneAt))

      // After pruning successfully, update the "max-event-age" metric
      // looking up the oldest event (in case prunedAt precedes any events and nothing was pruned).
      oldestEventTimestampO <- EitherT.right(
        stateInspection.locatePruningTimestamp(NonNegativeInt.zero)
      )
      _ = MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, oldestEventTimestampO)

    } yield ()
  }

  private def handler: ApplicationHandler[OrdinaryEnvelopeBox, ClosedEnvelope] =
    new ApplicationHandler[OrdinaryEnvelopeBox, ClosedEnvelope] {

      override def name: String = s"mediator-$mediatorId"

      override def subscriptionStartsAt(
          start: SubscriptionStart,
          synchronizerTimeTracker: SynchronizerTimeTracker,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        topologyTransactionProcessor.subscriptionStartsAt(start, synchronizerTimeTracker)

      private def sendMalformedRejection(
          rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
          timestamp: CantonTimestamp,
          verdict: MediatorVerdict.MediatorReject,
      )(implicit tc: TraceContext): FutureUnlessShutdown[Unit] = {
        val requestId = RequestId(timestamp)

        for {
          snapshot <- syncCrypto.awaitSnapshot(timestamp)
          synchronizerParameters <- snapshot.ipsSnapshot
            .findDynamicSynchronizerParameters()
            .flatMap(_.toFutureUS(new RuntimeException(_)))

          decisionTime <- synchronizerParameters.decisionTimeForF(timestamp)
          _ <- verdictSender.sendReject(
            requestId,
            None,
            rootHashMessages,
            verdict.toVerdict(protocolVersion),
            decisionTime,
          )
        } yield ()
      }

      override def apply(
          tracedEvents: Traced[Seq[BoxedEnvelope[OrdinarySequencedEvent, ClosedEnvelope]]]
      ): HandlerResult =
        tracedEvents.withTraceContext { implicit traceContext => events =>
          // update the delay logger using the latest event we've been handed
          events.lastOption.foreach(e => delayLogger.checkForDelay(e))

          val tracedOpenEventsWithRejectionsF = events.map { closedSignedEvent =>
            val closedEvent = closedSignedEvent.signedEvent.content

            val (openEvent, openingErrors) = SequencedEvent.openEnvelopes(closedEvent)(
              protocolVersion,
              syncCrypto.crypto.pureCrypto,
            )

            val rejectionsF = openingErrors.parTraverse_ { error =>
              val cause =
                s"Received an envelope at ${closedEvent.timestamp} that cannot be opened. Discarding envelope... Reason: $error"
              val alarm = MediatorError.MalformedMessage.Reject(cause)
              alarm.report()

              val rootHashMessages = openEvent.envelopes.mapFilter(
                ProtocolMessage.select[RootHashMessage[SerializedRootHashMessagePayload]]
              )

              if (rootHashMessages.nonEmpty) {
                // In this case, we assume it is a Mediator Confirmation Request message
                sendMalformedRejection(
                  rootHashMessages,
                  closedEvent.timestamp,
                  MediatorVerdict.MediatorReject(alarm),
                )
              } else FutureUnlessShutdown.unit
            }

            (
              Traced(openEvent)(closedSignedEvent.traceContext),
              rejectionsF,
            )
          }

          val (tracedOpenEvents, rejectionsF) = tracedOpenEventsWithRejectionsF.unzip
          logger.debug(s"Processing ${tracedOpenEvents.size} events for the mediator")

          val result = FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
            eventsProcessor.handle(tracedOpenEvents),
            "Failed to handle Mediator events",
            closeContext = Some(closeContext),
          )

          rejectionsF.sequence_.flatMap { case () => result }
        }
    }

  override def closeAsync() =
    Seq(
      SyncCloseable(
        "mediator",
        LifeCycle.close(
          topologyTransactionProcessor,
          syncCrypto.ips,
          timeTracker,
          processor,
          sequencerClient,
          topologyClient,
          sequencerCounterTrackerStore,
          state,
        )(logger),
      )
    )
}

private[mediator] object Mediator {
  sealed trait PruningError {
    def message: String
  }
  object PruningError {

    /** The mediator has not yet processed enough data for any to be available for pruning */
    case object NoDataAvailableForPruning extends PruningError {
      lazy val message: String = "There is no mediator data available for pruning"
    }

    /** Dynamic synchronizer parameters available for ts were not found */
    final case class MissingSynchronizerParametersForValidPruningTsComputation(ts: CantonTimestamp)
        extends PruningError {
      override def message: String =
        show"Dynamic synchronizer parameters to compute earliest available pruning timestamp not found for ts [$ts]"
    }

    /** The mediator can prune some data but data for the requested timestamp cannot yet be removed */
    final case class CannotPruneAtTimestamp(
        requestedTimestamp: CantonTimestamp,
        earliestPruningTimestamp: CantonTimestamp,
    ) extends PruningError {
      override def message: String =
        show"Requested pruning timestamp [$requestedTimestamp] is later than the earliest available pruning timestamp [$earliestPruningTimestamp]"
    }
  }

  sealed trait PruningSafetyCheck extends Product with Serializable
  case object Safe extends PruningSafetyCheck
  final case class SafeUntil(ts: CantonTimestamp) extends PruningSafetyCheck

  private[mediator] def checkPruningStatus(
      synchronizerParameters: DynamicSynchronizerParametersWithValidity,
      cleanTs: CantonTimestamp,
  ): PruningSafetyCheck = {
    lazy val timeout = synchronizerParameters.parameters.confirmationResponseTimeout
    lazy val cappedSafePruningTs = synchronizerParameters.validFrom.max(cleanTs - timeout)

    if (cleanTs <= synchronizerParameters.validFrom) // If these parameters apply only to the future
      Safe
    else {
      synchronizerParameters.validUntil match {
        case None => SafeUntil(cappedSafePruningTs)
        case Some(validUntil) if cleanTs <= validUntil => SafeUntil(cappedSafePruningTs)
        case Some(validUntil) =>
          if (validUntil + timeout <= cleanTs) Safe else SafeUntil(cappedSafePruningTs)
      }
    }
  }

  /** Returns the latest safe pruning ts which is <= cleanTs */
  private[mediator] def latestSafePruningTsBefore(
      allSynchronizerParametersChanges: NonEmptySeq[DynamicSynchronizerParametersWithValidity],
      cleanTs: CantonTimestamp,
  ): Option[CantonTimestamp] = allSynchronizerParametersChanges
    .map(checkPruningStatus(_, cleanTs))
    .collect { case SafeUntil(ts) => ts }
    .minOption
}
