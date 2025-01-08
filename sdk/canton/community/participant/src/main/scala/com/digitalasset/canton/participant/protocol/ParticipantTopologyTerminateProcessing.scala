// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessing.EventInfo
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStore.EffectiveStateChange
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerCounter, topology}

import scala.concurrent.ExecutionContext

object ParticipantTopologyTerminateProcessing {

  private[canton] val enabledWarningMessage =
    "Topology events are enabled. This is an experimental feature, unsafe for production use."

  /** Event with indication of whether the participant needs to initiate party replication */
  private final case class EventInfo(
      event: Update.TopologyTransactionEffective,
      requireLocalPartyReplication: Boolean,
  )
}

class ParticipantTopologyTerminateProcessing(
    synchronizerId: SynchronizerId,
    protocolVersion: ProtocolVersion,
    recordOrderPublisher: RecordOrderPublisher,
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    initialRecordTime: CantonTimestamp,
    participantId: ParticipantId,
    unsafeEnableOnlinePartyReplication: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
) extends topology.processing.TerminateProcessing
    with NamedLogging {

  import ParticipantTopologyTerminateProcessing.enabledWarningMessage

  noTracingLogger.warn(enabledWarningMessage)

  override def terminate(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    if (sequencedTime.value > initialRecordTime) {
      for {
        effectiveStateChanges <- store.findEffectiveStateChanges(
          fromEffectiveInclusive = effectiveTime.value,
          onlyAtEffective = true,
        )
        _ = if (effectiveStateChanges.sizeIs > 1)
          logger.error(
            s"Invalid: findEffectiveStateChanges with onlyAtEffective = true should return only one EffectiveStateChange for effective time $effectiveTime, only the first one is taken into consideration."
          )
        events = effectiveStateChanges.headOption.flatMap(getNewEvents)
        _ = events.foreach { case EventInfo(event, requireLocalPartyReplication) =>
          (for {
            _ <- recordOrderPublisher.scheduleFloatingEventPublication(
              timestamp = effectiveTime.value,
              eventFactory = _ => Some(event),
            )
            _ <-
              if (unsafeEnableOnlinePartyReplication && requireLocalPartyReplication)
                recordOrderPublisher.scheduleEventBuffering(effectiveTime.value)
              else
                Right(())
          } yield ()) match {
            case Right(()) =>
              logger.debug(
                s"Scheduled topology event publication with sequencer counter: $sc, sequenced time: $sequencedTime, effective time: $effectiveTime"
              )

            case Left(invalidTime) =>
              // invariant: sequencedTime <= effectiveTime
              // at this point we did not tick sequencedTime yet
              ErrorUtil.invalidState(
                s"Cannot schedule topology event as record time is already at $invalidTime (publication with sequencer counter: $sc, sequenced time: $sequencedTime, effective time: $effectiveTime)"
              )
          }
        }
      } yield ()
    } else {
      // invariant: initial record time < first processed record time, so we can safely omit ticking and publishing here
      // for crash recovery the scheduleMissingTopologyEventsAtInitialization method should be used
      logger.debug(
        s"Omit publishing and ticking during replay. Initial record time: $initialRecordTime, sequencer counter: $sc, sequenced time: $sequencedTime, effective time: $effectiveTime"
      )
      FutureUnlessShutdown.unit
    }

  def scheduleMissingTopologyEventsAtInitialization(
      topologyEventPublishedOnInitialRecordTime: Boolean,
      traceContextForSequencedEvent: CantonTimestamp => FutureUnlessShutdown[Option[TraceContext]],
      parallelism: Int,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] = {
    logger.info(
      s"Fetching effective state changes after initial record time $initialRecordTime at domain-startup for topology event recovery."
    )
    for {
      effectiveChanges <- store.findEffectiveStateChanges(
        fromEffectiveInclusive = initialRecordTime,
        onlyAtEffective = false,
      )
      eventsWithSequencedTimeWithoutTraceContext = effectiveChanges
        .filter(_.sequencedTime.value <= initialRecordTime)
        .flatMap(effectiveChange =>
          getNewEvents(effectiveChange)
            .map(eventInfo => eventInfo.event -> effectiveChange.sequencedTime.value)
        )
      _ = logger.info(
        s"Fetching trace-contexts for ${eventsWithSequencedTimeWithoutTraceContext.size} topology-updates for topology event recovery."
      )
      eventsWithSequencedTime <- MonadUtil.parTraverseWithLimit(parallelism)(
        eventsWithSequencedTimeWithoutTraceContext
      ) { case (event, sequencedTime) =>
        traceContextForSequencedEvent(sequencedTime).map {
          case Some(sourceEventTraceContext) =>
            event.copy()(traceContext = sourceEventTraceContext) -> sequencedTime

          case None =>
            logger.warn(
              s"Cannot find trace context for sequenced time: $sequencedTime, using initialization trace context to schedule this event."
            )
            event -> sequencedTime
        }
      }
    } yield {
      logger.info(s"Scheduling #${eventsWithSequencedTime.size} topology events at domain-startup.")
      eventsWithSequencedTime.foreach(
        scheduleEventAtRecovery(topologyEventPublishedOnInitialRecordTime)
      )
    }
  }

  private def scheduleEventAtRecovery(
      topologyEventPublishedOnInitialRecordTime: Boolean
  ): ((Update.TopologyTransactionEffective, CantonTimestamp)) => Unit = {
    case (event, sequencedTime) =>
      implicit val traceContext: TraceContext = event.traceContext
      val effectiveTime = event.effectiveTime
      recordOrderPublisher.scheduleFloatingEventPublication(
        timestamp = effectiveTime,
        eventFactory = _ => Some(event),
      ) match {
        case Right(()) =>
          logger.debug(
            s"Re-scheduled topology event publication after restart, with sequenced time: $sequencedTime, effective time: $effectiveTime"
          )

        case Left(`initialRecordTime`) =>
          if (topologyEventPublishedOnInitialRecordTime) {
            logger.debug(
              s"Omit re-scheduling topology event publication after restart: a topology event is already published at initial record time: $initialRecordTime. (sequenced time: $sequencedTime, effective time: $effectiveTime)"
            )
          } else {
            recordOrderPublisher.scheduleFloatingEventPublicationImmediately {
              actualEffectiveTime =>
                assert(actualEffectiveTime == initialRecordTime)
                Some(event)
            }.discard
            logger.debug(
              s"Re-scheduled topology event publication immediately after restart, as record time is already the effective time, and no topology event was published at this time. (sequenced time: $sequencedTime, effective time: $effectiveTime)"
            )
          }

        case Left(invalidTime) =>
          // invariant: sequencedTime <= effectiveTime
          // at this point we did not tick sequencedTime yet
          ErrorUtil.invalidState(
            s"Cannot schedule topology event as record time is already at $invalidTime (publication with sequenced time: $sequencedTime, effective time: $effectiveTime)"
          )
      }
  }

  private def getNewEvents(effectiveStateChange: EffectiveStateChange)(implicit
      traceContext: TraceContext
  ): Option[EventInfo] =
    TopologyTransactionDiff(
      synchronizerId = synchronizerId,
      oldRelevantState = effectiveStateChange.before.signedTransactions,
      currentRelevantState = effectiveStateChange.after.signedTransactions,
      participantId = participantId,
      protocolVersion = protocolVersion,
    ).map { case TopologyTransactionDiff(events, updateId, requiresLocalPartyReplication) =>
      EventInfo(
        Update.TopologyTransactionEffective(
          updateId = updateId,
          events = events,
          synchronizerId = synchronizerId,
          effectiveTime = effectiveStateChange.effectiveTime.value,
        ),
        requiresLocalPartyReplication,
      )
    }
}
