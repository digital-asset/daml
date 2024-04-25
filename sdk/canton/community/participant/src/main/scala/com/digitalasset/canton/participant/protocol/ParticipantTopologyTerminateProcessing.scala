// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.parallel.*
import com.digitalasset.canton.config.RequireTypes.NegativeLong
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.TopologyOffset
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.PositiveStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.TopologyMapping
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.{SequencerCounter, topology}

import scala.concurrent.{ExecutionContext, Future}

class ParticipantTopologyTerminateProcessingTicker(
    recordOrderPublisher: RecordOrderPublisher,
    override protected val loggerFactory: NamedLoggerFactory,
) extends topology.processing.TerminateProcessing
    with NamedLogging {

  override def terminate(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    recordOrderPublisher.tick(sc, sequencedTime.value)
    Future.unit
  }
}

class ParticipantTopologyTerminateProcessing(
    recordOrderPublisher: RecordOrderPublisher,
    store: TopologyStore[TopologyStoreId.DomainStore],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends topology.processing.TerminateProcessing
    with NamedLogging {
  private val topologyTransactionsToEvents: TopologyTransactionsToEvents =
    new TopologyTransactionsToEvents(loggerFactory)

  override def terminate(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val minimumTieBreaker = Long.MinValue + 1

    def offset(idx: Int) =
      TopologyOffset.tryCreate(effectiveTime, NegativeLong.tryCreate(minimumTieBreaker + idx))

    for {
      events <- getNewEvents(sequencedTime, effectiveTime)

      _ <-
        if (events.nonEmpty) {
          logger.debug(
            s"Batch of topology transactions with sc=$sc yielded ${events.size} new events"
          )

          events.zipWithIndex.parTraverse_ { case (event, idx) =>
            val localOffset = offset(idx)
            val timestampedEvent = TimestampedEvent(event, localOffset, None)

            recordOrderPublisher.schedulePublication(sc, localOffset, timestampedEvent)
          }

        } else Future.unit

      _ = recordOrderPublisher.tick(sc, sequencedTime.value)
    } yield ()
  }

  private def getNewEvents(
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit traceContext: TraceContext): Future[Seq[LedgerSyncEvent]] = {
    def queryStore(asOfInclusive: Boolean): Future[PositiveStoredTopologyTransactions] =
      store.findPositiveTransactions(
        // the effectiveTime of topology transactions is exclusive. so if we want to find
        // the old and new state, we need to take the immediateSuccessor of the effectiveTime
        asOf = effectiveTime.value.immediateSuccessor,
        asOfInclusive = asOfInclusive,
        isProposal = false,
        types = Seq(TopologyMapping.Code.PartyToParticipant),
        filterUid = None,
        filterNamespace = None,
      )

    for {
      old <- queryStore(asOfInclusive = false)
      current <- queryStore(asOfInclusive = true)

      events = topologyTransactionsToEvents.events(
        sequencedTime,
        effectiveTime,
        old.signedTransactions,
        current.signedTransactions,
      )

    } yield events
  }

}
