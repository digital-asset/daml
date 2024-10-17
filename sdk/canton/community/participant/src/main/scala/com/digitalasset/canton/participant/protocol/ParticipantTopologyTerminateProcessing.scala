// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.config.RequireTypes.NegativeLong
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, SequencerIndex, Update}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.PositiveStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.TopologyMapping
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{LedgerTransactionId, SequencerCounter, topology}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

object ParticipantTopologyTerminateProcessing {

  private[canton] val enabledWarningMessage =
    "Topology events are enabled. This is an experimental feature, unsafe for production use."

  private val minTopologyTieBreaker = NegativeLong.tryCreate(Long.MinValue + 1)

}

class ParticipantTopologyTerminateProcessing(
    domainId: DomainId,
    recordOrderPublisher: RecordOrderPublisher,
    store: TopologyStore[TopologyStoreId.DomainStore],
    override protected val loggerFactory: NamedLoggerFactory,
) extends topology.processing.TerminateProcessing
    with NamedLogging {

  import ParticipantTopologyTerminateProcessing.enabledWarningMessage

  noTracingLogger.warn(enabledWarningMessage)

  override def terminate(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Future[Unit] =
    for {
      events <- getNewEvents(sc, sequencedTime, effectiveTime)
      _ <-
        // TODO(i21243) This is a rudimentary first approach, and only proper if epsilon is 0
        recordOrderPublisher.tick(
          sc,
          sequencedTime.value,
          events.events.headOption.map(_ => Traced(events)),
          requestCounterO = None,
        )
    } yield ()

  private def queryStore(asOf: CantonTimestamp, asOfInclusive: Boolean)(implicit
      traceContext: TraceContext
  ): Future[PositiveStoredTopologyTransactions] =
    store.findPositiveTransactions(
      // the effectiveTime of topology transactions is exclusive. so if we want to find
      // the old and new state, we need to take the immediateSuccessor of the effectiveTime
      asOf = asOf,
      asOfInclusive = asOfInclusive,
      isProposal = false,
      types = Seq(TopologyMapping.Code.PartyToParticipant),
      filterUid = None,
      filterNamespace = None,
    )

  private def getNewEvents(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Update.TopologyTransactionEffective] = {

    val beforeF = queryStore(asOf = effectiveTime.value, asOfInclusive = false)
    val afterF = queryStore(asOf = effectiveTime.value, asOfInclusive = true)

    for {
      before <- beforeF
      after <- afterF
    } yield Update.TopologyTransactionEffective(
      updateId = randomUpdateId,
      events = TopologyTransactionDiff(before.signedTransactions, after.signedTransactions),
      // TODO(i21243) Use effective time when emitting with delay
      recordTime = sequencedTime.toLf,
      domainId = domainId,
      // TODO(i21243) Use effective time when emitting with delay
      domainIndex = DomainIndex.of(SequencerIndex(sc, sequencedTime.value)),
    )
  }

  // TODO(i21341): Create an update ID that would be the same across all participants,
  // submission-id is only stored in the LedgerServerPartyNotifier of the calling participant
  // hash of the transaction could play that role, because it is universally known to all
  // participants.
  private def randomUpdateId: LedgerTransactionId = {
    val bytes = new Array[Byte](8)
    scala.util.Random.nextBytes(bytes)
    LedgerTransactionId.assertFromString(
      Hash
        .digest(
          HashPurpose.TopologyTransactionSignature,
          ByteString.copyFrom(bytes),
          HashAlgorithm.Sha256,
        )
        .toHexString
    )
  }

}
