// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import io.grpc.Status
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.ExecutionContext

trait TopologyStateForInitializationService {
  def initialSnapshot(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Source[GenericStoredTopologyTransaction, NotUsed]

  def initialSnapshotHash(member: Member)(implicit
      executionContext: ExecutionContext,
      materializer: Materializer,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Hash]
}

final class StoreBasedTopologyStateForInitializationService(
    synchronizerTopologyStore: TopologyStore[SynchronizerStore],
    sequencingTimeLowerBoundExclusive: Option[CantonTimestamp],
    val loggerFactory: NamedLoggerFactory,
) extends TopologyStateForInitializationService
    with NamedLogging {

  private def getReferenceTime(
      member: Member
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] = {
    val effectiveFromF = member match {
      case participant @ ParticipantId(_) =>
        synchronizerTopologyStore
          .findFirstTrustCertificateForParticipant(participant)
          .map(_.map(_.validFrom))
      case mediator @ MediatorId(_) =>
        synchronizerTopologyStore
          .findFirstMediatorStateForMediator(mediator)
          .map(_.map(_.validFrom))
      case SequencerId(_) =>
        FutureUnlessShutdown.failed(
          Status.INVALID_ARGUMENT
            .withDescription(
              s"Downloading the initial topology snapshot for sequencers is not supported."
            )
            .asException()
        )
    }

    effectiveFromF.map { effectiveFromO =>
      effectiveFromO
        .map { effectiveFrom =>
          // This is not a mistake: all transactions with
          // `sequenced <= max(validFrom, minimumSequencingTime.immediatePredecessor)` need to come from this onboarding
          // snapshot, because the member only receives transactions that are sequenced:
          // * after the onboarding transaction has become effective
          // * with minimum sequencing time or later
          val sequencedTime = SequencedTime(
            sequencingTimeLowerBoundExclusive.fold(effectiveFrom.value)(_.max(effectiveFrom.value))
          )
          (sequencedTime, effectiveFrom)
        }
    }
  }

  /** Downloading the initial topology snapshot works as follows:
    *
    *   1. Determine the first MediatorSynchronizerState or SynchronizerTrustCertificate that
    *      mentions the member to onboard.
    *   1. Take its effective time (here t0')
    *   1. Find all transactions with sequence time <= t0'
    *   1. Find the maximum effective time of the transactions returned in 3. (here t1')
    *   1. Set all validUntil > t1' to None
    *
    * {{{
    *
    * t0 , t1   ... sequenced time
    * t0', t1'  ... effective time
    *
    *                           xxxxxxxxxxxxx
    *                         xxx           xxx
    *               t0        x       t0'     xx
    *      │         │        │        │       │
    *      ├─────────┼────────┼────────┼───────┼────────►
    *      │         │        │        │       │
    *                x       t1        x      t1'
    *                xx               xx
    *                 xx             xx
    *                   xx MDS/DTC xx
    * }}}
    */
  override def initialSnapshot(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Source[GenericStoredTopologyTransaction, NotUsed] = {
    val referenceSequencedTimeF = getReferenceTime(member)
    val sourceF = referenceSequencedTimeF.map { referenceSequencedTimeO =>
      referenceSequencedTimeO
        .map { case (referenceSequencedTime, effectiveFrom) =>
          logger.debug(
            s"Fetching initial topology state at ${referenceSequencedTime.value} for $member active at $effectiveFrom"
          )
          synchronizerTopologyStore.findEssentialStateAtSequencedTime(
            referenceSequencedTime,
            // we need to include rejected transactions, because they might have an impact on the TopologyTimestampPlusEpsilonTracker
            includeRejected = true,
          )
        }
        .getOrElse(
          PekkoUtil
            .futureSourceUS(
              synchronizerTopologyStore
                .maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
                .flatMap { maxTimestamp =>
                  FutureUnlessShutdown.failed(
                    Status.FAILED_PRECONDITION
                      .withDescription(
                        s"No onboarding transaction found for $member as of $maxTimestamp"
                      )
                      .asException()
                  )
                }
            )
        )
    }
    PekkoUtil.futureSourceUS(sourceF)
  }

  override def initialSnapshotHash(member: Member)(implicit
      executionContext: ExecutionContext,
      materializer: Materializer,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Hash] =
    getReferenceTime(member).flatMap { referenceSequencedTimeO =>
      referenceSequencedTimeO
        .map { case (referenceSequencedTime, effectiveFrom) =>
          logger.debug(
            s"Computing initial topology state hash at ${referenceSequencedTime.value} for $member active at $effectiveFrom"
          )
          synchronizerTopologyStore
            .findEssentialStateHashAtSequencedTime(
              referenceSequencedTime
            )
        }
        .getOrElse(
          synchronizerTopologyStore
            .maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
            .flatMap { maxTimestamp =>
              FutureUnlessShutdown.failed(
                Status.FAILED_PRECONDITION
                  .withDescription(
                    s"No onboarding transaction found for $member as of $maxTimestamp"
                  )
                  .asException()
              )
            }
        )
    }

}
