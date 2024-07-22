// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.SequencedTime
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

trait TopologyStateForInitializationService {
  def initialSnapshot(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[GenericStoredTopologyTransactions]
}

final class StoreBasedTopologyStateForInitializationService(
    domainTopologyStore: TopologyStore[DomainStore],
    val loggerFactory: NamedLoggerFactory,
) extends TopologyStateForInitializationService
    with NamedLogging {

  /** Downloading the initial topology snapshot works as follows:
    *
    * 1. Determine the first MediatorDomainState or DomainTrustCertificate that mentions the member to onboard.
    * 2. Take its effective time (here t0')
    * 3. Find all transactions with sequence time <= t0'
    * 4. Find the maximum effective time of the transactions returned in 3. (here ts1')
    * 5. Set all validUntil > ts1' to None
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
    *    }}}
    */
  override def initialSnapshot(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[GenericStoredTopologyTransactions] = {
    val effectiveFromF = member match {
      case participant @ ParticipantId(_) =>
        domainTopologyStore
          .findFirstTrustCertificateForParticipant(participant)
          .map(_.map(_.validFrom))
      case mediator @ MediatorId(_) =>
        domainTopologyStore.findFirstMediatorStateForMediator(mediator).map(_.map(_.validFrom))
      case SequencerId(_) =>
        Future.failed(
          Status.INVALID_ARGUMENT
            .withDescription(
              s"Downloading the initial topology snapshot for sequencers is not supported."
            )
            .asException()
        )
    }

    effectiveFromF.flatMap { effectiveFromO =>
      effectiveFromO
        .map { effectiveFrom =>
          logger.debug(s"Fetching initial topology state for $member at $effectiveFrom")
          // This is not a mistake: all transactions with `sequenced <= validFrom` need to come from this onboarding snapshot
          // because the member only receives transactions once its onboarding transaction becomes effective.
          val referenceSequencedTime = SequencedTime(effectiveFrom.value)
          domainTopologyStore.findEssentialStateAtSequencedTime(referenceSequencedTime)
        }
        .getOrElse(
          domainTopologyStore.maxTimestamp().flatMap { maxTimestamp =>
            Future.failed(
              Status.FAILED_PRECONDITION
                .withDescription(s"No onboarding transaction found for $member as of $maxTimestamp")
                .asException()
            )
          }
        )
    }
  }
}
