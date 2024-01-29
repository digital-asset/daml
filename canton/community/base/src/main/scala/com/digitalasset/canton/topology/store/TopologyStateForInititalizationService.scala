// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait TopologyStateForInitializationService {
  def initialSnapshot(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[GenericStoredTopologyTransactionsX]
}

final class StoreBasedTopologyStateForInitializationService(
    domainTopologyStore: TopologyStoreX[DomainStore],
    val loggerFactory: NamedLoggerFactory,
) extends TopologyStateForInitializationService
    with NamedLogging {

  /** Downloading the initial topology snapshot works as follows:
    *
    * 1. Determine the first MediatorDomainStateX or DomainTrustCertificateX that mentions the member to onboard.
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
  ): Future[GenericStoredTopologyTransactionsX] = {
    val effectiveFromF = member match {
      case participant @ ParticipantId(_) =>
        domainTopologyStore
          .findFirstTrustCertificateForParticipant(participant)
          .map(_.map(_.validFrom))
      case mediator @ MediatorId(_) =>
        domainTopologyStore.findFirstMediatorStateForMediator(mediator).map(_.map(_.validFrom))
      case _ =>
        // TODO(#12390) proper error
        ???
    }

    effectiveFromF.flatMap { effectiveFromO =>
      effectiveFromO
        .map { effectiveFrom =>
          logger.debug(s"Fetching initial topology state for $member at $effectiveFrom")
          domainTopologyStore.findEssentialStateForMember(member, effectiveFrom.value)
        }
        // TODO(#12390) should this error out if nothing can be found?
        .getOrElse(Future.successful(StoredTopologyTransactionsX.empty))
    }
  }
}
