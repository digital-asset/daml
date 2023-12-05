// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology.client

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.NoTracing

import scala.concurrent.{ExecutionContext, Future}

/** checks and notifies once the domain is initialized */
private[domain] class DomainInitializationObserver(
    domainId: DomainId,
    client: DomainTopologyClient,
    sequencedStore: TopologyStore[TopologyStoreId],
    mustHaveActiveMediator: Boolean,
    processingTimeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with NoTracing {

  private def initialisedAt(timestamp: CantonTimestamp): EitherT[Future, String, Unit] =
    DomainTopologyManager.isInitializedAt(
      domainId,
      sequencedStore,
      timestamp,
      mustHaveActiveMediator,
      loggerFactory,
    )

  /** returns unit if the initialisation data exists (but might not yet be effective) */
  def initialisedAtHead: Future[Boolean] =
    sequencedStore.timestamp().flatMap {
      case Some((_, effective)) =>
        initialisedAt(effective.value.immediateSuccessor).value.map(_.isRight)
      case None => Future.successful(false)
    }

  /** future that will complete once the domain has the correct initialisation data AND the data is effective */
  val waitUntilInitialisedAndEffective: FutureUnlessShutdown[Boolean] = client
    .await(
      snapshot => {
        // this is a bit stinky, but we are using the "check on a change" mechanism of the
        // normal client to just get notified whenever there was an update to the topology state
        initialisedAt(snapshot.timestamp).value.map {
          case Left(missing) =>
            logger.debug(s"Domain is not ready at=${snapshot.timestamp} due to ${missing}.")
            false
          case Right(_) => true
        }
      },
      processingTimeouts.unbounded.duration,
    )
    .map { init =>
      if (init)
        logger.debug("Domain is now initialised and effective.")
      else
        logger.error("Domain initialisation failed!")
      init
    }

}

private[domain] object DomainInitializationObserver {
  def apply(
      domainId: DomainId,
      client: DomainTopologyClient,
      sequencedStore: TopologyStore[TopologyStoreId.DomainStore],
      mustHaveActiveMediator: Boolean,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): Future[DomainInitializationObserver] = {
    val obs =
      new DomainInitializationObserver(
        domainId,
        client,
        sequencedStore,
        mustHaveActiveMediator,
        processingTimeout,
        loggerFactory,
      )
    Future.successful(obs)
  }
}
