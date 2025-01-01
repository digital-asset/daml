// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.platform.apiserver.execution.DynamicDomainParameterGetter
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class CantonDynamicDomainParameterGetter(
    syncCrypto: SyncCryptoApiProvider,
    protocolVersionFor: SynchronizerId => Option[ProtocolVersion],
    aliasManager: DomainAliasManager,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends DynamicDomainParameterGetter
    with NamedLogging {
  override def getLedgerTimeRecordTimeTolerance(synchronizerIdO: Option[SynchronizerId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, NonNegativeFiniteDuration] = {
    def getToleranceForDomain(
        synchronizerId: SynchronizerId,
        warnOnUsingDefault: Boolean,
    ): EitherT[FutureUnlessShutdown, String, NonNegativeFiniteDuration] =
      for {
        topoClient <- EitherT.fromOption[FutureUnlessShutdown](
          syncCrypto.ips.forDomain(synchronizerId),
          s"Cannot get topology client for domain $synchronizerId",
        )
        snapshot = topoClient.currentSnapshotApproximation
        protocolVersion <- EitherT.fromOption[FutureUnlessShutdown](
          protocolVersionFor(synchronizerId),
          s"Cannot get protocol version for domain $synchronizerId",
        )
        params <- EitherT.right(
          snapshot.findDynamicDomainParametersOrDefault(
            protocolVersion,
            warnOnUsingDefault,
          )
        )
      } yield params.ledgerTimeRecordTimeTolerance

    synchronizerIdO match {
      case Some(synchronizerId) => getToleranceForDomain(synchronizerId, warnOnUsingDefault = true)

      case None =>
        // TODO(i15313):
        // We should really receive a synchronizerId here, but this is not available within the ledger API for 2.x.
        // Instead, we retrieve the parameter for all defined domains, and return the maximum value.
        val domainAliases = aliasManager.ids.toSeq

        for {
          _ <- EitherT.fromOption[FutureUnlessShutdown](
            NonEmpty.from(domainAliases),
            "No domain defined",
          )
          allTolerances <- EitherT.right(domainAliases.parTraverseFilter { synchronizerId =>
            if (aliasManager.connectionStateForDomain(synchronizerId).exists(_.isActive)) {
              getToleranceForDomain(
                synchronizerId,
                warnOnUsingDefault = // don't warn as the domain parameters might not be set up yet
                  false,
              )
                .leftMap { error =>
                  logger.info(
                    s"Failed to get ledger time tolerance for domain $synchronizerId: $error"
                  )
                }
                .toOption
                .value
            } else FutureUnlessShutdown.pure(None)
          })
          allTolerancesNE <- EitherT.fromOption[FutureUnlessShutdown](
            NonEmpty.from(allTolerances),
            "All defined domains returned errors",
          )
        } yield allTolerancesNE.max1
    }
  }
}
