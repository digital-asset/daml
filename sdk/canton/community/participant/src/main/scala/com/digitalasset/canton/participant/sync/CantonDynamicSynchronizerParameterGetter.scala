// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager
import com.digitalasset.canton.platform.apiserver.execution.DynamicSynchronizerParameterGetter
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class CantonDynamicSynchronizerParameterGetter(
    syncCrypto: SyncCryptoApiParticipantProvider,
    protocolVersionFor: SynchronizerId => Option[ProtocolVersion],
    aliasManager: SynchronizerAliasManager,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends DynamicSynchronizerParameterGetter
    with NamedLogging {
  override def getLedgerTimeRecordTimeTolerance(synchronizerIdO: Option[SynchronizerId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, NonNegativeFiniteDuration] = {
    def getToleranceForSynchronizer(
        synchronizerId: SynchronizerId,
        warnOnUsingDefault: Boolean,
    ): EitherT[FutureUnlessShutdown, String, NonNegativeFiniteDuration] =
      for {
        topoClient <- EitherT.fromOption[FutureUnlessShutdown](
          syncCrypto.ips.forSynchronizer(synchronizerId),
          s"Cannot get topology client for synchronizer $synchronizerId",
        )
        snapshot = topoClient.currentSnapshotApproximation
        protocolVersion <- EitherT.fromOption[FutureUnlessShutdown](
          protocolVersionFor(synchronizerId),
          s"Cannot get protocol version for synchronizer $synchronizerId",
        )
        params <- EitherT.right(
          snapshot.findDynamicSynchronizerParametersOrDefault(
            protocolVersion,
            warnOnUsingDefault,
          )
        )
      } yield params.ledgerTimeRecordTimeTolerance

    synchronizerIdO match {
      case Some(synchronizerId) =>
        getToleranceForSynchronizer(synchronizerId, warnOnUsingDefault = true)

      case None =>
        // TODO(i15313):
        // We should really receive a synchronizerId here, but this is not available within the ledger API for 2.x.
        // Instead, we retrieve the parameter for all defined synchronizers, and return the maximum value.
        val synchronizerAliases = aliasManager.ids.toSeq

        for {
          _ <- EitherT.fromOption[FutureUnlessShutdown](
            NonEmpty.from(synchronizerAliases),
            "No synchronizer defined",
          )
          allTolerances <- EitherT.right(synchronizerAliases.parTraverseFilter { synchronizerId =>
            if (aliasManager.connectionStateForSynchronizer(synchronizerId).exists(_.isActive)) {
              getToleranceForSynchronizer(
                synchronizerId,
                warnOnUsingDefault = // don't warn as the synchronizer parameters might not be set up yet
                  false,
              )
                .leftMap { error =>
                  logger.info(
                    s"Failed to get ledger time tolerance for synchronizer $synchronizerId: $error"
                  )
                }
                .toOption
                .value
            } else FutureUnlessShutdown.pure(None)
          })
          allTolerancesNE <- EitherT.fromOption[FutureUnlessShutdown](
            NonEmpty.from(allTolerances),
            "All defined synchronizers returned errors",
          )
        } yield allTolerancesNE.max1
    }
  }
}
