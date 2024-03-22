// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.platform.apiserver.execution.DynamicDomainParameterGetter
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class CantonDynamicDomainParameterGetter(
    syncCrypto: SyncCryptoApiProvider,
    protocolVersionFor: DomainId => Option[ProtocolVersion],
    aliasManager: DomainAliasManager,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends DynamicDomainParameterGetter
    with NamedLogging {
  override def getLedgerTimeRecordTimeTolerance(domainIdO: Option[DomainId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, NonNegativeFiniteDuration] = {
    def getToleranceForDomain(
        domainId: DomainId
    ): EitherT[Future, String, NonNegativeFiniteDuration] =
      for {
        topoClient <- EitherT.fromOption[Future](
          syncCrypto.ips.forDomain(domainId),
          s"Cannot get topology client for domain $domainId",
        )
        snapshot = topoClient.currentSnapshotApproximation
        protocolVersion <- EitherT.fromOption[Future](
          protocolVersionFor(domainId),
          s"Cannot get protocol version for domain $domainId",
        )
        params <- EitherT.right(snapshot.findDynamicDomainParametersOrDefault(protocolVersion))
      } yield params.ledgerTimeRecordTimeTolerance

    domainIdO match {
      case Some(domainId) => getToleranceForDomain(domainId)

      case None =>
        // TODO(i15313):
        // We should really receive a domainId here, but this is not available within the ledger API for 2.x.
        // Instead, we retrieve the parameter for all defined domains, and return the maximum value.
        val domainAliases = aliasManager.ids.toSeq

        for {
          _ <- EitherT.fromOption[Future](
            NonEmpty.from(domainAliases),
            "No domain defined",
          )
          allTolerances <- EitherT.right(domainAliases.parTraverseFilter { domainId =>
            getToleranceForDomain(domainId)
              .leftMap { error =>
                logger.info(s"Failed to get ledger time tolerance for domain $domainId: $error")
              }
              .toOption
              .value
          })
          allTolerancesNE <- EitherT.fromOption[Future](
            NonEmpty.from(allTolerances),
            "All defined domains returned errors",
          )
        } yield allTolerancesNE.max1
    }
  }
}
