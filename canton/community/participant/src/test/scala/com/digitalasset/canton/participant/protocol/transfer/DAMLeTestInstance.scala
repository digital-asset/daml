// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.implicits.*
import com.daml.lf.engine.Error
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.participant.admin.{
  PackageDependencyResolver,
  PackageOpsForTesting,
  PackageService,
}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import org.mockito.MockitoSugar.mock

import scala.concurrent.{ExecutionContext, Future}

object DAMLeTestInstance {

  def apply(participant: ParticipantId, signatories: Set[LfPartyId], stakeholders: Set[LfPartyId])(
      loggerFactory: SuppressingLogger
  )(implicit ec: ExecutionContext): DAMLe = {
    val pureCrypto = new SymbolicPureCrypto
    val engine =
      DAMLe.newEngine(enableLfDev = false, enableStackTraces = false)
    val timeouts = ProcessingTimeout()
    val mockPackageService =
      new PackageService(
        engine,
        new PackageDependencyResolver(
          new InMemoryDamlPackageStore(loggerFactory),
          timeouts,
          loggerFactory,
        ),
        mock[ParticipantEventPublisher],
        pureCrypto,
        new PackageOpsForTesting(participant, loggerFactory),
        ParticipantTestMetrics,
        timeouts,
        loggerFactory,
      )
    val packageResolver = DAMLe.packageResolver(mockPackageService)
    new DAMLe(
      packageResolver,
      AuthorityResolver(),
      None,
      engine,
      loggerFactory,
    ) {
      override def contractMetadata(
          contractInstance: LfContractInst,
          supersetOfSignatories: Set[LfPartyId],
      )(implicit traceContext: TraceContext): EitherT[Future, Error, ContractMetadata] = {
        EitherT.pure[Future, Error](ContractMetadata.tryCreate(signatories, stakeholders, None))
      }
    }

  }
}
