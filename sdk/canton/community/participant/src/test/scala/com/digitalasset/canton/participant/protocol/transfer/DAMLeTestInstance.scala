// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.participant.admin.{
  PackageDependencyResolver,
  PackageOpsForTesting,
  PackageService,
  PackageUploader,
}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.EngineController.GetEngineAbortStatus
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.util.DAMLe.ReinterpretationError
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.time.SimClock
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

    val packageDependencyResolver = new PackageDependencyResolver(
      new InMemoryDamlPackageStore(loggerFactory),
      timeouts,
      loggerFactory,
    )
    val packageUploader = new PackageUploader(
      clock = new SimClock(loggerFactory = loggerFactory),
      engine = engine,
      hashOps = pureCrypto,
      eventPublisher = mock[ParticipantEventPublisher],
      packageDependencyResolver = packageDependencyResolver,
      enableUpgradeValidation = false,
      futureSupervisor = FutureSupervisor.Noop,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
      packageMetadataView = NoopPackageMetadataView,
    )
    val mockPackageService = new PackageService(
      packageDependencyResolver = packageDependencyResolver,
      packageUploader = packageUploader,
      packageMetadataView = NoopPackageMetadataView,
      packageOps = new PackageOpsForTesting(participant, loggerFactory),
      metrics = ParticipantTestMetrics,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )
    val packageResolver = DAMLe.packageResolver(mockPackageService)
    new DAMLe(
      packageResolver,
      AuthorityResolver(),
      None,
      engine,
      EngineLoggingConfig(),
      loggerFactory,
    ) {
      override def contractMetadata(
          contractInstance: LfContractInst,
          supersetOfSignatories: Set[LfPartyId],
          getEngineAbortStatus: GetEngineAbortStatus,
      )(implicit
          traceContext: TraceContext
      ): EitherT[Future, ReinterpretationError, ContractMetadata] = {
        EitherT.pure[Future, ReinterpretationError](
          ContractMetadata.tryCreate(signatories, stakeholders, None)
        )
      }
    }

  }
}
