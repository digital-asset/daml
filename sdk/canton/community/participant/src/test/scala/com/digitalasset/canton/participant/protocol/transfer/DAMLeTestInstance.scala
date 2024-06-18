// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.Eval
import cats.data.EitherT
import cats.implicits.*
import com.daml.lf.engine.Error
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
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.participant.util.DAMLe
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
    val engine = DAMLe.newEngine(
      uniqueContractKeys = false,
      enableLfDev = false,
      enableLfPreview = false,
      enableStackTraces = false,
    )
    val timeouts = ProcessingTimeout()
    val packageDependencyResolver = new PackageDependencyResolver(
      new InMemoryDamlPackageStore(loggerFactory),
      timeouts,
      loggerFactory,
    )
    val packageUploader = new PackageUploader(
      engine = engine,
      hashOps = pureCrypto,
      eventPublisher = mock[ParticipantEventPublisher],
      packageDependencyResolver = packageDependencyResolver,
      enableUpgradeValidation = false,
      clock = new SimClock(loggerFactory = loggerFactory),
      futureSupervisor = FutureSupervisor.Noop,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )

    val mockPackageService =
      new PackageService(
        packageDependencyResolver,
        Eval.now(packageUploader),
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
      EngineLoggingConfig(),
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
