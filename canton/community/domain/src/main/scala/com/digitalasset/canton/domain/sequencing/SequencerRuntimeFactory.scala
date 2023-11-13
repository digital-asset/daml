// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import org.apache.pekko.actor.ActorSystem
import cats.data.EitherT
import cats.syntax.option.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{Crypto, DomainSyncCryptoClient}
import com.digitalasset.canton.domain.admin.v0.EnterpriseSequencerAdministrationServiceGrpc
import com.digitalasset.canton.domain.config.DomainConfig
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.authentication.MemberAuthenticationServiceFactory
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.domain.sequencing.sequencer.{
  CommunityDatabaseSequencerFactory,
  CommunitySequencerConfig,
  Sequencer,
}
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.TopologyStateForInitializationService
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.{ExecutionContextExecutor, Future}

trait SequencerParameters {
  def maxBurstFactor: PositiveDouble

  def processingTimeouts: ProcessingTimeout
}

trait CantonNodeWithSequencerParameters extends CantonNodeParameters with SequencerParameters

trait SequencerRuntimeFactory {
  def create(
      domainId: DomainId,
      sequencerId: SequencerId,
      crypto: Crypto,
      topologyClientMember: Member,
      topologyClient: DomainTopologyClientWithInit,
      storage: Storage,
      clock: Clock,
      domainConfig: DomainConfig,
      staticDomainParameters: StaticDomainParameters,
      testingConfig: TestingConfigInternal,
      processingTimeout: ProcessingTimeout,
      auditLogger: TracedLogger,
      agreementManager: Option[ServiceAgreementManager],
      memberAuthenticationServiceFactory: MemberAuthenticationServiceFactory,
      localParameters: CantonNodeWithSequencerParameters,
      metrics: SequencerMetrics,
      indexedStringStore: IndexedStringStore,
      futureSupervisor: FutureSupervisor,
      topologyStateForInitializationService: Option[TopologyStateForInitializationService],
      rateLimitManager: Option[SequencerRateLimitManager],
      topologyManagerStatusO: Option[TopologyManagerStatus],
      loggerFactory: NamedLoggerFactory,
      logger: TracedLogger,
  )(implicit
      executionContext: ExecutionContextExecutor,
      esf: ExecutionSequencerFactory,
      scheduler: ScheduledExecutorService,
      tracer: Tracer,
      system: ActorSystem,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerRuntime]
}

object SequencerRuntimeFactory {
  class Community(sequencerConfig: CommunitySequencerConfig.Database)
      extends SequencerRuntimeFactory {
    override def create(
        domainId: DomainId,
        sequencerId: SequencerId,
        crypto: Crypto,
        topologyClientMember: Member,
        topologyClient: DomainTopologyClientWithInit,
        storage: Storage,
        clock: Clock,
        domainConfig: DomainConfig,
        staticDomainParameters: StaticDomainParameters,
        testingConfig: TestingConfigInternal,
        processingTimeout: ProcessingTimeout,
        auditLogger: TracedLogger,
        agreementManager: Option[ServiceAgreementManager],
        memberAuthenticationServiceFactory: MemberAuthenticationServiceFactory,
        localParameters: CantonNodeWithSequencerParameters,
        metrics: SequencerMetrics,
        indexedStringStore: IndexedStringStore,
        futureSupervisor: FutureSupervisor,
        topologyStateForInitializationService: Option[TopologyStateForInitializationService],
        rateLimitManager: Option[SequencerRateLimitManager],
        topologyManagerStatusO: Option[TopologyManagerStatus],
        loggerFactory: NamedLoggerFactory,
        logger: TracedLogger,
    )(implicit
        executionContext: ExecutionContextExecutor,
        esf: ExecutionSequencerFactory,
        scheduler: ScheduledExecutorService,
        tracer: Tracer,
        system: ActorSystem,
        traceContext: TraceContext,
    ): EitherT[Future, String, SequencerRuntime] = {

      val sequencerFactory = new CommunityDatabaseSequencerFactory(
        sequencerConfig,
        metrics,
        storage,
        staticDomainParameters.protocolVersion,
        topologyClientMember,
        localParameters,
        loggerFactory,
      )

      val syncCrypto = new DomainSyncCryptoClient(
        sequencerId,
        domainId,
        topologyClient,
        crypto,
        localParameters.cachingConfigs,
        processingTimeout,
        futureSupervisor,
        loggerFactory,
      )

      for {
        sequencer <- EitherT.liftF[Future, String, Sequencer](
          sequencerFactory
            .create(
              domainId,
              sequencerId,
              clock,
              clock,
              syncCrypto,
              futureSupervisor,
              rateLimitManager,
              implicitMemberRegistration = false,
            )
        )

        sequencerRuntime = new SequencerRuntime(
          sequencerId,
          sequencer,
          staticDomainParameters,
          localParameters,
          domainConfig.publicApi,
          metrics,
          domainId,
          syncCrypto,
          topologyClient,
          topologyManagerStatusO,
          storage,
          clock,
          auditLogger,
          SequencerAuthenticationConfig(
            agreementManager,
            domainConfig.publicApi.nonceExpirationTime,
            domainConfig.publicApi.tokenExpirationTime,
          ),
          _ =>
            StaticGrpcServices
              .notSupportedByCommunity(EnterpriseSequencerAdministrationServiceGrpc.SERVICE, logger)
              .some,
          DomainMember
            .list(domainId, includeSequencer = false)
            .toList, // the community sequencer is always an embedded single sequencer
          futureSupervisor,
          agreementManager,
          memberAuthenticationServiceFactory,
          topologyStateForInitializationService,
          loggerFactory,
        )

        _ <- sequencerRuntime.initialize()
      } yield sequencerRuntime
    }
  }
}
