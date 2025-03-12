// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.crypto.kms.CommunityKmsFactory
import com.digitalasset.canton.crypto.store.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.environment.{
  CantonNodeBootstrapCommonArguments,
  NodeFactoryArguments,
}
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.participant.admin.ResourceManagementService
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.ParticipantSettingsStore
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.resource.CommunityStorageFactory
import com.digitalasset.canton.time.TestingTimeService
import com.digitalasset.daml.lf.engine.Engine
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService

trait ParticipantNodeBootstrapFactory {

  type Arguments =
    CantonNodeBootstrapCommonArguments[
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ]

  protected def createEngine(arguments: Arguments): Engine = DAMLe.newEngine(
    enableLfDev = arguments.parameterConfig.alphaVersionSupport,
    enableLfBeta = arguments.parameterConfig.betaVersionSupport,
    enableStackTraces = arguments.parameterConfig.engine.enableEngineStackTraces,
    profileDir = arguments.config.features.profileDir,
    iterationsBetweenInterruptions = arguments.parameterConfig.engine.iterationsBetweenInterruptions,
  )

  protected def createResourceService(
      arguments: Arguments
  )(store: Eval[ParticipantSettingsStore]): ResourceManagementService =
    new ResourceManagementService(
      store,
      arguments.config.parameters.warnIfOverloadedFor.map(_.toInternal),
      arguments.metrics,
    )

  protected def createLedgerApiServerFactory(
      arguments: Arguments,
      engine: Engine,
      testingTimeService: TestingTimeService,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      actorSystem: ActorSystem,
  ): CantonLedgerApiServerFactory

  def create(
      arguments: NodeFactoryArguments[
        LocalParticipantConfig,
        ParticipantNodeParameters,
        ParticipantMetrics,
      ],
      testingTimeService: TestingTimeService,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      actorSystem: ActorSystem,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): Either[String, ParticipantNodeBootstrap]

}

object CommunityParticipantNodeBootstrapFactory extends ParticipantNodeBootstrapFactory {

  private def createReplicationServiceFactory(
      arguments: Arguments
  ): ServerServiceDefinition =
    StaticGrpcServices
      .notSupportedByCommunity(
        v30.EnterpriseParticipantReplicationServiceGrpc.SERVICE,
        arguments.loggerFactory,
      )

  override protected def createLedgerApiServerFactory(
      arguments: Arguments,
      engine: Engine,
      testingTimeService: TestingTimeService,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      actorSystem: ActorSystem,
  ): CantonLedgerApiServerFactory =
    new CantonLedgerApiServerFactory(
      engine = engine,
      clock = arguments.clock,
      testingTimeService = testingTimeService,
      allocateIndexerLockIds = _ => Option.empty[IndexerLockIds].asRight,
      meteringReportKey = CommunityKey,
      futureSupervisor = arguments.futureSupervisor,
      loggerFactory = arguments.loggerFactory,
    )

  override def create(
      arguments: NodeFactoryArguments[
        LocalParticipantConfig,
        ParticipantNodeParameters,
        ParticipantMetrics,
      ],
      testingTimeService: TestingTimeService,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      actorSystem: ActorSystem,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): Either[String, ParticipantNodeBootstrap] =
    arguments
      .toCantonNodeBootstrapCommonArguments(
        new CommunityStorageFactory(arguments.config.storage),
        new CommunityCryptoPrivateStoreFactory(
          arguments.config.crypto.provider,
          arguments.config.crypto.kms,
          CommunityKmsFactory,
          arguments.config.parameters.caching.kmsMetadataCache,
          arguments.config.crypto.privateKeyStore,
          arguments.parameters.nonStandardConfig,
          arguments.futureSupervisor,
          arguments.clock,
          arguments.executionContext,
        ),
        CommunityKmsFactory,
      )
      .map { arguments =>
        val engine = createEngine(arguments)
        createNode(
          arguments,
          createLedgerApiServerFactory(
            arguments,
            engine,
            testingTimeService,
          ),
        )
      }

  private def createNode(
      arguments: Arguments,
      ledgerApiServerFactory: CantonLedgerApiServerFactory,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      actorSystem: ActorSystem,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): ParticipantNodeBootstrap =
    new ParticipantNodeBootstrap(
      arguments,
      createEngine(arguments),
      CantonSyncService.DefaultFactory,
      createResourceService(arguments),
      _ => createReplicationServiceFactory(arguments),
      ledgerApiServerFactory = ledgerApiServerFactory,
      setInitialized = _ => (),
    )
}
