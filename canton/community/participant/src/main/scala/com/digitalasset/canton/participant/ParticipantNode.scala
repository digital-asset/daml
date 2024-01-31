// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.Engine
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.crypto.CommunityCryptoFactory
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.CommunityGrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.participant.admin.ResourceManagementService
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.time.*
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService

// TODO(#15161): Merge into ParticipantNodeBootstrapX
object ParticipantNodeBootstrap {
  val LoggerFactoryKeyName: String = "participant"

  trait Factory[PC <: LocalParticipantConfig, B <: CantonNodeBootstrap[_]] {

    type Arguments =
      CantonNodeBootstrapCommonArguments[PC, ParticipantNodeParameters, ParticipantMetrics]

    protected def createEngine(arguments: Arguments): Engine

    protected def createResourceService(
        arguments: Arguments
    )(store: Eval[ParticipantSettingsStore]): ResourceManagementService

    protected def createLedgerApiServerFactory(
        arguments: Arguments,
        engine: Engine,
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        actorSystem: ActorSystem,
    ): CantonLedgerApiServerFactory

    def create(
        arguments: NodeFactoryArguments[PC, ParticipantNodeParameters, ParticipantMetrics],
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): Either[String, B]
  }

  abstract class CommunityParticipantFactoryCommon[B <: CantonNodeBootstrap[_]]
      extends Factory[CommunityParticipantConfig, B] {

    override protected def createEngine(arguments: Arguments): Engine =
      DAMLe.newEngine(
        enableLfDev = arguments.parameterConfig.devVersionSupport,
        enableStackTraces = arguments.parameterConfig.enableEngineStackTrace,
        enableContractUpgrading = arguments.parameterConfig.enableContractUpgrading,
        iterationsBetweenInterruptions = arguments.parameterConfig.iterationsBetweenInterruptions,
      )

    override protected def createResourceService(
        arguments: Arguments
    )(store: Eval[ParticipantSettingsStore]): ResourceManagementService =
      new ResourceManagementService.CommunityResourceManagementService(
        arguments.config.parameters.warnIfOverloadedFor.map(_.toInternal),
        arguments.metrics,
      )

    protected def createReplicationServiceFactory(
        arguments: Arguments
    )(storage: Storage): ServerServiceDefinition =
      StaticGrpcServices
        .notSupportedByCommunity(
          EnterpriseParticipantReplicationServiceGrpc.SERVICE,
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
        allocateIndexerLockIds = _dbConfig => Option.empty[IndexerLockIds].asRight,
        meteringReportKey = CommunityKey,
        futureSupervisor = arguments.futureSupervisor,
        loggerFactory = arguments.loggerFactory,
        multiDomainEnabled = multiDomainEnabledForLedgerApiServer,
      )

    protected def multiDomainEnabledForLedgerApiServer: Boolean

    protected def createNode(
        arguments: Arguments,
        engine: Engine,
        ledgerApiServerFactory: CantonLedgerApiServerFactory,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): B

    override def create(
        arguments: NodeFactoryArguments[
          CommunityParticipantConfig,
          ParticipantNodeParameters,
          ParticipantMetrics,
        ],
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): Either[String, B] =
      arguments
        .toCantonNodeBootstrapCommonArguments(
          new CommunityStorageFactory(arguments.config.storage),
          new CommunityCryptoFactory,
          new CommunityCryptoPrivateStoreFactory,
          new CommunityGrpcVaultServiceFactory,
        )
        .map { arguments =>
          val engine = createEngine(arguments)
          createNode(
            arguments,
            engine,
            createLedgerApiServerFactory(
              arguments,
              engine,
              testingTimeService,
            ),
          )
        }

  }
}
