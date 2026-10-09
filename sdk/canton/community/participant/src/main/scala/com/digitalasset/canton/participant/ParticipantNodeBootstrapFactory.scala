// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.admin.participant.v30.ParticipantReplicationServiceGrpc
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.environment.{
  CantonNodeBootstrapCommonArguments,
  NodeFactoryArguments,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.LedgerApiServerBootstrapUtils.IndexerLockIds
import com.digitalasset.canton.participant.admin.ResourceManagementService
import com.digitalasset.canton.participant.admin.grpc.GrpcParticipantReplicationService
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.replica.ParticipantReplicaManager
import com.digitalasset.canton.participant.store.ParticipantSettingsStore
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.resource.{DbLockCounters, DbLockId, Storage, StorageMultiFactory}
import com.digitalasset.canton.time.TestingTimeService
import com.digitalasset.daml.lf.engine.Engine
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext

trait ParticipantNodeBootstrapFactory {

  type Arguments =
    CantonNodeBootstrapCommonArguments[
      ParticipantNodeConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ]

  protected def createEngine(arguments: Arguments): Engine = DAMLe.newEngine(
    enableLfDev = arguments.parameterConfig.alphaVersionSupport,
    enableLfBeta = arguments.parameterConfig.betaVersionSupport,
    enableStackTraces = arguments.parameterConfig.engine.enableEngineStackTraces,
    profileDir = arguments.config.features.profileDir,
    snapshotDir = arguments.config.features.snapshotDir,
    iterationsBetweenInterruptions =
      arguments.parameterConfig.engine.iterationsBetweenInterruptions,
    paranoidMode = arguments.parameterConfig.engine.enableAdditionalConsistencyChecks,
  )

  protected def createResourceService(
      arguments: Arguments
  )(store: Eval[ParticipantSettingsStore]): ResourceManagementService =
    new ResourceManagementService(
      store,
      arguments.config.parameters.warnIfOverloadedFor.map(_.toInternal),
      arguments.metrics,
    )

  protected def createLedgerApiBootstrapUtils(
      arguments: Arguments,
      engine: Engine,
      testingTimeService: TestingTimeService,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      actorSystem: ActorSystem,
  ): LedgerApiServerBootstrapUtils

  def create(
      arguments: NodeFactoryArguments[
        ParticipantNodeConfig,
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

object ParticipantNodeBootstrapFactoryImpl extends ParticipantNodeBootstrapFactory {

  private def replicationService(
      loggerFactory: NamedLoggerFactory
  )(storage: Storage)(implicit ec: ExecutionContext): ServerServiceDefinition =
    ParticipantReplicationServiceGrpc.bindService(
      new GrpcParticipantReplicationService(storage, loggerFactory),
      ec,
    )

  private def allocatedIndexerLockIds(
      dbConfig: DbConfig,
      loggerFactory: NamedLoggerFactory,
  ): Either[String, IndexerLockIds] = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
    for {
      mainLockId <- DbLockId
        .allocate(dbConfig, DbLockCounters.INDEXER_MAIN, loggerFactory)
        .leftMap(err => s"Failed to allocate indexer main lock id: $err")
      workerLockId <- DbLockId
        .allocate(dbConfig, DbLockCounters.INDEXER_WORKER, loggerFactory)
        .leftMap(err => s"Failed to allocate indexer worker lock id: $err")
    } yield IndexerLockIds(mainLockId.id, workerLockId.id)
  }

  override def create(
      arguments: NodeFactoryArguments[
        ParticipantNodeConfig,
        ParticipantNodeParameters,
        ParticipantMetrics,
      ],
      testingTimeService: TestingTimeService,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      actorSystem: ActorSystem,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): Either[String, ParticipantNodeBootstrap] = {
    val replicaManager = new ParticipantReplicaManager(
      arguments.parameters.exitOnFatalFailures,
      arguments.parameters.processingTimeouts,
      arguments.loggerFactory,
      arguments.futureSupervisor,
    )
    val storageFactory = new StorageMultiFactory(
      arguments.config.storage,
      exitOnFatalFailures = arguments.parameters.exitOnFatalFailures,
      arguments.config.replication,
      () =>
        replicaManager
          .setActive(),
      () => replicaManager.setPassive(),
      DbLockCounters.PARTICIPANT_WRITE,
      DbLockCounters.PARTICIPANT_WRITERS,
      arguments.futureSupervisor,
      arguments.loggerFactory,
      Some(replicaManager.getSessionContext),
    )
    arguments
      .toCantonNodeBootstrapCommonArguments(
        storageFactory,
        Some(replicaManager),
      )
      .map { arguments =>
        val engine = createEngine(arguments)
        createNode(
          arguments,
          engine,
          createLedgerApiBootstrapUtils(
            arguments,
            engine,
            testingTimeService,
          ),
          replicaManager,
        )
      }

  }

  override protected def createLedgerApiBootstrapUtils(
      arguments: Arguments,
      engine: Engine,
      testingTimeService: TestingTimeService,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      actorSystem: ActorSystem,
  ): LedgerApiServerBootstrapUtils =
    new LedgerApiServerBootstrapUtils(
      engine,
      arguments.clock,
      testingTimeService,
      dbConfig => allocatedIndexerLockIds(dbConfig, arguments.loggerFactory).map(Some(_)),
      arguments.loggerFactory,
    )

  private def createNode(
      arguments: Arguments,
      engine: Engine,
      ledgerApiServerBootstrapUtils: LedgerApiServerBootstrapUtils,
      replicaManager: ParticipantReplicaManager,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      actorSystem: ActorSystem,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): ParticipantNodeBootstrap = new ParticipantNodeBootstrap(
    arguments,
    replicaManager,
    engine,
    createResourceService(arguments),
    replicationService(arguments.loggerFactory),
    ledgerApiServerBootstrapUtils = ledgerApiServerBootstrapUtils,
    // We signal the replica manager that the participant node has been initialized.
    // Before that signal, passive->active transitions are not performed because the participant node initialization
    // would race with the transition in terms of starting up the participant services
    setInitialized = replicaManager.setInitialized,
  ) {
    override def onClosed(): Unit = {
      replicaManager.close()
      super.onClosed()
    }
  }
}
