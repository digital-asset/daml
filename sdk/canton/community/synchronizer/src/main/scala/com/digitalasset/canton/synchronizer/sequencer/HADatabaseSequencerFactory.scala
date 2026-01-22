// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.SequencingTimeBound
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.resource.StorageFactory.StorageCreationException
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.HASequencerExclusiveStorageBuilder.ExclusiveStorage
import com.digitalasset.canton.synchronizer.sequencer.HASequencerExclusiveStorageNotifier.FailoverNotification
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

class HADatabaseSequencerFactory(
    config: SequencerConfig.Database,
    storage: Storage,
    protocolVersion: ProtocolVersion,
    writerStorageFactory: SequencerWriterStoreFactory,
    exclusiveStorageBuilder: Option[
      Storage => Either[HASequencerExclusiveStorageBuilder.CreateError, ExclusiveStorage]
    ],
    pruningSchedulerBuilderPassedIn: (Storage, Sequencer) => PruningScheduler,
    health: Option[SequencerHealthConfig],
    metrics: SequencerMetrics,
    sequencerId: SequencerId,
    nodeParameters: SequencerNodeParameters,
    val loggerFactory: NamedLoggerFactory,
)(implicit ex: ExecutionContext)
    extends DatabaseSequencerFactory(
      config,
      storage,
      nodeParameters.cachingConfigs,
      nodeParameters.batchingConfig,
      nodeParameters.processingTimeouts,
      protocolVersion,
      sequencerId,
      blockSequencerMode = false,
      metrics = metrics,
    )
    with NamedLogging {

  override def create(
      sequencerId: SequencerId,
      clock: Clock,
      synchronizerSyncCryptoApi: SynchronizerCryptoClient,
      futureSupervisor: FutureSupervisor,
      trafficConfig: SequencerTrafficConfig,
      sequencingTimeLowerBoundExclusive: SequencingTimeBound,
      runtimeReady: FutureUnlessShutdown[Unit],
      sequencerSnapshot: Option[SequencerSnapshot],
      authenticationServices: Option[AuthenticationServices],
  )(implicit
      traceContext: TraceContext,
      tracer: Tracer,
      actorMaterializer: Materializer,
  ): FutureUnlessShutdown[Sequencer] = {

    val totalNodeCount =
      if (config.highAvailabilityEnabled)
        config.highAvailability
          .getOrElse(ErrorUtil.invalidState("High availability must specify a total node count"))
          .totalNodeCount
      else
        TotalNodeCountValues.SingleSequencerTotalNodeCount

    // if high availability is configured we will assume that more than one sequencer is being used
    // and we will switch to using the polling based event signalling as we won't have visibility
    // of all writes locally.
    val eventSignaller =
      if (config.highAvailabilityEnabled)
        new PollingEventSignaller(
          config.reader.pollingInterval
            .getOrElse(SequencerReaderConfig.defaultPollingInterval)
            .toInternal,
          loggerFactory,
        )
      else
        new LocalSequencerStateEventSignaller(
          nodeParameters.processingTimeouts,
          loggerFactory,
        )

    // Throw if storage misconfigured or if couldn't build DbStorageMulti for exclusive storage.
    val exclusiveStorage = exclusiveStorageBuilder.map(
      _(storage).valueOr(err => throw new StorageCreationException(err.message))
    )
    logger.info(s"Creating database sequencer")

    val sequencer = new DatabaseSequencer(
      writerStorageFactory,
      sequencerStore,
      config,
      None,
      totalNodeCount,
      eventSignaller,
      config.highAvailability.map(_.keepAliveInterval.toInternal),
      config.highAvailability.map(_.toOnlineSequencerCheckConfig),
      nodeParameters.processingTimeouts,
      storage,
      exclusiveStorage.map(_.storage),
      health,
      clock,
      sequencerId,
      synchronizerSyncCryptoApi,
      metrics,
      loggerFactory,
      blockSequencerMode = false,
      sequencingTimeLowerBoundExclusive = sequencingTimeLowerBoundExclusive,
      rateLimitManagerO = None,
    ) {
      override def pruningSchedulerBuilder: Option[Storage => PruningScheduler] = {
        (storage: Storage) =>
          pruningSchedulerBuilderPassedIn(storage, this).tap(pruningScheduler =>
            // With exclusive storage, register failover notifications to configure pruning schedulers
            // to adapted pruning schedule.
            exclusiveStorage.foreach {
              _.failoverNotifier.setNotifications(
                failoverNotification(pruningScheduler, onActive = true),
                failoverNotification(pruningScheduler, onActive = false),
              )
            }
          )
      }.some
    }

    FutureUnlessShutdown.pure(
      config.testingInterceptor.map(_(clock)(sequencer)(ex)).getOrElse(sequencer)
    )
  }

  private def failoverNotification(
      pruningScheduler: PruningScheduler,
      onActive: Boolean,
  ): FailoverNotification = () =>
    withNewTraceContext("db_failover_notification") { implicit traceContext =>
      logger.info(
        s"Database Sequencer exclusive storage becoming ${if (onActive) "active" else "passive"}"
      )
      FutureUnlessShutdown.outcomeF(
        pruningScheduler
          .restart()
          .map(_ =>
            logger.info(
              "Restarted database sequencer pruning scheduler to failover-adapted schedule"
            )
          )
      )
    }

}
