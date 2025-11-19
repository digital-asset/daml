// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.synchronizer.block.SequencerDriver
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.HASequencerExclusiveStorageBuilder.ExclusiveStorage
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.{BftSequencer, External}
import com.digitalasset.canton.synchronizer.sequencer.block.DriverBlockSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing.BftSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext

trait SequencerFactory extends FlagCloseable with HasCloseContext {

  def initialize(
      initialState: SequencerInitialState,
      sequencerId: SequencerId,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]

  def create(
      sequencerId: SequencerId,
      clock: Clock,
      synchronizerSyncCryptoApi: SynchronizerCryptoClient,
      futureSupervisor: FutureSupervisor,
      trafficConfig: SequencerTrafficConfig,
      runtimeReady: FutureUnlessShutdown[Unit],
      sequencerSnapshot: Option[SequencerSnapshot],
      authenticationServices: Option[AuthenticationServices],
  )(implicit
      traceContext: TraceContext,
      tracer: Tracer,
      actorMaterializer: Materializer,
  ): FutureUnlessShutdown[Sequencer]
}

object SequencerMetaFactory {

  def createFactory(
      protocolVersion: ProtocolVersion,
      health: Option[SequencerHealthConfig],
      clock: Clock,
      scheduler: ScheduledExecutorService,
      metrics: SequencerMetrics,
      storage: Storage,
      sequencerId: SequencerId,
      nodeParameters: SequencerNodeParameters,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(
      sequencerConfig: SequencerConfig,
      useTimeProofsToObserveEffectiveTime: Boolean,
  )(implicit executionContext: ExecutionContext): SequencerFactory =
    sequencerConfig match {
      case databaseConfig: SequencerConfig.Database =>
        // if we're configured for high availability switch to using a writer storage factory that will
        // dynamically determine a node instance index and ensure that all sensitive writes are performed
        // while holding an exclusive lock for that instance.
        val writerStorageFactory =
          if (databaseConfig.highAvailabilityEnabled)
            new HASequencerWriterStoreFactory(
              protocolVersion,
              databaseConfig.highAvailability.getOrElse(
                throw new IllegalStateException("HA config not set despite being enabled")
              ),
              nodeParameters.loggingConfig.queryCost,
              Some(scheduler),
              sequencerId,
              nodeParameters.cachingConfigs,
              nodeParameters.batchingConfig,
              metrics,
              nodeParameters.processingTimeouts,
              exitOnFatalFailures = nodeParameters.exitOnFatalFailures,
              futureSupervisor,
              loggerFactory,
            )
          else
            SequencerWriterStoreFactory.singleInstance

        // under high availability create a separate, exclusive DbStorageMulti to manage
        // configuration stored in the database and also for use by pruning.
        val exclusiveStorageBuilder =
          Option.when[
            Storage => Either[HASequencerExclusiveStorageBuilder.CreateError, ExclusiveStorage]
          ](
            databaseConfig.highAvailabilityEnabled
          )(
            new HASequencerExclusiveStorageBuilder(
              databaseConfig.highAvailability
                .getOrElse(
                  throw new IllegalStateException("HA config not set despite being enabled")
                )
                .exclusiveStorage,
              nodeParameters.loggingConfig.queryCost,
              scheduler,
              nodeParameters.processingTimeouts,
              exitOnFatalFailures = nodeParameters.exitOnFatalFailures,
              futureSupervisor,
              loggerFactory,
            ).create
          )

        val pruningSchedulerBuilder: (Storage, Sequencer) => DatabaseSequencerPruningScheduler = {
          case (storage, sequencer) =>
            new DatabaseSequencerPruningScheduler(
              clock,
              sequencer,
              storage,
              databaseConfig.pruning,
              nodeParameters.processingTimeouts,
              loggerFactory,
            )
        }

        new HADatabaseSequencerFactory(
          databaseConfig,
          storage,
          protocolVersion,
          writerStorageFactory,
          exclusiveStorageBuilder,
          pruningSchedulerBuilder,
          health,
          metrics,
          sequencerId,
          nodeParameters,
          loggerFactory,
        )

      case BftSequencer(blockSequencerConfig, config) =>
        new BftSequencerFactory(
          config,
          blockSequencerConfig,
          useTimeProofsToObserveEffectiveTime,
          health,
          storage,
          protocolVersion,
          sequencerId,
          nodeParameters,
          metrics,
          loggerFactory,
          blockSequencerConfig.testingInterceptor,
        )

      case External(sequencerType, blockSequencerConfig, rawConfig) =>
        DriverBlockSequencerFactory(
          sequencerType,
          SequencerDriver.DriverApiVersion,
          rawConfig,
          blockSequencerConfig,
          useTimeProofsToObserveEffectiveTime,
          health,
          storage,
          protocolVersion,
          sequencerId,
          nodeParameters,
          metrics,
          loggerFactory,
          blockSequencerConfig.testingInterceptor,
        )
    }
}
