// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.block.data.SequencerBlockStore
import com.digitalasset.canton.domain.block.{BlockSequencerStateManager, SequencerDriverFactory}
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.DatabaseSequencerConfig.TestingInterceptor
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerHealthConfig
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficPurchasedStore
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, SequencerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import pureconfig.ConfigCursor

import java.util.ServiceLoader
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class DriverBlockSequencerFactory[C](
    sequencerDriverFactory: SequencerDriverFactory { type ConfigType = C },
    config: C,
    health: Option[SequencerHealthConfig],
    storage: Storage,
    protocolVersion: ProtocolVersion,
    sequencerId: SequencerId,
    nodeParameters: CantonNodeParameters,
    metrics: SequencerMetrics,
    override val loggerFactory: NamedLoggerFactory,
    testingInterceptor: Option[TestingInterceptor],
)(implicit ec: ExecutionContext)
    extends BlockSequencerFactory(
      health: Option[SequencerHealthConfig],
      storage,
      protocolVersion,
      sequencerId,
      nodeParameters,
      loggerFactory,
      testingInterceptor,
      metrics,
    ) {

  override protected final lazy val name: String = sequencerDriverFactory.name

  override protected final lazy val orderingTimeFixMode: OrderingTimeFixMode =
    OrderingTimeFixMode.MakeStrictlyIncreasing

  override protected final def createBlockSequencer(
      name: String,
      domainId: DomainId,
      cryptoApi: DomainSyncCryptoClient,
      stateManager: BlockSequencerStateManager,
      store: SequencerBlockStore,
      balanceStore: TrafficPurchasedStore,
      storage: Storage,
      futureSupervisor: FutureSupervisor,
      health: Option[SequencerHealthConfig],
      clock: Clock,
      driverClock: Clock,
      protocolVersion: ProtocolVersion,
      rateLimitManager: SequencerRateLimitManager,
      orderingTimeFixMode: OrderingTimeFixMode,
      initialBlockHeight: Option[Long],
      domainLoggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      materializer: Materializer,
      tracer: Tracer,
  ): BlockSequencer =
    new BlockSequencer(
      new DriverBlockOrderer(
        sequencerDriverFactory.create(
          config,
          nodeParameters.nonStandardConfig,
          driverClock,
          initialBlockHeight,
          domainId.toString,
          domainLoggerFactory,
        ),
        protocolVersion,
      ),
      name,
      domainId,
      cryptoApi,
      sequencerId,
      stateManager,
      store,
      balanceStore,
      storage,
      futureSupervisor,
      health,
      clock,
      protocolVersion,
      rateLimitManager,
      orderingTimeFixMode,
      nodeParameters.processingTimeouts,
      nodeParameters.loggingConfig.eventDetails,
      nodeParameters.loggingConfig.api.printer,
      metrics,
      domainLoggerFactory,
      unifiedSequencer = nodeParameters.useUnifiedSequencer,
    )
}

object DriverBlockSequencerFactory extends LazyLogging {

  def apply[C](
      driverName: String,
      driverVersion: Int,
      rawConfig: ConfigCursor,
      health: Option[SequencerHealthConfig],
      storage: Storage,
      protocolVersion: ProtocolVersion,
      sequencerId: SequencerId,
      nodeParameters: CantonNodeParameters,
      metrics: SequencerMetrics,
      loggerFactory: NamedLoggerFactory,
      testingInterceptor: Option[TestingInterceptor],
  )(implicit ec: ExecutionContext): DriverBlockSequencerFactory[C] = {
    val driverFactory: SequencerDriverFactory { type ConfigType = C } = getSequencerDriverFactory(
      driverName,
      driverVersion,
    )
    val config: C = driverFactory.configParser.from(rawConfig) match {
      case Right(config) => config
      case Left(error) =>
        sys.error(
          s"Failed to parse sequencer driver config of type $driverName version $driverVersion. Error: $error"
        )
    }
    new DriverBlockSequencerFactory[C](
      driverFactory,
      config,
      health,
      storage,
      protocolVersion,
      sequencerId,
      nodeParameters,
      metrics,
      loggerFactory,
      testingInterceptor,
    )
  }

  // for use with pre-parsed config
  def getFactory[C](
      driverName: String,
      driverVersion: Int,
      config: C,
      health: Option[SequencerHealthConfig],
      storage: Storage,
      protocolVersion: ProtocolVersion,
      sequencerId: SequencerId,
      nodeParameters: CantonNodeParameters,
      metrics: SequencerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): DriverBlockSequencerFactory[C] =
    new DriverBlockSequencerFactory[C](
      getSequencerDriverFactory(driverName, driverVersion),
      config,
      health,
      storage,
      protocolVersion,
      sequencerId,
      nodeParameters,
      metrics,
      loggerFactory,
      None,
    )

  // get just the sequencer driver factory
  def getSequencerDriverFactory[C](
      driverName: String,
      driverVersion: Int,
  ): SequencerDriverFactory { type ConfigType = C } = {
    val (matching, all) =
      try {
        ServiceLoader
          .load(classOf[SequencerDriverFactory])
          .iterator()
          .asScala
          .partition(f => f.name == driverName && f.version == driverVersion)
      } catch {
        // will occur if method called for filtering does not exist
        case _: AbstractMethodError =>
          logger.warn("Incompatible sequencer driver class found, aborting load")
          (Iterator(), Iterator())
      }

    matching
      .collectFirst { case factory: (SequencerDriverFactory { type ConfigType = C }) @unchecked =>
        factory
      }
      .getOrElse {
        val drivers = all.map(f => "'" + f.name + " v" + f.version + "'").mkString(", ")
        val lookupMessage =
          s"Sequencer implementation '$driverName', API version $driverVersion not found (out of $drivers)"

        if (all.exists(_.name.contains(driverName))) {
          sys.error(
            s"Sequencer driver version mismatch, looks like '$driverName' exists, but with a different version number. " +
              "Please ensure that you install same major and minor version for canton and canton drivers.\n" +
              lookupMessage
          )
        } else {
          sys.error(
            s"Sequencer driver missing, looks like '$driverName' does not exist, " +
              "please install canton-drivers of same major and minor version as your canton.\n" +
              lookupMessage
          )
        }

      }
  }
}
