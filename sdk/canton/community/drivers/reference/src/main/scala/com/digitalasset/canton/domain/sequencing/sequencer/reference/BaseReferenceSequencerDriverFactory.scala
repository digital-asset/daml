// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.config.{BatchAggregatorConfig, BatchingConfig, ConnectionAllocation, DbParametersConfig, ProcessingTimeout, QueryCostMonitoringConfig, StorageConfig}
import com.digitalasset.canton.domain.block.BlockFormat.DefaultFirstBlockHeight
import com.digitalasset.canton.domain.block.{SequencerDriver, SequencerDriverFactory}
import com.digitalasset.canton.domain.sequencing.sequencer.reference.BaseReferenceSequencerDriverFactory.createClock
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{DbStorageHistograms, DbStorageMetrics}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.{Clock, TimeProvider, TimeProviderClock}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.Materializer
import pureconfig.{ConfigReader, ConfigWriter}

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

@nowarn("cat=unused") // Work-around for IntelliJ Idea wrongly reporting unused implicits
@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
private[reference] abstract class BaseReferenceSequencerDriverFactory
    extends SequencerDriverFactory {

  type StorageConfigType <: StorageConfig

  override final type ConfigType = ReferenceSequencerDriver.Config[StorageConfigType]

  protected def createStorage(
      config: ConfigType,
      clock: Clock,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
      metricsContext: MetricsContext,
  ): Storage

  override final def version: Int = 1

  override final def usesTimeProvider: Boolean = true

  import pureconfig.generic.semiauto.*

  protected lazy implicit final val batchAggregatorConfigReader
      : ConfigReader[BatchAggregatorConfig] = {
    implicit val batching: ConfigReader[BatchAggregatorConfig.Batching] =
      deriveReader[BatchAggregatorConfig.Batching]
    implicit val noBatching: ConfigReader[BatchAggregatorConfig.NoBatching.type] =
      deriveReader[BatchAggregatorConfig.NoBatching.type]

    deriveReader[BatchAggregatorConfig]
  }
  protected lazy implicit final val batchingReader: ConfigReader[BatchingConfig] =
    deriveReader[BatchingConfig]

  protected lazy implicit final val connectionAllocationReader: ConfigReader[ConnectionAllocation] =
    deriveReader[ConnectionAllocation]

  protected lazy implicit final val dbParamsReader: ConfigReader[DbParametersConfig] =
    deriveReader[DbParametersConfig]

  protected implicit final val queryCostMonitoringConfigReader
      : ConfigReader[QueryCostMonitoringConfig] =
    deriveReader[QueryCostMonitoringConfig]

  protected lazy implicit final val batchAggregatorConfigWriter
      : ConfigWriter[BatchAggregatorConfig] = {
    implicit val batching: ConfigWriter[BatchAggregatorConfig.Batching] =
      deriveWriter[BatchAggregatorConfig.Batching]
    implicit val noBatching: ConfigWriter[BatchAggregatorConfig.NoBatching.type] =
      deriveWriter[BatchAggregatorConfig.NoBatching.type]

    deriveWriter[BatchAggregatorConfig]
  }

  protected lazy implicit final val batchingWriter: ConfigWriter[BatchingConfig] =
    deriveWriter[BatchingConfig]

  protected lazy implicit final val connectionAllocationWriter: ConfigWriter[ConnectionAllocation] =
    deriveWriter[ConnectionAllocation]

  protected lazy implicit final val dbParamsWriter: ConfigWriter[DbParametersConfig] =
    deriveWriter[DbParametersConfig]

  protected lazy implicit final val queryCostMonitoringConfigWriter
      : ConfigWriter[QueryCostMonitoringConfig] =
    deriveWriter[QueryCostMonitoringConfig]

  override def create(
      config: ConfigType,
      nonStandardConfig: Boolean,
      timeProvider: TimeProvider,
      firstBlockHeight: Option[Long],
      domainId: String,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): SequencerDriver = {
    val processingTimeout = ProcessingTimeout()
    val closeable = flagCloseable(processingTimeout, loggerFactory)
    val storage =
      createStorage(
        config,
        createClock(timeProvider, loggerFactory),
        processingTimeout,
        loggerFactory,
      )(
        executionContext,
        TraceContext.empty,
        new CloseContext(closeable),
        MetricsContext.Empty,
      )
    val store =
      ReferenceBlockOrderingStore(storage, processingTimeout, loggerFactory)
    new ReferenceSequencerDriver(
      store,
      config,
      timeProvider,
      firstBlockHeight.getOrElse(DefaultFirstBlockHeight),
      storage,
      closeable,
      loggerFactory,
      processingTimeout,
    )
  }

  private def flagCloseable(
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): FlagCloseable =
    FlagCloseable(loggerFactory.getTracedLogger(getClass), processingTimeout)
}

private[reference] object BaseReferenceSequencerDriverFactory {

  final def createDbStorageMetrics()(implicit
      metricsContext: MetricsContext
  ): DbStorageMetrics =
    new DbStorageMetrics(
      new DbStorageHistograms(MetricName("none"))(new HistogramInventory),
      NoOpMetricsFactory,
    )

  final def createClock(timeProvider: TimeProvider, loggerFactory: NamedLoggerFactory) =
    new TimeProviderClock(timeProvider, loggerFactory)
}
