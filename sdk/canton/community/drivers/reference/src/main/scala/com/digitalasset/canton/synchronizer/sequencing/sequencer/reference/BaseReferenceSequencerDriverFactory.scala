// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.synchronizer.sequencing.sequencer.reference

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.config.{ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{DbStorageHistograms, DbStorageMetrics}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.synchronizer.block.BlockFormat.DefaultFirstBlockHeight
import com.digitalasset.canton.synchronizer.block.{SequencerDriver, SequencerDriverFactory}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.BaseReferenceSequencerDriverFactory.createClock
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore
import com.digitalasset.canton.time.{Clock, TimeProvider, TimeProviderClock}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext

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

  override def create(
      config: ConfigType,
      nonStandardConfig: Boolean,
      timeProvider: TimeProvider,
      firstBlockHeight: Option[Long],
      synchronizerId: String,
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
