// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.reference

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.synchronizer.block.BlockFormat.DefaultFirstBlockHeight
import com.digitalasset.canton.synchronizer.block.{SequencerDriver, SequencerDriverFactory}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.BaseReferenceSequencerDriverFactory.createClock
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore
import com.digitalasset.canton.time.{Clock, TimeProvider, TimeProviderClock}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.Materializer
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

private[reference] abstract class BaseReferenceSequencerDriverFactory
    extends SequencerDriverFactory {

  override final type ConfigType = ReferenceSequencerDriver.Config[StorageConfig]

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

  override def configParser: ConfigReader[ConfigType] = {
    import pureconfig.generic.semiauto.*
    import com.digitalasset.canton.config.BaseCantonConfig.Readers.*
    implicit val communityMemoryStorageConfigReader: ConfigReader[StorageConfig.Memory] =
      deriveReader[StorageConfig.Memory]
    implicit val communityH2StorageConfigReader: ConfigReader[DbConfig.H2] =
      deriveReader[DbConfig.H2]
    implicit val communityPostgresStorageConfigReader: ConfigReader[DbConfig.Postgres] =
      deriveReader[DbConfig.Postgres]
    implicit val communityStorageConfigReader: ConfigReader[StorageConfig] =
      deriveReader[StorageConfig]

    deriveReader[ConfigType]
  }

  override def configWriter(confidential: Boolean): ConfigWriter[ConfigType] = {
    import pureconfig.generic.semiauto.*
    import com.digitalasset.canton.config.BaseCantonConfig.Writers.*

    implicit val enterpriseMemoryStorageConfigWriter: ConfigWriter[StorageConfig.Memory] =
      deriveWriter[StorageConfig.Memory]
    implicit val enterpriseH2StorageConfigWriter: ConfigWriter[DbConfig.H2] =
      deriveWriter[DbConfig.H2]
    implicit val enterprisePostgresStorageConfigWriter: ConfigWriter[DbConfig.Postgres] =
      deriveWriter[DbConfig.Postgres]
    implicit val StorageConfigWriter: ConfigWriter[StorageConfig] =
      deriveWriter[StorageConfig]

    deriveWriter[ConfigType]
  }

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

  final def createClock(timeProvider: TimeProvider, loggerFactory: NamedLoggerFactory) =
    new TimeProviderClock(timeProvider, loggerFactory)
}
