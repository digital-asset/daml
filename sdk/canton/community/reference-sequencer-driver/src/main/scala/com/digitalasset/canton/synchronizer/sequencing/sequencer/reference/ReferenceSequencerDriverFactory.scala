// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.reference

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{Storage, StorageSingleSetup}
import com.digitalasset.canton.synchronizer.block.BlockFormat.DefaultFirstBlockHeight
import com.digitalasset.canton.synchronizer.block.{SequencerDriver, SequencerDriverFactory}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore
import com.digitalasset.canton.time.{Clock, TimeProvider, TimeProviderClock}
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*
import org.apache.pekko.stream.Materializer
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

class ReferenceSequencerDriverFactory extends SequencerDriverFactory {

  override final type ConfigType = ReferenceSequencerDriver.Config[StorageConfig]

  override final def version: Int = 1

  override final def usesTimeProvider: Boolean = true

  override def name: String = "reference"

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

  private def createClock(timeProvider: TimeProvider, loggerFactory: NamedLoggerFactory) =
    new TimeProviderClock(timeProvider, loggerFactory)

  override def create(
      config: ConfigType,
      nonStandardConfig: Boolean,
      timeProvider: TimeProvider,
      firstBlockHeight: Option[Long],
      synchronizerId: String,
      sequencerId: String,
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
      sequencerId,
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

  protected def createStorage(
      config: ReferenceSequencerDriver.Config[StorageConfig],
      clock: Clock,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
      metricsContext: MetricsContext,
  ): Storage =
    StorageSingleSetup.tryCreateAndMigrateStorage(
      config.storage,
      config.logQueryCost,
      clock,
      processingTimeout,
      loggerFactory,
      setMigrationsPath,
    )

  def setMigrationsPath(config: StorageConfig): StorageConfig =
    config match {
      case h2: DbConfig.H2 =>
        h2.focus(_.parameters.migrationsPaths)
          .replace(Seq("classpath:db/migration/canton/h2/dev/reference/"))
      case pg: DbConfig.Postgres =>
        pg.focus(_.parameters.migrationsPaths)
          .replace(Seq("classpath:db/migration/canton/postgres/dev/reference/"))
      case x => x
    }
}
