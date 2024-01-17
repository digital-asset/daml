// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference

import com.daml.metrics.api.MetricName
import com.digitalasset.canton.config.{
  BatchAggregatorConfig,
  BatchingConfig,
  CommunityDbConfig,
  CommunityStorageConfig,
  ConnectionAllocation,
  DbParametersConfig,
  ProcessingTimeout,
  StorageConfig,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.{BlockOrderer, BlockOrdererFactory}
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.metrics.MetricHandle.NoOpMetricsFactory
import com.digitalasset.canton.resource.{CommunityDbMigrations, CommunityStorageFactory, Storage}
import com.digitalasset.canton.time.{Clock, TimeProvider}
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*
import org.apache.pekko.stream.Materializer
import pureconfig.{ConfigReader, ConfigWriter}

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

class CommunityReferenceBlockOrdererFactory extends BlockOrdererFactory {

  import CommunityReferenceBlockOrdererFactory.*

  override def version: Int = 1

  override type ConfigType = ReferenceBlockOrderer.Config[CommunityStorageConfig]

  @nowarn("cat=unused")
  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  override def configParser: ConfigReader[ConfigType] = {
    import pureconfig.generic.semiauto.*

    lazy implicit val batchAggregatorConfigReader: ConfigReader[BatchAggregatorConfig] = {
      implicit val batching = deriveReader[BatchAggregatorConfig.Batching]
      implicit val noBatching = deriveReader[BatchAggregatorConfig.NoBatching.type]

      deriveReader[BatchAggregatorConfig]
    }
    lazy implicit val batchingReader: ConfigReader[BatchingConfig] =
      deriveReader[BatchingConfig]
    lazy implicit val connectionAllocationReader: ConfigReader[ConnectionAllocation] =
      deriveReader[ConnectionAllocation]
    lazy implicit val dbParamsReader: ConfigReader[DbParametersConfig] =
      deriveReader[DbParametersConfig]
    implicit val communityMemoryStorageConfigReader: ConfigReader[CommunityStorageConfig.Memory] =
      deriveReader[CommunityStorageConfig.Memory]
    implicit val communityH2StorageConfigReader: ConfigReader[CommunityDbConfig.H2] =
      deriveReader[CommunityDbConfig.H2]
    implicit val communityPostgresStorageConfigReader: ConfigReader[CommunityDbConfig.Postgres] =
      deriveReader[CommunityDbConfig.Postgres]
    implicit val communityStorageConfigReader: ConfigReader[CommunityStorageConfig] =
      deriveReader[CommunityStorageConfig]
    deriveReader[ConfigType]
  }

  @nowarn("cat=unused")
  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  override def configWriter(confidential: Boolean): ConfigWriter[ConfigType] = {
    import pureconfig.generic.semiauto.*
    lazy implicit val batchAggregatorConfigWriter: ConfigWriter[BatchAggregatorConfig] = {
      @nowarn("cat=unused") implicit val batching: ConfigWriter[BatchAggregatorConfig.Batching] =
        deriveWriter[BatchAggregatorConfig.Batching]
      @nowarn("cat=unused") implicit val noBatching
          : ConfigWriter[BatchAggregatorConfig.NoBatching.type] =
        deriveWriter[BatchAggregatorConfig.NoBatching.type]

      deriveWriter[BatchAggregatorConfig]
    }
    lazy implicit val batchingWriter: ConfigWriter[BatchingConfig] =
      deriveWriter[BatchingConfig]
    lazy implicit val connectionAllocationWriter: ConfigWriter[ConnectionAllocation] =
      deriveWriter[ConnectionAllocation]
    lazy implicit val dbParamsWriter: ConfigWriter[DbParametersConfig] =
      deriveWriter[DbParametersConfig]
    implicit val communityMemoryStorageConfigWriter: ConfigWriter[CommunityStorageConfig.Memory] =
      deriveWriter[CommunityStorageConfig.Memory]
    implicit val communityH2StorageConfigWriter: ConfigWriter[CommunityDbConfig.H2] =
      deriveWriter[CommunityDbConfig.H2]
    implicit val communityPostgresStorageConfigWriter: ConfigWriter[CommunityDbConfig.Postgres] =
      deriveWriter[CommunityDbConfig.Postgres]
    implicit val communityStorageConfigWriter: ConfigWriter[CommunityStorageConfig] =
      deriveWriter[CommunityStorageConfig]
    deriveWriter[ConfigType]
  }

  override def create(
      config: ConfigType,
      domainTopologyManagerId: String,
      timeProvider: TimeProvider,
      lFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): BlockOrderer = {
    val processingTimeout = ProcessingTimeout()
    val closeable = flagCloseable(processingTimeout, lFactory)
    val storage = createStorage(config, timeProvider, closeable, processingTimeout, lFactory)
    val store = ReferenceBlockOrderingStore(storage, processingTimeout, lFactory)
    new ReferenceBlockOrderer(
      store,
      config.pollInterval,
      timeProvider,
      storage,
      closeable,
      lFactory,
      processingTimeout,
    )
  }
}

object CommunityReferenceBlockOrdererFactory {

  private[sequencer] def createStorage(
      config: CommunityReferenceBlockOrdererFactory#ConfigType,
      timeProvider: TimeProvider,
      closeable: FlagCloseable,
      processingTimeout: ProcessingTimeout,
      lFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): Storage = {
    val clock = new Clock {
      override def now: CantonTimestamp =
        CantonTimestamp.assertFromLong(timeProvider.nowInMicrosecondsSinceEpoch)

      override protected def addToQueue(queue: Queued): Unit = ()

      override protected def loggerFactory: NamedLoggerFactory = lFactory

      override def close(): Unit = ()
    }
    implicit val traceContext: TraceContext = TraceContext.empty
    implicit val closeContext: CloseContext = new CloseContext(closeable)
    val storageConfig = setMigrationsPath(config.storage)
    storageConfig match {
      case communityStorageConfig: CommunityStorageConfig =>
        communityStorageConfig match {
          case dbConfig: CommunityDbConfig =>
            new CommunityDbMigrations(dbConfig, false, lFactory)
              .migrateDatabase()
              .value
              .map {
                case Left(error) => sys.error(s"Error with migration $error")
                case Right(_) => ()
              }
              .discard
          case _ =>
            // Not a DB storage (currently, only memory) => no need for migrations.
            ()
        }
        new CommunityStorageFactory(communityStorageConfig)
          .tryCreate(
            connectionPoolForParticipant = false,
            logQueryCost = None,
            clock = clock,
            scheduler = None,
            metrics = new DbStorageMetrics(MetricName("none"), NoOpMetricsFactory),
            timeouts = processingTimeout,
            loggerFactory = lFactory,
          )
      case _ =>
        sys.error("The community reference sequencer only supports community storage")
    }
  }

  private[sequencer] def flagCloseable(
      processingTimeout: ProcessingTimeout,
      lFactory: NamedLoggerFactory,
  ): FlagCloseable = FlagCloseable(lFactory.getTracedLogger(this.getClass), processingTimeout)

  private def setMigrationsPath(config: StorageConfig): StorageConfig =
    config match {
      case h2: CommunityDbConfig.H2 =>
        h2.focus(_.parameters.migrationsPaths)
          .replace(Seq("classpath:db/migration/canton/h2/dev/reference/"))
      case pg: CommunityDbConfig.Postgres =>
        pg.focus(_.parameters.migrationsPaths)
          .replace(Seq("classpath:db/migration/canton/postgres/dev/reference/"))
      case x => x
    }
}
