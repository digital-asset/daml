// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.{CommunityDbConfig, CommunityStorageConfig, ProcessingTimeout}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.sequencer.reference.BaseReferenceSequencerDriverFactory.createDbStorageMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.reference.CommunityReferenceSequencerDriverFactory.setMigrationsPath
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{CommunityDbMigrations, CommunityStorageFactory, Storage}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*
import pureconfig.{ConfigReader, ConfigWriter}

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

final class CommunityReferenceSequencerDriverFactory extends BaseReferenceSequencerDriverFactory {

  override def name: String = "community-reference"

  override type StorageConfigType = CommunityStorageConfig

  @nowarn("cat=unused") // Work-around for IntelliJ Idea wrongly reporting unused implicits
  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  override def configParser: ConfigReader[ConfigType] = {
    import pureconfig.generic.semiauto.*

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

  @nowarn("cat=unused") // Work-around for IntelliJ Idea wrongly reporting unused implicits
  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  override def configWriter(confidential: Boolean): ConfigWriter[ConfigType] = {
    import pureconfig.generic.semiauto.*

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

  override def createStorage(
      config: ReferenceSequencerDriver.Config[CommunityStorageConfig],
      clock: Clock,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
      metricsContext: MetricsContext,
  ): Storage = {
    val storageConfig = setMigrationsPath(config.storage)
    storageConfig match {
      case dbConfig: CommunityDbConfig =>
        new CommunityDbMigrations(dbConfig, false, loggerFactory)
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
    new CommunityStorageFactory(storageConfig)
      .tryCreate(
        connectionPoolForParticipant = false,
        config.logQueryCost,
        clock,
        scheduler = None,
        metrics = createDbStorageMetrics(),
        processingTimeout,
        loggerFactory,
      )
  }
}

object CommunityReferenceSequencerDriverFactory {

  private def setMigrationsPath(config: CommunityStorageConfig): CommunityStorageConfig =
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
