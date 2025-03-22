// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.integration.ConfigTransforms.generateUniqueH2DatabaseName
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.{
  MultiSynchronizer,
  SequencerSynchronizerGroups,
  SingleSynchronizer,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.{BlockSequencerConfig, SequencerConfig}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.{
  BaseReferenceSequencerDriverFactory,
  ReferenceSequencerDriver,
}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.{TempDirectory, TempFile}
import com.typesafe.config.{Config, ConfigFactory}
import monocle.macros.syntax.lens.*
import pureconfig.ConfigCursor

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

/** @param sequencerGroups
  *   If sequencerGroups is defined, all the sequencers of the same set will share the same storage
  *   (which means they are part of the same synchronizer). All sequencers in the config that are
  *   not in a defined group will be placed in the same default group. If it is not defined, all
  *   sequencers will share the same storage by default and one synchronizer only is assumed. If
  *   in-memory storage is defined, sequencers sharing storage is not supported (each one is a
  *   different synchronizer).
  */
abstract class UseReferenceBlockSequencerBase[
    StorageConfigT <: StorageConfig
](
    override protected val loggerFactory: NamedLoggerFactory,
    driverSingleWordName: String,
    driverDescription: String,
    sequencerGroups: SequencerSynchronizerGroups = SingleSynchronizer,
    postgres: Option[UsePostgres] = None,
)(implicit c: ClassTag[StorageConfigT])
    extends EnvironmentSetupPlugin {

  private implicit val pluginExecutionContext: ExecutionContext =
    Threading.newExecutionContext(
      loggerFactory.threadName + s"-$driverSingleWordName-sequencer-plugin-execution-context",
      noTracingLogger,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[canton] var pgPlugin: Option[UsePostgres] = postgres

  protected val driverFactory: BaseReferenceSequencerDriverFactory

  private final def dbNameForGroup(group: Int): String = s"${driverSingleWordName}_db_$group"

  protected val dbNames: NonEmpty[List[String]] = sequencerGroups match {
    case SingleSynchronizer => NonEmpty(List, dbNameForGroup(0))
    case MultiSynchronizer(_) =>
      NonEmpty(
        List,
        dbNameForGroup(0), // db 0 is the default one
        (1 to sequencerGroups.numberOfSynchronizers).map(i => dbNameForGroup(i)).toList *,
      )
  }

  private def driverConfigs(
      config: CantonConfig,
      storageConfigs: Map[InstanceName, StorageConfigT],
  ): Map[InstanceName, SequencerConfig] = {
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.forClass(loggerFactory, getClass)
    config.sequencers.keys.map { sequencerName =>
      sequencerName -> SequencerConfig.External(
        driverFactory.name,
        BlockSequencerConfig(),
        ConfigCursor(
          driverFactory
            .configWriter(confidential = false)
            .to(
              ReferenceSequencerDriver
                .Config(
                  storageConfigs.getOrElse(
                    sequencerName,
                    ErrorUtil.invalidState(s"Missing storage config for $sequencerName"),
                  )
                )
            ),
          List(),
        ),
      )
    }.toMap
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    c.runtimeClass match {
      case cl if cl == classOf[DbConfig.Postgres] =>
        pgPlugin =
          if (pgPlugin.nonEmpty) pgPlugin
          else {
            val postgresPlugin = new UsePostgres(loggerFactory)
            postgresPlugin.beforeTests()
            Some(postgresPlugin)
          }
        val postgresPlugin =
          pgPlugin.getOrElse(
            throw new IllegalStateException("Impossible code path: pgPlugin is set above")
          )
        Await.result(
          dbNames.forgetNE.parTraverse_(db => postgresPlugin.recreateDatabaseForNodeWithName(db)),
          config.parameters.timeouts.processing.io.duration,
        )
      case _ =>
    }

    // in H2 we need to make db name unique, but it has to be matching for all sequencers, so we cache it
    lazy val dbNamesH2: Map[String, String] = dbNames.forgetNE.map { dbName =>
      dbName -> generateUniqueH2DatabaseName(dbName)
    }.toMap

    def dbToStorageConfig(
        dbName: String,
        dbParametersConfig: DbParametersConfig,
        baseDbConfig: Config,
    ): StorageConfigT =
      c.runtimeClass match {
        case cl if cl == classOf[DbConfig.H2] =>
          val h2DbName = dbNamesH2.getOrElse(
            dbName,
            throw new IllegalStateException(
              s"Impossible code path: dbName $dbName not found in $dbNamesH2"
            ),
          )
          DbConfig
            .H2(
              DbBasicConfig("user", "pass", h2DbName, "", 0).toH2DbConfig
                .copy(parameters = dbParametersConfig)
                .config
            )
            .asInstanceOf[StorageConfigT]
        case cl if cl == classOf[DbConfig.Postgres] =>
          val postgresPlugin =
            pgPlugin.getOrElse(
              throw new IllegalStateException("Impossible code path: pgPlugin is set above")
            )
          postgresPlugin
            .generateDbConfig(dbName, dbParametersConfig, baseDbConfig)
            .asInstanceOf[StorageConfigT]
        case cl if cl == classOf[StorageConfig.Memory] =>
          StorageConfig.Memory(parameters = dbParametersConfig).asInstanceOf[StorageConfigT]
        case other =>
          // E.g. Nothing; we need to check and fail b/c the Scala compiler doesn't enforce
          //  passing the ClassTag-reified type parameter, if it's only used for a ClassTag implicit
          sys.error(
            s"The $driverDescription sequencer driver doesn't recognize storage type $other"
          )
      }

    val storageConfigMap: Map[InstanceName, StorageConfigT] = sequencerGroups match {
      case SingleSynchronizer =>
        config.sequencers.map { case (name, sequencerConfig) =>
          val dbParameters = sequencerConfig.storage.parameters
          (name, dbToStorageConfig(dbNames.head1, dbParameters, sequencerConfig.storage.config))
        }
      case MultiSynchronizer(groups) =>
        groups.zipWithIndex.flatMap { case (sequencers, i) =>
          val dbName = dbNameForGroup(i + 1)
          sequencers.map { name =>
            val (dbParameters, baseDbConfig) = config.sequencers
              .get(name)
              .map(s => (s.storage.parameters, s.storage.config))
              .getOrElse((DbParametersConfig(), ConfigFactory.empty()))

            (name, dbToStorageConfig(dbName, dbParameters, baseDbConfig))
          }
        }.toMap
    }

    val sequencersToConfig: Map[InstanceName, SequencerConfig] =
      driverConfigs(config, storageConfigMap)

    def mapSequencerConfigs(
        kv: (InstanceName, SequencerNodeConfig)
    ): (InstanceName, SequencerNodeConfig) = kv match {
      case (name, cfg) =>
        (
          name,
          cfg.focus(_.sequencer).replace(sequencersToConfig(name)),
        )
    }

    config
      .focus(_.sequencers)
      .modify(_.map(mapSequencerConfigs))
  }

  override def afterTests(): Unit = pgPlugin.foreach(_.afterTests())

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit =
    pgPlugin.foreach { postgresPlugin =>
      Await.result(
        postgresPlugin.dropDatabases(dbNames),
        config.parameters.timeouts.processing.io.duration,
      )
    }

  def recreateDatabases(): Future[Unit] =
    pgPlugin match {
      case Some(postgresPlugin) =>
        dbNames.forgetNE.parTraverse_(db =>
          postgresPlugin
            .recreateDatabaseForNodeWithName(db)
            .failOnShutdownToAbortException("recreateDatabases")
        )
      case _ =>
        logger.underlying.warn(
          s"`$functionFullName` was called on a test without UsePostgres plugin, which is not supported!"
        )
        Future.unit
    }

  def dumpDatabases(tempDirectory: TempDirectory, forceLocal: Boolean = false): Future[Unit] =
    pgPlugin match {
      case Some(postgresPlugin) =>
        val pgDumpRestore = PostgresDumpRestore(postgresPlugin, forceLocal)
        dbNames.forgetNE.parTraverse_(db =>
          pgDumpRestore.saveDump(db, dumpTempFile(tempDirectory, db))
        )
      case _ =>
        logger.underlying.warn(
          s"`$functionFullName` was called on a test without UsePostgres plugin, which is not supported!"
        )
        Future.unit
    }

  def restoreDatabases(tempDirectory: TempDirectory, forceLocal: Boolean = false): Future[Unit] =
    pgPlugin match {
      case Some(postgresPlugin) =>
        val pgDumpRestore = PostgresDumpRestore(postgresPlugin, forceLocal)
        dbNames.forgetNE.parTraverse_(db =>
          pgDumpRestore.restoreDump(db, dumpTempFile(tempDirectory, db).path)
        )
      case _ =>
        logger.underlying.warn(
          s"`$functionFullName` was called on a test without UsePostgres plugin, which is not supported!"
        )
        Future.unit
    }

  private def dumpTempFile(tempDirectory: TempDirectory, dbName: String): TempFile =
    tempDirectory.toTempFile(s"pg-dump-$dbName.tar")

}

object UseReferenceBlockSequencerBase {

  sealed trait SequencerSynchronizerGroups {
    def numberOfSynchronizers: Int
  }

  case object SingleSynchronizer extends SequencerSynchronizerGroups {
    override val numberOfSynchronizers: Int = 1
  }

  final case class MultiSynchronizer(sequencerGroups: Seq[Set[InstanceName]])
      extends SequencerSynchronizerGroups {
    override val numberOfSynchronizers: Int = sequencerGroups.size
  }

  object MultiSynchronizer {
    def tryCreate(sequencerGroups: Set[String]*): MultiSynchronizer =
      MultiSynchronizer(sequencerGroups.map(_.map(InstanceName.tryCreate)))
  }
}
