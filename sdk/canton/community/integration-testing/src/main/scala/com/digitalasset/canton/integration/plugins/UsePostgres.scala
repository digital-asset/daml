// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  ExecutorServiceExtensions,
  Threading,
}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.{DbStorage, DbStorageSingle}
import com.digitalasset.canton.store.db.{DbStorageSetup, PostgresDbStorageSetup}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import monocle.macros.syntax.lens.*

import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.NonFatal
import scala.util.{Random, Success}

/** Plugin to provide a postgres backend to a
  * [[com.digitalasset.canton.integration.BaseIntegrationTest]] instance.
  *
  * By default, tests will run against a dockerized db. So you need to nothing except for installing
  * docker.
  *
  * To get higher performance (in particular on Mac OSX), you may want to run tests against a
  * non-containerized postgres db. To do so, export the following environment variables:
  * {{{
  * CI=1
  * POSTGRES_USER=test # changing the username is not recommended, see data continuity tests documentation in contributing.md
  * POSTGRES_PASSWORD=supersafe
  * POSTGRES_DB=postgres
  * }}}
  *
  * Please note that you need to create in this case two databases: $POSTGRES_DB AND
  * "${POSTGRES_DB}_dev"
  *
  * On the database, you need to create the user as follows:
  *
  * {{{
  * $ psql postgres
  * postgres=# create user test with password 'supersafe';
  * CREATE ROLE
  * postgres=# ALTER USER test CREATEDB CREATEROLE;
  * ALTER ROLE
  * postgres=# ALTER USER test CREATEROLE;
  * ALTER ROLE
  * postgres=# exit
  * }}}
  *
  * @param customDbNames
  *   optionally defines a mapping from identifier to db name (String => String) and a suffix to add
  *   to the db name
  */
class UsePostgres(
    protected val loggerFactory: NamedLoggerFactory,
    customDbNames: Option[(String => String, String)] = None,
    customMaxConnectionsByNode: Option[String => Option[PositiveInt]] = None,
    forceTestContainer: Boolean = false,
) extends EnvironmentSetupPlugin
    with FlagCloseable
    with HasCloseContext
    with NoTracing {
  import org.scalatest.TryValues.*

  private lazy val dbSetupExecutorService: ExecutionContextIdlenessExecutorService =
    Threading.newExecutionContext(
      loggerFactory.threadName + "-db-execution-context",
      noTracingLogger,
    )

  private[plugins] def dbSetupExecutionContext: ExecutionContext = dbSetupExecutorService

  // Make sure that each environment and its database names are unique by generating a random prefix
  // Short prefix due to postgres database name limit of 63 bytes
  private lazy val dbPrefix = "d" + Random.alphanumeric.map(_.toLower).take(7).mkString

  lazy val applicationName: String = loggerFactory.name

  def generateDbName(nodeName: String): String = customDbNames match {
    case Some((dbNames, suffix)) => dbNames(nodeName) + suffix
    case None => s"${dbPrefix}_$nodeName"
  }

  // we have to keep the storage instance alive for the duration of the test as this could be
  // managing an external docker container and other resources
  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private[integration] var dbSetup: PostgresDbStorageSetup = _

  override def beforeTests(): Unit =
    dbSetup = DbStorageSetup.postgres(loggerFactory, forceTestContainer = forceTestContainer)(
      dbSetupExecutionContext
    )

  // can throw NullPointerException if called before `beforeTests`
  def getDbUsernameOrThrow: String = dbSetup.basicConfig.username

  override def onClosed(): Unit =
    LifeCycle.close(
      dbSetup,
      ExecutorServiceExtensions(dbSetupExecutorService)(logger, DefaultProcessingTimeouts.testing),
    )(logger)

  override def afterTests(): Unit =
    close()

  def generateDbConfig(
      name: String,
      baseParameters: DbParametersConfig,
      baseDbConfig: Config,
  ): StorageConfig = {

    val dbName = generateDbName(name)

    val basicConfigWithDbName =
      dbSetup.basicConfig.copy(
        username = s"$dbName-user",
        password = "user",
        dbName = dbName,
        connectionPoolEnabled = true,
      )

    val configWithApplicationName =
      ConfigFactory
        .parseString(s"properties.applicationName = $applicationName")
        .withFallback(basicConfigWithDbName.toPostgresConfig)
        .withFallback(baseDbConfig)

    val dbConfigCanton = DbConfig.Postgres(
      configWithApplicationName,
      baseParameters,
    )

    customMaxConnectionsByNode match {
      case Some(maxConnectionsByNode) =>
        dbConfigCanton.focus(_.parameters.maxConnections).replace(maxConnectionsByNode(name))
      case None => dbConfigCanton
    }
  }

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    implicit val ec: ExecutionContext = dbSetupExecutionContext
    val transformedConfig = {
      val storageChange = ConfigTransforms.modifyAllStorageConfigs((_, name, storage) =>
        generateDbConfig(name, storage.parameters, storage.config)
      )(config)

      if (sys.env.contains("NON_STANDARD_POSTGRES")) {
        // This environment variable is set by the conformance tests for non-standard Postgres versions.
        // So the non-standard-config flag is set to disable the Postgres version checks.
        logger.info(s"Disabling Postgres version checks for conformance test")
        storageChange.focus(_.parameters.nonStandardConfig).replace(true)
      } else storageChange
    }

    Await.result(
      nodeNamesOfConfig(transformedConfig)
        .map(generateDbName)
        .distinct
        .parTraverse_(dbName => recreateDatabase(dbName)),
      config.parameters.timeouts.processing.io.duration,
    )
    transformedConfig
  }

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit = {
    val nodes = nodeNamesOfConfig(config)
    val drops = dropDatabases(nodes)
    Await.result(drops, config.parameters.timeouts.processing.io.duration)
  }

  private def logOpenQueries(storage: DbStorage): FutureUnlessShutdown[Unit] = {
    implicit val ec = dbSetupExecutionContext
    import storage.api.*

    storage
      .query(
        sql"select datname, query, state from pg_stat_activity".as[(String, String, String)],
        functionFullName,
      )
      .map { queries =>
        logger.info(s"Running queries: ${queries.mkString("\n")}")
      }
  }

  def dropDatabases(nodes: Seq[String]): FutureUnlessShutdown[Unit] = {
    implicit val ec = dbSetupExecutionContext
    val dbNames = nodes.map(generateDbName).distinct
    val storage = dbSetup.storage
    import storage.api.*
    storage
      .update_(
        DBIO.seq(
          dbNames.flatMap(db =>
            List(
              sqlu""" drop database if exists "#$db"""",
              sqlu""" drop user if exists "#$db-user"""",
            )
          )*
        ),
        functionFullName,
      )
      .recover { case NonFatal(e) =>
        logger.warn(s"Dropping database failed", e)
        logOpenQueries(storage).discard
        UnlessShutdown.Outcome(())
      }
  }

  def recreateDatabase(node: InstanceReference): FutureUnlessShutdown[Unit] =
    recreateDatabaseForNodeWithName(node.name)

  def recreateDatabaseForNodeWithName(node: String): FutureUnlessShutdown[Unit] =
    recreateDatabase(generateDbName(node))

  def recreateDatabase(dbName: String): FutureUnlessShutdown[Unit] = {
    val storage = dbSetup.storage
    import storage.api.*
    implicit val ec = dbSetupExecutionContext

    for {
      _ <- storage.update_(
        DBIO.seq(
          sqlu"""drop database if exists "#$dbName" """,
          sqlu"""drop user if exists "#$dbName-user" """,
          sqlu"""create database "#$dbName" """,
          sqlu"""create user "#$dbName-user" WITH PASSWORD 'user'""",
          sqlu"""GRANT ALL PRIVILEGES ON DATABASE "#$dbName" TO "#$dbName-user"""",
        ),
        operationName = "Recreate databases",
      )
      storageForSettingSchemaRights = DbStorageSingle.tryCreate(
        dbSetup.config.copy(config =
          dbSetup.config.config
            .withValue("properties.databaseName", ConfigValueFactory.fromAnyRef(dbName))
        ),
        new SimClock(CantonTimestamp.Epoch, loggerFactory),
        None,
        connectionPoolForParticipant =
          false, // can always be false, because this storage is only used for initialization and unit tests
        None,
        CommonMockMetrics.dbStorage,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
        dbSetup.retryConfig,
      )(dbSetupExecutionContext, TraceContext.empty, dbSetup.closeContext)
      grantRightsResult <- storageForSettingSchemaRights
        .update_(
          DBIO.seq(
            sqlu"""GRANT ALL ON SCHEMA public TO "#$dbName-user""""
          ),
          operationName = "Recreate databases part 2: granting rights on public schema",
        )
        .transform(t =>
          Success(UnlessShutdown.Outcome(t))
        ) // wrapping the result for finally closing the storageForSettingSchemaRights anyway
    } yield {
      storageForSettingSchemaRights.close()
      grantRightsResult.success.value // unwrapping the result
    }
  }

  private def nodeNamesOfConfig(transformedConfig: CantonConfig): List[String] =
    (transformedConfig.participants.keys ++ transformedConfig.mediators.keys ++ transformedConfig.sequencers.keys).toList
      .map(_.unwrap)

  override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
}
