// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CommunityDbConfig.{H2, Postgres}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.DbStorage.RetryConfig
import com.digitalasset.canton.resource.{
  CommunityDbMigrationsFactory,
  DbMigrationsFactory,
  DbStorage,
  DbStorageSingle,
}
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.typesafe.config.{Config, ConfigFactory}
import org.postgresql.util.PSQLException
import org.scalatest.Assertions.fail
import org.testcontainers.containers.PostgreSQLContainer

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Provide a storage backend for tests.
  */
trait DbStorageSetup extends FlagCloseable with HasCloseContext with NamedLogging {
  type C <: DbConfig

  protected implicit def executionContext: ExecutionContext
  private implicit val traceContext: TraceContext = TraceContext.empty

  def basicConfig: DbBasicConfig

  def mkDbConfig: DbBasicConfig => C

  /** Used when creating the storage */
  def retryConfig: DbStorage.RetryConfig

  // Generous timeout to give the db some time to startup.
  protected lazy val dbStartupTimeout: NonNegativeDuration = NonNegativeDuration.ofMinutes(10)

  protected def prepareDatabase(): Unit

  protected def migrationsFactory: DbMigrationsFactory

  def migrationMode: MigrationMode

  protected def destroyDatabase(): Unit

  override protected val timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

  // Needs to run first, because test containers need a running db to determine basicConfig.
  prepareDatabase()

  final lazy val config: C = mkDbConfig(basicConfig)

  final lazy val storage: DbStorage = mkStorage(config)

  /** Will run all setup code of this instance and throw the setup code fails.
    * If this method is not called up front, the setup code will run (and possibly fail) when a field is accessed.
    */
  // We could make `storage` a `val` so that no separate method is needed.
  // In that case, the initialization code of this trait would invoke code from subtypes through abstract methods.
  // As a result, it could read uninitialized fields from subtypes, which can lead to annoying bugs.
  def initialized(): this.type = {
    storage.discard[DbStorage]
    this
  }

  protected final def mkStorage(cfg: DbConfig): DbStorage =
    DbStorageSingle.tryCreate(
      cfg,
      new SimClock(CantonTimestamp.Epoch, loggerFactory),
      None,
      connectionPoolForParticipant =
        false, // can always be false, because this storage is only used for initialization and unit tests
      None,
      CommonMockMetrics.dbStorage,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      retryConfig,
    )

  final def migrateDb(): Unit = {
    val migrationResult =
      migrationsFactory.create(config, migrationMode == MigrationMode.DevVersion).migrateDatabase()
    // throw so the first part of the test that attempts to use storage will fail with an exception
    migrationResult
      .valueOr(err => fail(s"Failed to migrate database: $err"))
      .onShutdown(fail("DB migration interrupted due to shutdown"))
  }

  override final def onClosed(): Unit = {
    storage.close()
    destroyDatabase()
  }

  /** Lookup environment variable and return. Throw [[java.lang.RuntimeException]] if missing. */
  protected def env(name: String): String =
    sys.env.getOrElse(name, sys.error(s"Environment variable not set [$name]"))
}

sealed trait MigrationMode extends Product with Serializable
object MigrationMode {
  case object Standard extends MigrationMode
  case object DevVersion extends MigrationMode
}

/** Postgres database storage setup
  */
abstract class PostgresDbStorageSetup(
    override protected val loggerFactory: NamedLoggerFactory
)(implicit override val executionContext: ExecutionContext)
    extends DbStorageSetup
    with NamedLogging
    with NoTracing {

  override type C = Postgres

  override lazy val retryConfig: RetryConfig = RetryConfig.failFast

  override protected lazy val migrationsFactory: DbMigrationsFactory =
    new CommunityDbMigrationsFactory(loggerFactory)
}

/** Assumes Postgres is available on a already running and that connections details are
  * provided through environment variables.
  * In CI this is done by running a Postgres docker container alongside the build.
  */
class PostgresCISetup(
    override val migrationMode: MigrationMode,
    override val mkDbConfig: DbBasicConfig => Postgres,
    loggerFactory: NamedLoggerFactory,
)(implicit
    override val executionContext: ExecutionContext
) extends PostgresDbStorageSetup(loggerFactory) {

  /** name of existing database we can use (either for testing or for setting up new databases) */
  private lazy val envDb = env("POSTGRES_DB")

  /** name of db to use for the tests (avoiding flyway migration conflicts) */
  private lazy val useDb = envDb + (if (migrationMode == MigrationMode.DevVersion) "_dev" else "")

  private lazy val useHost = sys.env.getOrElse("POSTGRES_HOST", "localhost")

  override lazy val basicConfig: DbBasicConfig = DbBasicConfig(
    env("POSTGRES_USER"),
    env("POSTGRES_PASSWORD"),
    useDb,
    useHost,
    5432,
  )

  @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
  override protected def prepareDatabase(): Unit = if (migrationMode == MigrationMode.DevVersion) {
    val envDbConfig =
      CommunityDbConfig.Postgres(basicConfig.copy(dbName = envDb).toPostgresConfig)
    val envDbStorage = mkStorage(envDbConfig)

    def transformQueryResult[T](v: T): PartialFunction[Throwable, T] = {
      // Due to a race condition, it may happen that between checking if the database exists and creating it,
      // it has already been created and a duplicate database error is thrown.
      // We can safely ignore this error.
      case ex: PSQLException
          // 23505 means "unique_violation"
          // 42P04 means "duplicate_database"
          // either of these 2 errors could happen when trying to create a database that already exists
          // (source: https://www.postgresql.org/docs/current/errcodes-appendix.html)
          if ex.getSQLState == "23505" || ex.getSQLState == "42P04" =>
        v

      // Two concurrent calls try to do some operation
      case ex: PSQLException if ex.getMessage.contains("ERROR: tuple concurrently updated") =>
        v
    }

    try {
      import envDbStorage.api.*
      val genF = envDbStorage
        .query(sql"SELECT 1 FROM pg_database WHERE datname = $useDb".as[Int], functionFullName)
        .recover(transformQueryResult(Vector(1)))
        .flatMap { res =>
          if (res.isEmpty) {
            logger.debug(s"Creating database ${useDb} using connection to ${envDb}")
            envDbStorage
              .update_(sqlu"CREATE DATABASE #${useDb}", functionFullName)
              .recover(transformQueryResult(()))
              .flatMap(_ =>
                Future {
                  val useDbConfig =
                    CommunityDbConfig.Postgres(basicConfig.copy(dbName = useDb).toPostgresConfig)
                  val useDbStorage = mkStorage(useDbConfig)
                  try {
                    import useDbStorage.api.*
                    val adaptSchemaF = useDbStorage.update_(
                      sqlu"""GRANT ALL ON SCHEMA public TO "#${env("POSTGRES_USER")}"""",
                      functionFullName,
                    )
                    DefaultProcessingTimeouts.default.await_(
                      s"granting rights on public schema for database $useDb"
                    )(adaptSchemaF)
                  } finally useDbStorage.close()
                }.recover(transformQueryResult(()))
              )
          } else Future.unit
        }
      DefaultProcessingTimeouts.default.await_(s"creating database $useDb")(genF)
    } finally envDbStorage.close()
  }

  override protected def destroyDatabase(): Unit = ()
}

/** Use [TestContainers]() to create a Postgres docker container instance to run against.
  * Used for running tests locally.
  */
class PostgresTestContainerSetup(
    override val migrationMode: MigrationMode,
    override val mkDbConfig: DbBasicConfig => Postgres,
    loggerFactory: NamedLoggerFactory,
)(implicit
    override val executionContext: ExecutionContext
) extends PostgresDbStorageSetup(loggerFactory)
    with NamedLogging {

  private lazy val postgresContainer = new PostgreSQLContainer(s"${PostgreSQLContainer.IMAGE}:12")

  override protected def prepareDatabase(): Unit = {
    // up the connection limit to deal with everyone using connection pools in tests that can run concurrently.
    // we also have a matching max connections limit set in the CircleCI postgres executor (`.circle/config.yml`)
    val command = postgresContainer.getCommandParts.toSeq :+ "-c" :+ "max_connections=500"
    postgresContainer.setCommandParts(command.toArray)
    noTracingLogger.debug(s"Starting postgres container with $command")

    val startF = Future { postgresContainer.start() }
    dbStartupTimeout.await_("startup of postgres container")(startF)
  }

  override lazy val basicConfig: DbBasicConfig = DbBasicConfig(
    postgresContainer.getUsername,
    postgresContainer.getPassword,
    postgresContainer.getDatabaseName,
    postgresContainer.getHost,
    postgresContainer.getFirstMappedPort,
  )

  def getContainerID: String = postgresContainer.getContainerId

  override protected def destroyDatabase(): Unit = postgresContainer.close()
}

class H2DbStorageSetup(
    override val migrationMode: MigrationMode,
    override val mkDbConfig: DbBasicConfig => H2,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    override val executionContext: ExecutionContext
) extends DbStorageSetup
    with NamedLogging {

  override type C = H2

  override lazy val basicConfig: DbBasicConfig = DbBasicConfig("", "", loggerFactory.name, "", 0)

  override lazy val retryConfig: RetryConfig = RetryConfig.failFast

  override lazy val migrationsFactory: DbMigrationsFactory =
    new CommunityDbMigrationsFactory(loggerFactory)

  override protected def prepareDatabase(): Unit = ()

  override protected def destroyDatabase(): Unit = ()
}

object DbStorageSetup {

  /** If we are running in CI on a docker executor opt to use a separate docker setup for testing postgres
    * If we are running in CI on a `machine` use a local [org.testcontainers.containers.PostgreSQLContainer.PostgreSQLContainer]
    * If we are running locally use a local [org.testcontainers.containers.PostgreSQLContainer.PostgreSQLContainer]
    */
  def postgres(
      loggerFactory: NamedLoggerFactory,
      migrationMode: MigrationMode = MigrationMode.Standard,
      mkDbConfig: DbBasicConfig => Postgres = _.toPostgresDbConfig,
  )(implicit ec: ExecutionContext): PostgresDbStorageSetup = {

    val isCI = sys.env.contains("CI")
    val isMachine = sys.env.contains("MACHINE")
    val forceTestContainer = sys.env.contains("DB_FORCE_TEST_CONTAINER")

    if (!forceTestContainer && (isCI && !isMachine))
      new PostgresCISetup(migrationMode, mkDbConfig, loggerFactory).initialized()
    else new PostgresTestContainerSetup(migrationMode, mkDbConfig, loggerFactory).initialized()
  }

  def h2(
      loggerFactory: NamedLoggerFactory,
      migrationMode: MigrationMode = MigrationMode.Standard,
      mkDbConfig: DbBasicConfig => H2 = _.toH2DbConfig,
  )(implicit ec: ExecutionContext): H2DbStorageSetup =
    new H2DbStorageSetup(migrationMode, mkDbConfig, loggerFactory).initialized()

  final case class DbBasicConfig(
      username: String,
      password: String,
      dbName: String,
      host: String,
      port: Int,
      connectionPoolEnabled: Boolean =
        false, // disable by default, as the config is mainly used for setup / migration
  ) {

    private def configOfMap(map: Map[String, Object]): Config = {
      val connectionPoolConfig = ConfigFactory.parseString(
        if (connectionPoolEnabled) "connectionPool = HikariCP" else "connectionPool = disabled"
      )
      val config = ConfigFactory.parseMap(map.asJava)
      connectionPoolConfig.withFallback(config)
    }

    def toPostgresConfig: Config = configOfMap(
      Map[String, Object](
        "dataSourceClass" -> "org.postgresql.ds.PGSimpleDataSource",
        "properties.serverName" -> host,
        "properties.databaseName" -> dbName,
        "properties.portNumber" -> (port: Integer),
        "properties.user" -> username,
        "properties.password" -> password,
      )
    )

    def toPostgresDbConfig: Postgres = Postgres(
      toPostgresConfig,
      parameters = DbParametersConfig(unsafeCleanOnValidationError = true),
    )

    def toH2Config: Config = configOfMap(
      Map(
        "driver" -> "org.h2.Driver",
        "url" -> DbConfig.h2Url(dbName),
        "user" -> username,
        "password" -> password,
      )
    )

    def toH2DbConfig: H2 = H2(toH2Config)

    def toOracleConfig: Config = configOfMap(
      Map(
        "driver" -> "oracle.jdbc.OracleDriver",
        "url" -> DbConfig.oracleUrl(host, port, dbName),
        "user" -> username,
        "password" -> password,
      )
    )
  }
}
