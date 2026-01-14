// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.SqlParser
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy.withoutNetworkTimeout
import com.digitalasset.canton.platform.store.backend.postgresql.{
  PostgresDataSourceConfig,
  PostgresDataSourceStorageBackend,
}
import org.scalatest.Inside
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.SQLException
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

final class StorageBackendSpecPostgres
    extends AnyFlatSpec
    with StorageBackendProviderPostgres
    with StorageBackendSuite
    with Inside {

  behavior of "StorageBackend (Postgres)"

  it should "find the Postgres version" in {
    val version = executeSql(PostgresDataSourceStorageBackend(loggerFactory).getPostgresVersion)

    inside(version) { case Some(versionNumbers) =>
      // Minimum Postgres version used in tests
      versionNumbers._1 should be >= 9
      versionNumbers._2 should be >= 0
    }
  }

  it should "correctly parse a Postgres version" in {
    val backend = PostgresDataSourceStorageBackend(loggerFactory)
    backend.parsePostgresVersion("1.2") shouldBe Some((1, 2))
    backend.parsePostgresVersion("1.2.3") shouldBe Some((1, 2))
    backend.parsePostgresVersion("1.2.3-alpha.4.5") shouldBe Some((1, 2))
    backend.parsePostgresVersion("10.11") shouldBe Some((10, 11))
  }

  it should "fail the compatibility check for Postgres versions lower than minimum" in {
    val version = executeSql(PostgresDataSourceStorageBackend(loggerFactory).getPostgresVersion)
    val currentlyUsedMajorVersion = inside(version) { case Some((majorVersion, _)) =>
      majorVersion
    }
    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = currentlyUsedMajorVersion + 1,
        loggerFactory = loggerFactory,
      )

    loggerFactory.assertThrowsAndLogs[PostgresDataSourceStorageBackend.UnsupportedPostgresVersion](
      within = executeSql(
        backend.checkCompatibility
      ),
      assertions = _.errorMessage should include(
        "Deprecated Postgres version."
      ),
    )
  }

  it should "throw an exception if network timeout has been violated" in {
    import anorm.SqlStringInterpolation

    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = 14,
        loggerFactory = loggerFactory,
      )

    val dataSource = backend.createDataSource(
      DataSourceStorageBackend
        .DataSourceConfig(
          jdbcUrl = jdbcUrl,
          postgresConfig = PostgresDataSourceConfig(
            networkTimeout = Some(config.NonNegativeFiniteDuration.ofSeconds(1))
          ),
        ),
      loggerFactory,
    )

    val connection = dataSource.getConnection
    val startTime = System.nanoTime()
    val thrown = intercept[SQLException] {
      // sleep for 5 seconds to simulate a long-running query
      SQL"SELECT pg_sleep(5);".execute()(connection)
    }
    thrown.getMessage should include("An I/O error occurred while sending to the backend.")
    thrown.getCause.getMessage should include("Read timed out")
    val endTime = System.nanoTime()
    val timedOutAfterMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)

    // the network timeout should have occurred after approximately 1 second
    timedOutAfterMillis should be < 1500L
    timedOutAfterMillis should be >= 1000L

    connection.close()

    // when the network timeout is disabled, the long-running query should complete successfully
    val connection2 = dataSource.getConnection
    withoutNetworkTimeout { connection =>
      SQL"SELECT pg_sleep(5);".execute()(connection)
    }(connection2, logger)

    // and when re-enabling the network timeout, it should again throw after the specified time
    val thrown2 = intercept[SQLException] {
      SQL"SELECT pg_sleep(5);".execute()(connection2)
    }
    thrown2.getMessage should include("An I/O error occurred while sending to the backend.")
    thrown2.getCause.getMessage should include("Read timed out")

    connection2.close()
  }

  it should "wait for a long running query when clientConnectionCheckInterval is not set" in {
    import anorm.SqlStringInterpolation

    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = 14,
        loggerFactory = loggerFactory,
      )

    val dataSource = backend.createDataSource(
      DataSourceStorageBackend
        .DataSourceConfig(
          jdbcUrl = jdbcUrl,
          postgresConfig = PostgresDataSourceConfig(
            clientConnectionCheckInterval = None,
            networkTimeout = None,
          ),
        ),
      loggerFactory,
    )

    val connection1 = dataSource.getConnection
    val connection2 = dataSource.getConnection

    // acquire advisory lock
    val acquired1 = SQL"SELECT pg_try_advisory_lock(123456);".as(
      SqlParser.bool("pg_try_advisory_lock").single
    )(connection1)
    acquired1 shouldBe true

    // blocking acquire advisory lock in another connection
    val acquired2 = Future(
      SQL"SELECT pg_advisory_lock(123456);".execute()(connection2)
    )(parallelExecutionContext)

    // long-running query
    val longRunning = Future(
      SQL"SELECT pg_sleep(5);".execute()(connection1)
    )(parallelExecutionContext)

    Threading.sleep(500) // let some time for db to start the long-running query
    acquired2.isCompleted shouldBe false
    // close the connection while the long-running query is still running simulating a lost client connection
    // since there is no clientConnectionCheckInterval the server will not interrupt the long-running query and will
    // only release the lock after it
    val thrown = intercept[TestFailedException] {
      connection1.close()
      longRunning.futureValue
    }
    thrown.getCause shouldBe a[org.postgresql.util.PSQLException]

    val startTime = System.nanoTime()
    acquired2.futureValue
    val endTime = System.nanoTime()
    val getLockDurationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)
    // should take almost 5 seconds to acquire the lock after client is disconnected since there is no clientConnectionCheckInterval
    // and the long-running query continues to run until completion and then releases the lock
    getLockDurationMillis should be > 4000L

    connection2.close()
  }

  it should "abort a long running query when clientConnectionCheckInterval is set" in {
    import anorm.SqlStringInterpolation

    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = 14,
        loggerFactory = loggerFactory,
      )

    val dataSource = backend.createDataSource(
      DataSourceStorageBackend
        .DataSourceConfig(
          jdbcUrl = jdbcUrl,
          postgresConfig = PostgresDataSourceConfig(
            clientConnectionCheckInterval = Some(config.NonNegativeFiniteDuration.ofSeconds(2)),
            networkTimeout = None,
          ),
        ),
      loggerFactory,
    )

    val connection1 = dataSource.getConnection
    val connection2 = dataSource.getConnection
    // acquire advisory lock
    val acquired1 = SQL"SELECT pg_try_advisory_lock(123456);"
      .as(SqlParser.bool("pg_try_advisory_lock").single)(connection1)
    acquired1 shouldBe true

    // blocking acquire advisory lock in another connection
    val acquired2 = Future(
      SQL"SELECT pg_advisory_lock(123456);".execute()(connection2)
    )(parallelExecutionContext)

    // long-running query
    val longRunning = Future(
      SQL"SELECT pg_sleep(5);".execute()(connection1)
    )(parallelExecutionContext)

    Threading.sleep(500) // let some time for db to start the long-running query
    acquired2.isCompleted shouldBe false
    // close the connection while the long-running query is still running simulating a lost client connection
    // when clientConnectionCheckInterval kicks in the server should interrupt the long-running query and release the lock
    val thrown = intercept[TestFailedException] {
      connection1.close()
      longRunning.futureValue
    }
    thrown.getCause shouldBe a[org.postgresql.util.PSQLException]

    val startTime = System.nanoTime()
    acquired2.futureValue
    val endTime = System.nanoTime()
    val getLockDurationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)

    // should take at most 2 seconds for the server to check that the client is disconnected, abort the long-running query
    // and release the lock so that the second connection can acquire it
    getLockDurationMillis should be < 2000L

    connection2.close()
  }

  it should "throw an exception when a long-running query violates network timeout" in {
    import anorm.SqlStringInterpolation

    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = 14,
        loggerFactory = loggerFactory,
      )

    val postgresConfig = PostgresDataSourceConfig(
      clientConnectionCheckInterval = Some(config.NonNegativeFiniteDuration.ofSeconds(1)),
      networkTimeout = Some(config.NonNegativeFiniteDuration.ofSeconds(2)),
    )

    val dataSource = backend.createDataSource(
      DataSourceStorageBackend
        .DataSourceConfig(
          jdbcUrl = jdbcUrl,
          postgresConfig = postgresConfig,
        ),
      loggerFactory,
    )

    val connection1 = dataSource.getConnection
    val connection2 = dataSource.getConnection()
    // setting a high timeout for the second connection
    connection2.setNetworkTimeout(
      parallelExecutionContext,
      60000, // 60 seconds
    )
    // acquire advisory lock
    val acquired1 = SQL"SELECT pg_try_advisory_lock(123456);"
      .as(SqlParser.bool("pg_try_advisory_lock").single)(connection1)
    acquired1 shouldBe true

    // blocking acquire advisory lock in another connection
    val acquired2 = Future(
      SQL"SELECT pg_advisory_lock(123456);".execute()(connection2)
    )(parallelExecutionContext)

    // long-running query
    val longRunning = Future(
      SQL"SELECT pg_sleep(5);".execute()(connection1)
    )(parallelExecutionContext)

    Threading.sleep(500) // let some time for db to start the long-running query
    acquired2.isCompleted shouldBe false
    val startTime = System.nanoTime()

    // long-running query should throw after network timeout, however the server continues to execute it
    val thrown = intercept[TestFailedException] {
      longRunning.futureValue
    }
    thrown.getCause shouldBe a[org.postgresql.util.PSQLException]
    thrown.getCause.getCause.getMessage should include("Read timed out")

    val networkTimeoutTime = System.nanoTime()
    val timeUntilNetworkTimeout = TimeUnit.NANOSECONDS.toMillis(networkTimeoutTime - startTime)
    // should take at most 2 seconds for the client to check that the network timeout has been violated
    timeUntilNetworkTimeout should be < 2000L

    acquired2.futureValue
    val endTime = System.nanoTime()

    // should take at most 1 more second for the server to check that the client is disconnected, abort the long-running query
    // and release the lock so that the second connection can acquire it
    val timeToAbortAfterTimeout = TimeUnit.NANOSECONDS.toMillis(endTime - networkTimeoutTime)
    timeToAbortAfterTimeout should be < 1000L

    val getLockDurationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)
    getLockDurationMillis should be < (postgresConfig.networkTimeout.value.duration.toMillis + postgresConfig.clientConnectionCheckInterval.value.duration.toMillis)

    connection1.close()
    connection2.close()
  }
}
