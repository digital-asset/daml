// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.resources.ResourceContext
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.logging.SuppressionRule.{FullSuppression, LoggerNameContains}
import com.digitalasset.canton.logging.{NamedLogging, SuppressingLogger}
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.platform.indexer.ha.TestConnection
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import java.io.PrintWriter
import java.sql.Connection
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import javax.naming.OperationNotSupportedException
import javax.sql.DataSource
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class HikariJdbcConnectionProviderSpec
    extends AsyncFlatSpec
    with Matchers
    with NamedLogging
    with HasExecutionContext
    with Eventually {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(scaled(Span(5, Seconds)))
  val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)
  private implicit val tc: TraceContext = TraceContext.empty

  behavior of "HikariJdbcConnectionProvider"

  // in case of a depleted Hikari pool getting the connection can take connectionTimeout
  it should "not wait for releasing for a long-running health check" in {
    val connectionGate = new AtomicBoolean(true)
    val dataSourceFixture = new DataSource {
      private def notSupported = throw new OperationNotSupportedException
      override def getConnection: Connection =
        if (connectionGate.get()) new TestConnection {
          override def isClosed: Boolean = {
            // connection is signalled as closed after we gate connections
            if (connectionGate.get()) super.isClosed
            else true
          }

          // connection is signalled as invalid after we gate connections
          override def isValid(i: Int): Boolean = connectionGate.get()

          // the rest of the overrides here are needed to successfully mock a connection to Hikari
          override def isReadOnly: Boolean = false

          override def getAutoCommit: Boolean = true

          override def setAutoCommit(b: Boolean): Unit = ()

          override def getTransactionIsolation: Int = Connection.TRANSACTION_READ_COMMITTED

          override def getNetworkTimeout: Int = 5000

          override def setNetworkTimeout(executor: Executor, i: Int): Unit = ()

          override def commit(): Unit = ()

          override def rollback(): Unit = ()

          override def clearWarnings(): Unit = ()
        }
        else throw new Exception("no connection available at the moment")

      override def getConnection(username: String, password: String): Connection = notSupported

      override def getLogWriter: PrintWriter = notSupported

      override def setLogWriter(out: PrintWriter): Unit = notSupported

      override def setLoginTimeout(seconds: Int): Unit = ()

      override def getLoginTimeout: Int = 0

      override def unwrap[T](iface: Class[T]): T = notSupported

      override def isWrapperFor(iface: Class[_]): Boolean = notSupported

      override def getParentLogger: Logger = notSupported
    }
    val connectionProviderOwner =
      for {
        hikariDataSource <- HikariDataSourceOwner(
          dataSource = dataSourceFixture,
          serverRole = ServerRole.Testing(this.getClass),
          minimumIdle = 10,
          maxPoolSize = 10,
          connectionTimeout = FiniteDuration(10, "seconds"),
          metrics = None,
        )
        _ <- DataSourceConnectionProvider.owner(
          dataSource = hikariDataSource,
          logMarker = "test",
          loggerFactory = loggerFactory,
        )
      } yield hikariDataSource

    implicit val resourceContext = ResourceContext(implicitly)

    val suppressionRules = FullSuppression &&
      LoggerNameContains("HealthCheckTask")
    loggerFactory.suppress(suppressionRules) {
      connectionProviderOwner
        .use { hikariDataSource =>
          logger.info("HikariJdbcConnectionProvider initialized")
          def getConnection = Future(hikariDataSource.getConnection.close())
          for {
            _ <- Future.sequence(1.to(20).map(_ => getConnection))
          } yield {
            logger.info(
              "HikariJdbcConnectionProvider is functional: fetched and returned 20 connections"
            )
            val start = System.currentTimeMillis()
            connectionGate.set(false);
            Threading.sleep(
              500
            ) // so that the Hikari isValid bypass default duration of 500 millis pass (otherwise getting connection from the pool is not necessary checked)
            val failingConnection = getConnection
            Threading.sleep(500) // so that health check already hangs (this is polled every second)
            failingConnection.isCompleted shouldBe false // failing connection will hang for 10 seconds
            (start, failingConnection)
          }
        }
        .flatMap { case (start, failingConnection) =>
          val released = System.currentTimeMillis()
          released - start should be > 1000L
          released - start should be < 1500L // because we are not waiting for the healthcheck to be finished
          failingConnection.isCompleted shouldBe false // failing connection will hang for 10 seconds
          failingConnection.failed.map(_ => start)
        }
        .map { start =>
          val failedConnectionFinished = System.currentTimeMillis()
          failedConnectionFinished - start should be > 10000L
          eventually {
            val logEntries = loggerFactory.fetchRecordedLogEntries
            logEntries.size shouldBe 1
            logEntries(0).debugMessage should include(
              "Hikari connection health check failed after health checking stopped with"
            )
          }
        }
    }
  }

}
