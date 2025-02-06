// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.DbConfig.{H2, Postgres}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.*

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Base test for writing a database backed storage test.
  * To ensure idempotency and safety under retries of the store each write operation is executed twice.
  * Each database should provide a DbTest implementation that can then be mixed into a storage test to provide the actual backend.
  * See DbCryptoVaultStoreTest for example usage.
  */
trait DbTest
    extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with FlagCloseable
    with HasCloseContext // not used here, but required by most tests. So extending it for convenience.
    with HasExecutionContext
    with NamedLogging {
  this: Suite =>

  /** Flag to define the migration mode for the schemas */
  def migrationMode: MigrationMode =
    // TODO(i15561): Revert back to `== ProtocolVersion.dev` once v30 is a stable Daml 3 protocol version
    if (BaseTest.testedProtocolVersion >= ProtocolVersion.v33) MigrationMode.DevVersion
    else MigrationMode.Standard

  protected def mkDbConfig(basicConfig: DbBasicConfig): DbConfig

  protected def createSetup(): DbStorageSetup

  /** Hook for cleaning database before running next test. */
  protected def cleanDb(storage: DbStorage)(implicit tc: TraceContext): FutureUnlessShutdown[?]

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private var setup: DbStorageSetup = _

  /** Stores the db storage implementation. Will throw if accessed before the test has started */
  protected lazy val storage: DbStorageIdempotency = {
    val s = Option(setup).map(_.storage).getOrElse(sys.error("Test has not started"))
    new DbStorageIdempotency(s, timeouts, loggerFactory)
  }

  override def beforeAll(): Unit = TraceContext.withNewTraceContext { implicit tc =>
    // Non-standard order. Setup needs to be created first, because super can be MyDbTest and therefore super.beforeAll
    // may already access setup.
    try {
      setup = createSetup().initialized()
      setup.migrateDb()
      super.beforeAll()
    } catch {
      case NonFatal(e) =>
        // Logging the error, as an exception in this method will abort the test suite with no log output.
        logger.error("beforeAll failed", e)
        throw e
    }
  }

  override def afterAll(): Unit = TraceContext.withNewTraceContext { implicit tc =>
    try {
      // Non-standard order.
      // First delete test data.
      cleanup()

      // Free resources of MyDbTest, if there are any.
      close()
      super.afterAll() // This will also close the executionContext, unfortunately.

      // Release database. Fortunately, this seems not to require an executionContext.
      storage.close()
      setup.close()
    } catch {
      case NonFatal(e) =>
        // Logging the error, as an exception in this method will abort the test suite with no log output.
        logger.error("afterAll failed", e)
        throw e
    }
  }

  override def beforeEach(): Unit = TraceContext.withNewTraceContext { implicit tc =>
    try {
      cleanup()
      super.beforeEach()
    } catch {
      case NonFatal(e) =>
        // Logging the error, as an exception in this method will abort the test suite with no log output.
        logger.error("beforeEach failed", e)
        throw e
    }
  }

  private def cleanup()(implicit tc: TraceContext): Unit =
    // Use the underlying storage for clean-up operations, so we don't run clean-ups twice
    Await.result(cleanDb(storage.underlying), 10.seconds)
}

/** Run db test against h2 */
trait H2Test extends DbTest { this: Suite =>

  override protected def mkDbConfig(basicConfig: DbBasicConfig): H2 = basicConfig.toH2DbConfig

  override protected def createSetup(): DbStorageSetup =
    DbStorageSetup.h2(loggerFactory, migrationMode, mkDbConfig)
}

/** Run db test for running against postgres */
trait PostgresTest extends DbTest { this: Suite =>

  override protected def mkDbConfig(basicConfig: DbBasicConfig): Postgres =
    basicConfig.toPostgresDbConfig

  override protected def createSetup(): DbStorageSetup = {
    // postgres has limit of 63 chars for the db name
    val dbName = this.getClass.getSimpleName.replaceAll("[^a-zA-Z0-9]", "").toLowerCase.take(63)
    DbStorageSetup.postgres(loggerFactory, migrationMode, mkDbConfig, useDbNameO = Some(dbName))
  }
}
