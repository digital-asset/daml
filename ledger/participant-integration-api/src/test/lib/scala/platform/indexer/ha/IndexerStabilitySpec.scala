// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.ResourceContext
import com.daml.logging.LoggingContext
import com.daml.platform.store.DbType
import com.daml.platform.store.backend.{ParameterStorageBackend, StorageBackendFactory}
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import java.sql.Connection

import scala.concurrent.{ExecutionContext, Future}

trait IndexerStabilitySpec
    extends AsyncFlatSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with Eventually {

  import IndexerStabilitySpec._

  // To be overriden by the spec implementation
  def jdbcUrl: String
  // This will be used to pick lock IDs for DB locking
  def lockIdSeed: Int

  // The default EC is coming from AsyncTestSuite and is serial, do not use it
  implicit val ec: ExecutionContext = system.dispatcher
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  behavior of "concurrently running indexers"

  it should "correctly work in high availability mode" in {
    val updatesPerSecond = 10 // Number of updates per second produced by the read service
    val indexerCount = 8 // Number of concurrently running indexers
    val restartIterations = 4 // Number of times the indexer should restart

    implicit val rc: ResourceContext = ResourceContext(ec)

    info(s"Creating indexers fixture with $indexerCount indexers")
    IndexerStabilityTestFixture
      .owner(
        updatesPerSecond,
        indexerCount,
        jdbcUrl,
        lockIdSeed,
        materializer,
      )
      .use[Unit] { indexers =>
        val factory = StorageBackendFactory.of(DbType.jdbcType(jdbcUrl))
        val dataSource = factory.createDataSourceStorageBackend.createDataSource(jdbcUrl)
        val parameterStorageBackend = factory.createParameterStorageBackend
        val integrityStorageBackend = factory.createIntegrityStorageBackend
        val connection = dataSource.getConnection()

        Iterator
          .iterate(IterationState())(previousState => {
            // Assert that there is exactly one indexer running
            val activeIndexer = findActiveIndexer(indexers)
            info(s"Indexer ${activeIndexer.readService.name} is running")

            // Assert that state updates are being indexed
            assertLedgerEndHasMoved(parameterStorageBackend, connection)
            info("Ledger end has moved")

            // At this point, the indexer that was aborted by the previous iteration can be reset,
            // in order to keep the pool of competing indexers full.
            previousState.abortedIndexer.foreach(idx => {
              idx.readService.reset()
              info(s"ReadService ${idx.readService.name} was reset")
            })

            // Abort the indexer by terminating the ReadService stream
            activeIndexer.readService.abort(simulatedFailure())
            info(s"ReadService ${activeIndexer.readService.name} was aborted")

            IterationState(Some(activeIndexer))
          })
          .take(restartIterations + 1)
          .foreach(_ => ())

        // Stop all indexers, in order to stop all database operations
        indexers.indexers.foreach(_.readService.abort(simulatedFailure()))
        info(s"All ReadServices were aborted")

        // Wait until all indexers stop using the database, otherwise the test will
        // fail while trying to drop the database at the end.
        // It can take some time until all indexers actually stop indexing after the
        // state update stream was aborted. It is difficult to observe this event,
        // as the only externally visible signal is the health status of the indexer,
        // which is only "unhealthy" while RecoveringIndexer is waiting to restart.
        // Instead, we just wait a short time.
        Thread.sleep(1000L)

        // Verify the integrity of the index database
        integrityStorageBackend.verifyIntegrity()(connection)
        info(s"Integrity of the index database was checked")

        connection.close()
        Future.successful(())
      }
      .map(_ => succeed)
  }

  // Finds the first non-aborted indexer that has subscribed to the ReadService stream
  private def findActiveIndexer(indexers: Indexers): ReadServiceAndIndexer = {
    // It takes some time until a new indexer takes over after a failure.
    // The default ScalaTest timeout for eventually() is too short for this.
    implicit val patienceConfig: PatienceConfig = PatienceConfig(
      timeout = scaled(Span(10, Seconds)),
      interval = scaled(Span(100, Millis)),
    )
    eventually {
      indexers.runningIndexers.headOption.getOrElse(
        throw new RuntimeException("No indexer running")
      )
    }
  }

  // Asserts that the ledger end has moved at least the specified number of events within a short time
  private def assertLedgerEndHasMoved(
      parameterStorageBackend: ParameterStorageBackend,
      connection: Connection,
  )(implicit pos: org.scalactic.source.Position): Assertion = {
    implicit val patienceConfig: PatienceConfig = PatienceConfig(
      timeout = scaled(Span(10, Seconds)),
      interval = scaled(Span(100, Millis)),
    )
    // Note: we don't know exactly at which ledger end the current indexer has started.
    // We only observe that the ledger end is moving right now.
    val initialLedgerEnd = parameterStorageBackend.ledgerEnd(connection)
    val minEvents = 2L
    eventually {
      val ledgerEnd = parameterStorageBackend.ledgerEnd(connection)
      assert(ledgerEnd.lastEventSeqId > initialLedgerEnd.lastEventSeqId + minEvents)
    }
  }

  private def simulatedFailure() = new RuntimeException("Simulated failure")
}

object IndexerStabilitySpec {
  case class IterationState(
      abortedIndexer: Option[ReadServiceAndIndexer] = None
  )
}
