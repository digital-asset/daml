// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.ResourceContext
import com.daml.logging.LoggingContext
import com.daml.platform.store.DbType
import com.daml.platform.store.backend.StorageBackend
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.{ExecutionContext, Future}

trait IndexerStabilitySpec
    extends AsyncFlatSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with Eventually {

  // To be overriden by the spec implementation
  def jdbcUrl: String

  // The default EC is coming from AsyncTestSuite and is serial, do not use it
  implicit val ec: ExecutionContext = system.dispatcher
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  behavior of "concurrently running indexers"

  it should "correctly work in high availability mode" in {
    val updatesPerSecond = 10 // Number of updates per second produced by the read service
    val indexerCount = 8 // Number of concurrently running indexers
    val restartIterations = 8 // Number of times the indexer should restart

    implicit val rc: ResourceContext = ResourceContext(ec)

    info(s"Creating indexers fixture with $indexerCount indexers")
    IndexerStabilityTestFixture
      .owner(
        updatesPerSecond,
        indexerCount,
        jdbcUrl,
        materializer,
      )
      .use[Unit] { indexers =>
        var abortedIndexer: Option[ReadServiceAndIndexer] = None
        (1 to restartIterations).foreach(_ => {
          // Assert that there is exactly one indexer running
          val activeIndexer = findActiveIndexer(indexers)
          info(s"Indexer ${activeIndexer.readService.name} is running")

          // The indexer should appear "healthy"
          assertHealthStatus(activeIndexer, HealthStatus.healthy)
          info(s"Indexer ${activeIndexer.readService.name} appears to be healthy")

          // At this point, the indexer that was aborted by the previous iteration can be reset,
          // in order to keep the pool of competing indexers full.
          abortedIndexer.foreach(idx => {
            idx.readService.reset()
            info(s"ReadService ${idx.readService.name} was reset")
          })

          // Abort the indexer by terminating the ReadService stream
          activeIndexer.readService.abort(simulatedFailure())
          abortedIndexer = Some(activeIndexer)
          info(s"ReadService ${activeIndexer.readService.name} was aborted")

          // The indexer should appear "unhealthy"
          assertHealthStatus(activeIndexer, HealthStatus.unhealthy)
          info(s"Indexer ${activeIndexer.readService.name} appears to be unhealthy")
        })

        // Stop all indexers, in order to stop all database operations
        indexers.indexers.foreach(_.readService.abort(simulatedFailure()))
        indexers.indexers.foreach(assertHealthStatus(_, HealthStatus.unhealthy))
        info(s"All ReadServices were aborted")

        // Verify the integrity of the index database
        val storageBackend = StorageBackend.of(DbType.jdbcType(jdbcUrl))
        val dataSource = storageBackend.createDataSource(jdbcUrl)
        val connection = dataSource.getConnection()
        storageBackend.verifyIntegrity()(connection)
        connection.close()
        info(s"Integrity of the index database was checked")

        Future.successful(())
      }
      .map(_ => succeed)
  }

  // Finds the only indexer that has subscribed to the ReadService stream
  private def findActiveIndexer(indexers: Indexers): ReadServiceAndIndexer = {
    // It takes some time until a new indexer takes over after a failure.
    // The default ScalaTest timeout for eventually() is too short for this.
    implicit val patienceConfig: PatienceConfig = PatienceConfig(
      timeout = scaled(Span(10, Seconds)),
      interval = scaled(Span(100, Millis)),
    )
    eventually {
      indexers.runningIndexer
    }
  }

  // Asserts that the given indexer changes its health status to the given value
  private def assertHealthStatus(
      indexer: ReadServiceAndIndexer,
      status: HealthStatus,
  )(implicit pos: org.scalactic.source.Position): Assertion = {
    // It takes some time until an indexer changes its health status after a failure.
    // The default ScalaTest timeout for eventually() is too short for this.
    implicit val patienceConfig: PatienceConfig = PatienceConfig(
      timeout = scaled(Span(1, Seconds)),
      interval = scaled(Span(100, Millis)),
    )
    eventually {
      assert(indexer.indexing.currentHealth() == status)
    }
  }

  private def simulatedFailure() = new RuntimeException("Simulated failure")
}
