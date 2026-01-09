// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing

import cats.syntax.functor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.SequencerHighAvailabilityConfig
import com.digitalasset.canton.synchronizer.sequencer.store.{SequencerStore, SequencerStoreTest}
import com.digitalasset.canton.synchronizer.sequencer.{
  HASequencerWriterStoreFactory,
  SequencerWriterConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.BytesUnit
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

/** Verifies that many [[store.SequencerWriterStore]] instances can be created concurrently sharing
  * the same database and coordinate using locks to assign exclusive instance indexes.
  */
trait HASequencerWriterStoreFactoryTest extends HasExecutionContext {
  this: SequencerStoreTest with AsyncWordSpec with BaseTest with DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*

    storage.update_(sqlu"truncate table sequencer_watermarks", functionFullName)
  }

  "HASequencerWriterStoreFactory" should {
    "eventually allocate indexes and storage instances for all sequencer writers" in {
      val instances = 5
      val haConfig =
        SequencerHighAvailabilityConfig(totalNodeCount = PositiveInt.tryCreate(instances))
      val store = SequencerStore(
        storage,
        testedProtocolVersion,
        bufferedEventsMaxMemory = BytesUnit.zero, // HA mode does not support events cache
        bufferedEventsPreloadBatchSize =
          SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
        timeouts,
        loggerFactory,
        sequencerMember,
        // TODO(#15837): support HA in unified sequencer
        blockSequencerMode = false,
        cachingConfigs = CachingConfigs(),
        batchingConfig = BatchingConfig(),
        sequencerMetrics = SequencerMetrics.noop("ha-writer-store-factory-test"),
      )

      val writerStorageFactory =
        new HASequencerWriterStoreFactory(
          testedProtocolVersion,
          haConfig,
          None,
          None,
          sequencerMember,
          cachingConfigs = CachingConfigs(),
          batchingConfig = BatchingConfig(),
          sequencerMetrics = SequencerMetrics.noop("ha-writer-store-factory-test"),
          DefaultProcessingTimeouts.testing,
          exitOnFatalFailures = true,
          futureSupervisor,
          loggerFactory,
        )

      for {
        // we're doing this odd traversal to capture the Future[Either[...]] values returned so we can
        // explicitly ensure all storage instances created are closed in the yield block
        results <- Future.sequence {
          (0 until instances).toList.map { attemptIndex =>
            withNewTraceContext("writer") { implicit traceContext =>
              logger.debug(s"Attempting to create writer storage for $attemptIndex")

              for {
                result <- writerStorageFactory
                  .create(storage, store)
                  .onShutdown(fail("Create writer storage factory"))
                  .value
                _ <- result.toOption.fold(Future.unit) { writerStore =>
                  val instanceIndex = writerStore.instanceIndex
                  logger.debug(s"Marking $instanceIndex as online")
                  store.goOnline(instanceIndex, CantonTimestamp.Epoch).void.failOnShutdown
                }
              } yield result
            }
          }
        }
      } yield {
        try {
          val indexes = results.flatMap(_.map(_.instanceIndex).toOption.toList)
          indexes should contain allElementsOf ((0 until instances))
        } finally {
          // ensure all created storage instances are closed regardless of the assertion
          results.flatMap(_.toOption.toList).foreach(_.close())
        }
      }
    }
  }
}
