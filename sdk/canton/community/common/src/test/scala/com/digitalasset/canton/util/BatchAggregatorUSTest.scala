// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{LogEntry, TracedLogger}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.github.blemale.scaffeine.Scaffeine
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.concurrent.TrieMap
import scala.util.Random

class BatchAggregatorUSTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with HasTestCloseContext {
  type K = Int
  type V = String
  type BatchGetterType = NonEmpty[Seq[Traced[K]]] => FutureUnlessShutdown[Iterable[V]]

  private val defaultKeyToValue: K => V = _.toString
  private val defaultBatchGetter: NonEmpty[Seq[Traced[K]]] => FutureUnlessShutdown[Iterable[V]] =
    keys => FutureUnlessShutdown.pure(keys.map(item => defaultKeyToValue(item.value)))

  private val defaultMaximumInFlight: Int = 5
  private val defaultMaximumBatchSize: Int = 5

  private def aggregatorWithDefaults(
      maximumInFlight: Int = defaultMaximumInFlight,
      batchGetter: BatchGetterType = defaultBatchGetter,
  ): BatchAggregatorUS[K, V] = {
    val processor = new BatchAggregatorUS.ProcessorUS[K, V] {
      override def kind: String = "item"
      override def logger: TracedLogger = BatchAggregatorUSTest.this.logger
      override def executeBatch(items: NonEmpty[Seq[Traced[K]]])(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[Iterable[V]] = batchGetter(items)
      override def prettyItem: Pretty[K] = implicitly
    }

    val config = BatchAggregatorConfig(
      maximumInFlight = PositiveNumeric.tryCreate(maximumInFlight),
      maximumBatchSize = PositiveNumeric.tryCreate(defaultMaximumBatchSize),
    )

    BatchAggregatorUS[K, V](processor, config)
  }

  /** @param requestsCountPerSize Track the number of requests per size
    * @param blocker Future that blocks the computations.
    * @return The default batcher (Int => String = _.toString)
    */
  private def batchGetterWithCounter(
      requestsCountPerSize: TrieMap[Int, Int],
      blocker: FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit,
  ): BatchGetterType =
    keys => {
      requestsCountPerSize.updateWith(keys.size)(_.map(count => count + 1).orElse(Some(1)))
      blocker.flatMap(_ =>
        FutureUnlessShutdown.pure(keys.toList.map(item => defaultKeyToValue(item.value)))
      )
    }

  case class CacheWithAggregator(aggregator: BatchAggregatorUS[K, V])(implicit
      traceContext: TraceContext
  ) {
    private val cache = Scaffeine().buildAsync[K, V]()

    def get(key: K): FutureUnlessShutdown[V] =
      FutureUnlessShutdown.outcomeF(cache.get(key, key => aggregator.run(key).futureValueUS))
  }

  object CacheWithAggregator {
    def apply(batchGetter: BatchGetterType = defaultBatchGetter): CacheWithAggregator = {
      val queryBatcher = aggregatorWithDefaults(batchGetter = batchGetter)
      CacheWithAggregator(queryBatcher)
    }
  }

  "BatchAggregatorUS" should {
    "batch queries when the number of in-flight queries is too big" in {
      val blocker = new PromiseUnlessShutdown[Unit]("blocker", FutureSupervisor.Noop)

      val requestsCountPerSize = TrieMap[Int, Int]()
      val aggregator = aggregatorWithDefaults(
        maximumInFlight = 1,
        batchGetter = batchGetterWithCounter(requestsCountPerSize, blocker.futureUS),
      )

      val resultF = List(1, 2, 3).parTraverse(aggregator.run)

      blocker.success(UnlessShutdown.unit)

      resultF.futureValueUS shouldBe List("1", "2", "3")
      requestsCountPerSize.toMap shouldBe Map(
        1 -> 1, // One request for a single element
        2 -> 1, // One request for two elements
      )
    }

    "propagate an error thrown when issuing a single request" in {
      val exception = new RuntimeException("sad getter")

      val cache = CacheWithAggregator(batchGetter = _ => FutureUnlessShutdown.failed(exception))
      val key = 42

      loggerFactory
        .assertThrowsAndLogsAsync[RuntimeException](
          cache.get(key).unwrap,
          _.getCause.getCause shouldBe exception,
          logEntry => {
            logEntry.errorMessage shouldBe s"Failed to process item $key"
            logEntry.throwable shouldBe Some(exception)
          },
        )
        .futureValue
    }

    "propagate an error when no result is returned" in {
      val key = 41

      val aggregator =
        aggregatorWithDefaults(
          maximumInFlight = 1,
          batchGetter = _ => FutureUnlessShutdown.pure(Nil),
        )

      val result = loggerFactory
        .assertLogs(
          aggregator.run(key).failed.futureValueUS,
          _.errorMessage should include("executeBatch returned an empty sequence of results"),
          _.errorMessage shouldBe s"Failed to process item $key",
        )

      result shouldBe a[RuntimeException]
    }

    "propagate an error thrown by the getter" in {
      val blocker = new PromiseUnlessShutdown[Unit]("blocker", FutureSupervisor.Noop)

      val exception = new RuntimeException("sad getter")

      val aggregator = aggregatorWithDefaults(
        maximumInFlight = 1,
        batchGetter =
          _ => blocker.futureUS.flatMap(_ => FutureUnlessShutdown.failed[Iterable[V]](exception)),
      )

      val results = List(1, 2, 3).map(aggregator.run)

      loggerFactory.assertLogsUnordered(
        {
          blocker.success(UnlessShutdown.unit)
          results.foreach(_.failed.futureValueUS shouldBe exception)
        },
        _.errorMessage shouldBe "Failed to process item 1",
        _.errorMessage shouldBe show"Batch request failed for items ${Seq(2, 3)}",
      )
    }

    "support many requests" in {
      val aggregator = aggregatorWithDefaults(
        maximumInFlight = 2,
        batchGetter = keys =>
          FutureUnlessShutdown.pure {
            Threading.sleep(Random.nextLong(50))
            keys.toList.map(key => defaultKeyToValue(key.value))
          },
      )

      val requests = (0 until 100).map(_ => Random.nextInt(20)).toList
      val expectedResult = requests.map(key => (key, defaultKeyToValue(key)))

      val results = requests.parTraverse(key => aggregator.run(key).map((key, _))).futureValueUS
      results shouldBe expectedResult
    }

    "complain about too few results in the batch response" in {
      val blocker = new PromiseUnlessShutdown[Unit]("blocker", FutureSupervisor.Noop)

      val aggregator = aggregatorWithDefaults(
        maximumInFlight = 1,
        batchGetter = keys =>
          blocker.futureUS.flatMap { _ =>
            if (keys.size == 1) FutureUnlessShutdown.pure(List("0"))
            else
              FutureUnlessShutdown.pure(Iterable.empty)
          },
      )

      val results = List(0, 1, 2).map(aggregator.run)

      def tooFewResponses(key: K)(logEntry: LogEntry): Assertion = {
        logEntry.errorMessage shouldBe ErrorUtil.internalErrorMessage
        logEntry.throwable.value.getMessage should include(show"No response for item $key")
      }

      loggerFactory.assertLogs(
        {
          blocker.success(UnlessShutdown.unit)

          results(0).futureValueUS shouldBe "0"
          results(1).failed.futureValueUS.getMessage shouldBe "No response for item 1"
          results(2).failed.futureValueUS.getMessage shouldBe "No response for item 2"
        },
        tooFewResponses(1),
        tooFewResponses(2),
      )
    }

    "complain about too many results in the batch response" in {
      val blocker = new PromiseUnlessShutdown[Unit]("blocker", FutureSupervisor.Noop)

      val aggregator = aggregatorWithDefaults(
        maximumInFlight = 1,
        batchGetter = keys =>
          blocker.futureUS.flatMap { _ =>
            if (keys.size == 1) FutureUnlessShutdown.pure(List("0"))
            else
              defaultBatchGetter(keys).map(_.toList).map(_ :+ "42")
          },
      )

      val results = List(0, 1, 2).map(aggregator.run)

      loggerFactory.assertLogs(
        {
          blocker.success(UnlessShutdown.unit)
          results.sequence.futureValueUS shouldBe List("0", "1", "2")
        },
        _.errorMessage should include("Received 1 excess responses for item batch"),
      )
    }
  }
}
