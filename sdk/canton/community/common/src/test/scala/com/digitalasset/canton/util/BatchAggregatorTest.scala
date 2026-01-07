// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveNumeric}
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
import scala.collection.immutable
import scala.util.Random

class BatchAggregatorTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with HasTestCloseContext {
  type K = Int
  type V = String
  type BatchGetterType = NonEmpty[Seq[Traced[K]]] => FutureUnlessShutdown[immutable.Iterable[V]]

  private val defaultKeyToValue: K => V = _.toString
  private val defaultBatchGetter
      : NonEmpty[Seq[Traced[K]]] => FutureUnlessShutdown[immutable.Iterable[V]] =
    keys => FutureUnlessShutdown.pure(keys.map(item => defaultKeyToValue(item.value)))

  private val defaultMaximumInFlight: Int = 5
  private val defaultMaximumBatchSize: Int = 5

  private def aggregatorWithDefaults(
      maximumInFlight: Int = defaultMaximumInFlight,
      batchGetter: BatchGetterType,
  ): BatchAggregatorImpl[K, V] = {

    val processor =
      new BatchAggregator.Processor[K, V] {
        override def kind: String = "item"
        override def logger: TracedLogger = BatchAggregatorTest.this.logger
        override def executeBatch(items: NonEmpty[Seq[Traced[K]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[immutable.Iterable[V]] = batchGetter(items)
        override def prettyItem: Pretty[K] = implicitly
      }

    new BatchAggregatorImpl(
      processor,
      maximumInFlight = maximumInFlight,
      maximumBatchSize = PositiveNumeric.tryCreate(defaultMaximumBatchSize),
    )
  }

  /** @param requestsCountPerSize
    *   Track the number of requests per size
    * @param blocker
    *   Future that blocks the computations.
    * @return
    *   The default batcher (Int => String = _.toString)
    */
  private def batchGetterWithCounter(
      requestsCountPerSize: TrieMap[Int, Int],
      blocker: FutureUnlessShutdown[Unit],
  ): BatchGetterType =
    keys => {
      requestsCountPerSize.updateWith(keys.size)(_.map(count => count + 1).orElse(Some(1)))
      blocker.flatMap(_ =>
        FutureUnlessShutdown.pure(keys.toList.map(item => defaultKeyToValue(item.value)))
      )
    }

  case class CacheWithAggregator(aggregator: BatchAggregator[K, V])(implicit
      traceContext: TraceContext
  ) {
    private val cache = Scaffeine().executor(executorService).buildAsync[K, V]()

    def get(key: K): FutureUnlessShutdown[V] =
      FutureUnlessShutdown.outcomeF(cache.get(key, key => aggregator.run(key).futureValueUS))
  }

  object CacheWithAggregator {
    def apply(batchGetter: BatchGetterType = defaultBatchGetter): CacheWithAggregator = {
      val queryBatcher = aggregatorWithDefaults(batchGetter = batchGetter)
      CacheWithAggregator(queryBatcher)
    }
  }

  "BatchAggregator" when {

    "invoked with a single item" should {
      "batch queries when the number of in-flight queries is too big" in {
        val blocker = PromiseUnlessShutdown.unsupervised[Unit]()

        val requestsCountPerSize = TrieMap[Int, Int]()
        val aggregator = aggregatorWithDefaults(
          maximumInFlight = 1,
          batchGetter = batchGetterWithCounter(requestsCountPerSize, blocker.futureUS),
        )

        val resultF = List(1, 2, 3).parTraverse(aggregator.run)

        blocker.success(UnlessShutdown.unit)

        val result = resultF.futureValueUS
        result shouldBe List("1", "2", "3")
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
        val blocker = PromiseUnlessShutdown.unsupervised[Unit]()

        val exception = new RuntimeException("sad getter")

        val aggregator = aggregatorWithDefaults(
          maximumInFlight = 1,
          batchGetter = _ =>
            blocker.futureUS.flatMap(_ =>
              FutureUnlessShutdown.failed[immutable.Iterable[V]](exception)
            ),
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
        val blocker = PromiseUnlessShutdown.unsupervised[Unit]()

        val aggregator = aggregatorWithDefaults(
          maximumInFlight = 1,
          batchGetter = keys =>
            blocker.futureUS.flatMap { _ =>
              if (keys.sizeIs == 1) FutureUnlessShutdown.pure(List("0"))
              else
                FutureUnlessShutdown.pure(immutable.Iterable.empty)
            },
        )

        val results = List(0, 1, 2).map(aggregator.run)

        def tooFewResponses(key: K)(logEntry: LogEntry): Assertion = {
          logEntry.errorMessage shouldBe ErrorUtil.internalErrorMessage
          logEntry.throwable.value.getMessage should include(
            show"BatchAggregatorImpl.runTogether returned an empty Iterable for item $key"
          )
        }

        loggerFactory.assertLogsUnordered(
          {
            blocker.success(UnlessShutdown.unit)

            results(0).futureValueUS shouldBe "0"
            results(
              1
            ).failed.futureValueUS.getMessage shouldBe "BatchAggregatorImpl.runTogether returned an empty Iterable for item 1"
            results(
              2
            ).failed.futureValueUS.getMessage shouldBe "BatchAggregatorImpl.runTogether returned an empty Iterable for item 2"
          },
          tooFewResponses(1),
          tooFewResponses(2),
          logEntry =>
            logEntry.errorMessage should include(
              "Detected 2 excess items for item batch"
            ),
        )
      }

      "complain about too many results in the batch response" in {
        val blocker = PromiseUnlessShutdown.unsupervised[Unit]()

        val aggregator = aggregatorWithDefaults(
          maximumInFlight = 1,
          batchGetter = keys =>
            blocker.futureUS.flatMap { _ =>
              if (keys.sizeIs == 1) FutureUnlessShutdown.pure(List("0"))
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

    "invoked with multiple items that must end up in the same batch" should {

      "batch the items together if they fit" in {
        val blocker = PromiseUnlessShutdown.unsupervised[Unit]()

        val requestsCountPerSize = TrieMap[Int, Int]()
        val aggregator =
          aggregatorWithDefaults(
            maximumInFlight = 5,
            batchGetter = batchGetterWithCounter(requestsCountPerSize, blocker.futureUS),
          )

        val resultF =
          Seq(
            NonEmpty(Seq, Traced(0)),
            NonEmpty.from(1 to 5).map(_.map(Traced(_))).getOrElse(fail()),
            NonEmpty(Seq, Traced(6)),
          ).map(items => aggregator.runInSameBatch(items))
            .map {
              case Right(fus) => fus
              case Left(_) => fail("Did not expect a batch size rejection")
            }
            .parTraverse(identity)

        blocker.success(UnlessShutdown.unit)

        resultF.futureValueUS shouldBe Seq(
          List("0"),
          List("1", "2", "3", "4", "5"),
          List("6"),
        )
        requestsCountPerSize.toMap shouldBe Map(
          5 -> 1, // One batch for the five elements request
          1 -> 2,
        )
      }

      "reject a batch if it exceeds the maximum batch size" in {
        val blocker = PromiseUnlessShutdown.unsupervised[Unit]()

        val requestsCountPerSize = TrieMap[Int, Int]()
        val aggregator =
          aggregatorWithDefaults(
            maximumInFlight = 5,
            batchGetter = batchGetterWithCounter(requestsCountPerSize, blocker.futureUS),
          )

        val result =
          Vector(
            NonEmpty(Seq, Traced(0)),
            NonEmpty.from(1 to 6).map(_.map(Traced(_))).getOrElse(fail()),
            NonEmpty(Seq, Traced(7)),
          ).map(items => aggregator.runInSameBatch(items))

        blocker.success(UnlessShutdown.unit)

        result(1) shouldBe Left(PositiveInt.tryCreate(defaultMaximumBatchSize))

        val resultF = result.collect { case Right(fus) => fus }.parTraverse(identity)

        resultF.futureValueUS shouldBe Seq(List("0"), List("7"))
        requestsCountPerSize.toMap shouldBe Map(1 -> 2)
      }
    }

    "invoked with runMany for multiple items" should {

      "efficiently batch items when slots are available" in {
        val blocker = PromiseUnlessShutdown.unsupervised[Unit]()

        val requestsCountPerSize = TrieMap[Int, Int]()
        val aggregator =
          aggregatorWithDefaults(
            maximumInFlight = 2,
            batchGetter = batchGetterWithCounter(requestsCountPerSize, blocker.futureUS),
          )

        // Test with 12 items, should create batches of 5, 5, 2 (maximumBatchSize = 5)
        val items = NonEmpty.from(0 to 11).value.map(Traced(_))
        val resultF = aggregator.runMany(items)

        blocker.success(UnlessShutdown.unit)

        val result = resultF.futureValueUS
        result.toList shouldBe (0 to 11).map(_.toString).toList

        // Should have executed as [0..4], [5..9], [10,11]
        requestsCountPerSize.toMap shouldBe Map(
          5 -> 2, // Two batches of 5 items
          2 -> 1, // One batch of 2 items
        )
      }

      "fill existing queued batches with runMany items" in {
        val blocker = PromiseUnlessShutdown.unsupervised[Unit]()
        val batchSizes = scala.collection.mutable.ListBuffer[Int]()

        val processor =
          new BatchAggregator.Processor[K, V] {
            override def kind: String = "item"
            override def logger: TracedLogger = BatchAggregatorTest.this.logger
            override def executeBatch(items: NonEmpty[Seq[Traced[K]]])(implicit
                traceContext: TraceContext,
                callerCloseContext: CloseContext,
            ): FutureUnlessShutdown[immutable.Iterable[V]] = {
              scala.concurrent.blocking {
                batchSizes.synchronized(batchSizes += items.size)
              }
              blocker.futureUS.map(_ => items.toList.map(item => defaultKeyToValue(item.value)))
            }
            override def prettyItem: Pretty[K] = implicitly
          }

        val aggregator = new BatchAggregatorImpl(
          processor,
          maximumInFlight = 1,
          maximumBatchSize = PositiveNumeric.tryCreate(4), // Maximum batch size is 4
        )

        // First, occupy the slot with a single item
        val firstItemF = aggregator.run(99)

        // Create partial batches explicitly using runInSameBatch
        // These will queue as separate batches of sizes 3, 3, 3, 2
        val batch1F = aggregator.runInSameBatch(
          NonEmpty.from(0 to 2).map(_.map(Traced(_))).getOrElse(fail())
        )
        val batch2F = aggregator.runInSameBatch(
          NonEmpty.from(3 to 5).map(_.map(Traced(_))).getOrElse(fail())
        )
        val batch3F = aggregator.runInSameBatch(
          NonEmpty.from(6 to 8).map(_.map(Traced(_))).getOrElse(fail())
        )
        val batch4F = aggregator.runInSameBatch(
          NonEmpty.from(9 to 10).map(_.map(Traced(_))).getOrElse(fail())
        )

        // Now call runMany with 5 items
        // Expected: all 5 items will fill existing batches
        // Result should be batches of sizes: 1 (first item), 4, 4, 4, 4
        val runManyItems = NonEmpty.from(11 to 15).map(_.map(Traced(_))).getOrElse(fail())
        val runManyF = aggregator.runMany(runManyItems)

        // Release blocker
        blocker.success(UnlessShutdown.unit)

        // Verify all items complete
        firstItemF.futureValueUS shouldBe "99"

        // Verify batch results
        batch1F shouldBe a[Right[?, ?]]
        batch2F shouldBe a[Right[?, ?]]
        batch3F shouldBe a[Right[?, ?]]
        batch4F shouldBe a[Right[?, ?]]

        batch1F.getOrElse(fail()).futureValueUS.toList shouldBe List("0", "1", "2")
        batch2F.getOrElse(fail()).futureValueUS.toList shouldBe List("3", "4", "5")
        batch3F.getOrElse(fail()).futureValueUS.toList shouldBe List("6", "7", "8")
        batch4F.getOrElse(fail()).futureValueUS.toList shouldBe List("9", "10")

        runManyF.futureValueUS.toList shouldBe (11 to 15).map(_.toString).toList

        // Should have executed as [99], [0,1,2,11], [3,4,5,12], [6,7,8,13], [9,10,14,15]
        // Batch sizes should be: 1 (first item), then 4, 4, 4, 4
        // The runMany items fill the first batches and create a new batch (4)
        batchSizes.toList shouldBe List(1, 4, 4, 4, 4)
      }
    }
  }
}
