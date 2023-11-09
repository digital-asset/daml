// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import akka.stream.scaladsl.Source
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.scalatest.flatspec.AsyncFlatSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AkkaStreamParallelBatchedLoaderSpec
    extends AsyncFlatSpec
    with BaseTest
    with AkkaBeforeAndAfterAll {

  // AsyncFlatSpec is with serial execution context
  private implicit val ec: ExecutionContext = system.dispatcher

  private implicit val loggingContext = LoggingContextWithTrace.empty

  it should "not batch if no backpressure" in {
    val testee = new AkkaStreamParallelBatchedLoader[Int, Int](
      batchLoad = in => {
        Future(in.size shouldBe 1)
          .map(_ => in.map(_._1).map(x => x -> x).toMap)
      },
      createQueue = () => Source.queue(10),
      parallelism = 5,
      maxBatchSize = 10,
      loggerFactory = loggerFactory,
    )

    val inputs = 1.to(100)

    for {
      _ <- inputs.foldLeft(Future.successful(succeed)) { case (f, num) =>
        f.flatMap(_ =>
          testee
            .load(num)
            .map(_ shouldBe Some(num))
        )
      }
      _ <- testee.closeAsync() // teardown
    } yield succeed
  }

  it should "batch if backpressure" in {
    val fullBatchCounter = new AtomicInteger(0)
    val testee = new AkkaStreamParallelBatchedLoader[Int, Int](
      batchLoad = in => {
        Future {
          Threading.sleep(10)
          fullBatchCounter.incrementAndGet()
          succeed
        }
          .map(_ => in.map(_._1).map(x => x -> x).toMap)
      },
      createQueue = () => Source.queue(1000),
      parallelism = 5,
      maxBatchSize = 10,
      loggerFactory = loggerFactory,
    )

    val inputs = 1.to(100)

    for {
      _ <- Future.sequence(
        inputs.map(num => testee.load(num).map(_ shouldBe Some(num)))
      )
      _ <- testee.closeAsync() // teardown
    } yield {
      fullBatchCounter.get() should be > 8
    }
  }

  it should "throw backpressure error if queue exhausted" in {
    val testee = new AkkaStreamParallelBatchedLoader[Int, Int](
      batchLoad = in => {
        Future {
          Threading.sleep(10)
          succeed
        }
          .map(_ => in.map(_._1).map(x => x -> x).toMap)
      },
      createQueue = () => Source.queue(1),
      parallelism = 5,
      maxBatchSize = 10,
      loggerFactory = loggerFactory,
    )

    val inputs = 1.to(100)

    for {
      _ <- Future
        .sequence(
          inputs.map(num => testee.load(num).map(_ shouldBe Some(num)))
        )
        .failed
        .map(_.getMessage should include("PARTICIPANT_BACKPRESSURE"))
      _ <- testee.closeAsync() // teardown
    } yield succeed
  }

  it should "keep working if batch loading throws error" in {
    val testee = new AkkaStreamParallelBatchedLoader[Int, Int](
      batchLoad = in => {
        Future {
          if (in.head._1 == 50) throw new Exception("boom")
          else succeed
        }
          .map(_ => in.map(_._1).map(x => x -> x).toMap)
      },
      createQueue = () => Source.queue(1000),
      parallelism = 5,
      maxBatchSize = 1,
      loggerFactory = loggerFactory,
    )

    val inputs = 1.to(100)

    for {
      _ <- Future.sequence(
        inputs.map(num =>
          testee.load(num).transform {
            case Success(value) => Success(value shouldBe Some(num))
            case Failure(value) =>
              Try {
                num shouldBe 50
                value.getMessage should include("boom")
              }
          }
        )
      )
      _ <- testee.closeAsync() // teardown
    } yield succeed
  }

  it should "keep working if batch loading throws error before async barrier" in {
    val testee = new AkkaStreamParallelBatchedLoader[Int, Int](
      batchLoad = in => {
        if (in.head._1 == 50) throw new Exception("boom")
        Future(succeed)
          .map(_ => in.map(_._1).map(x => x -> x).toMap)
      },
      createQueue = () => Source.queue(1000),
      parallelism = 5,
      maxBatchSize = 1,
      loggerFactory = loggerFactory,
    )

    val inputs = 1.to(100)

    for {
      _ <- Future.sequence(
        inputs.map(num =>
          testee.load(num).transform {
            case Success(value) => Success(value shouldBe Some(num))
            case Failure(value) =>
              Try {
                num shouldBe 50
                value.getMessage should include("boom")
              }
          }
        )
      )
      _ <- testee.closeAsync() // teardown
    } yield succeed
  }

  it should "reject load requests after closed" in {
    val testee = new AkkaStreamParallelBatchedLoader[Int, Int](
      batchLoad = _ => {
        throw new Exception("boom")
      },
      createQueue = () => Source.queue(1000),
      parallelism = 5,
      maxBatchSize = 1,
      loggerFactory = loggerFactory,
    )

    for {
      _ <- testee.closeAsync()
      error <- testee.load(10).failed
    } yield {
      error.getMessage should include("Queue closed")
    }
  }

  it should "process enqueued items before closed" in {
    val testee = new AkkaStreamParallelBatchedLoader[Int, Int](
      batchLoad = in => {
        Future {
          Threading.sleep(10)
          succeed
        }
          .map(_ => in.map(_._1).map(x => x -> x).toMap)
      },
      createQueue = () => Source.queue(1000),
      parallelism = 5,
      maxBatchSize = 10,
      loggerFactory = loggerFactory,
    )

    val inputFs = 1.to(100).map(num => testee.load(num).map(_ shouldBe Some(num)))

    for {
      _ <- testee.closeAsync()
      _ <- Future.sequence(inputFs)
    } yield {
      succeed
    }
  }
}
