// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.utils

import org.scalatest.exceptions.TestFailedException
import org.scalatest.{Assertion, Assertions}
import org.scalatest.flatspec.AsyncFlatSpec

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}

final class ConcurrencyLimiterSpec extends AsyncFlatSpec {
  behavior of "QueueBasedConcurrencyLimiter"

  it should "work with parallelism of 1" in {
    ConcurrencyLimiterSpec.runTest(
      createLimiter = ec => new QueueBasedConcurrencyLimiter(1, ec),
      waitTimeMillis = 1,
      threads = 32,
      items = 100,
      parallelism = 1,
      expectedParallelism = Some(1),
    )
  }

  it should "work with parallelism of 4" in {
    ConcurrencyLimiterSpec.runTest(
      createLimiter = ec => new QueueBasedConcurrencyLimiter(4, ec),
      waitTimeMillis = 1,
      threads = 32,
      items = 100,
      parallelism = 4,
      expectedParallelism = Some(4),
    )
  }

  it should "limit the parallelism to the level of the execution context" in {
    ConcurrencyLimiterSpec.runTest(
      createLimiter = ec => new QueueBasedConcurrencyLimiter(8, ec),
      waitTimeMillis = 1,
      threads = 4,
      items = 100,
      parallelism = 8,
      expectedParallelism = Some(4),
    )
  }

  it should "limit the parallelism to the number of work items" in {
    ConcurrencyLimiterSpec.runTest(
      createLimiter = ec => new QueueBasedConcurrencyLimiter(8, ec),
      waitTimeMillis = 1000,
      threads = 16,
      items = 4,
      parallelism = 8,
      expectedParallelism = Some(4),
    )
  }

  it should "work if the futures complete instantly" in {
    ConcurrencyLimiterSpec.runTest(
      createLimiter = ec => new QueueBasedConcurrencyLimiter(4, ec),
      waitTimeMillis = 0, // Test futures complete instantly
      threads = 32,
      items = 10000,
      parallelism = 4,
      expectedParallelism = None, // Futures complete too fast to reach target parallelism
    )
  }

  behavior of "NoConcurrencyLimiter"

  it should "not work" in {
    recoverToSucceededIf[TestFailedException] {
      ConcurrencyLimiterSpec.runTest(
        createLimiter = _ => new NoConcurrencyLimiter(),
        waitTimeMillis = 1,
        threads = 32,
        items = 100,
        parallelism = 4,
        expectedParallelism = None,
      )
    }
  }
}

object ConcurrencyLimiterSpec extends Assertions {
  def runTest(
      createLimiter: ExecutionContext => ConcurrencyLimiter,
      waitTimeMillis: Long,
      threads: Int,
      items: Int,
      parallelism: Int,
      expectedParallelism: Option[Int],
  ): Future[Assertion] = {
    // EC for running the test Futures
    val threadPoolExecutor: ExecutionContext =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threads))
    // EC for all other work
    implicit val ec: ExecutionContext =
      ExecutionContext.fromExecutorService(Executors.newWorkStealingPool())

    val running = new AtomicInteger(0)
    val limiter = createLimiter(ec)

    val results = (1 to items).map(i =>
      limiter.execute(
        // Note: this future is running in the thread pool executor that is capable of running many tasks in parallel
        // The limiter is responsible for not starting too many futures in parallel
        Future {
          val before = running.getAndIncrement()
          assert(
            before < parallelism,
            s"Task $i started although already $before tasks were running",
          )

          // Simulate some work
          if (waitTimeMillis > 0) {
            Thread.sleep(waitTimeMillis)
          }

          val after = running.decrementAndGet()
          assert(after < parallelism, s"Task $i finished while $after other tasks are running")

          before
        }(threadPoolExecutor)
      )
    )

    Future
      .sequence(results)
      .map(xs => {
        val actualParallelism = xs.max + 1
        expectedParallelism.foreach(expected =>
          assert(
            actualParallelism == expected,
            s"$expected were expected to run in parallel, but only up to $actualParallelism task were found to be running in parallel",
          )
        )
        succeed
      })
  }
}
