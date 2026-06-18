// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import better.files.File
import com.digitalasset.canton.concurrent.Threading
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class UniqueBoundedCounterTest
    extends AnyFlatSpec
    with BaseTest
    with BeforeAndAfterEach
    with HasExecutionContext {

  private var dataFile: File = _
  private var lockFile: File = _
  private val testLogger = logger.underlying

  override def beforeEach(): Unit = {
    dataFile = File.newTemporaryFile("unique_bounded_counter_test", ".dat")
    lockFile = File(dataFile.pathAsString + ".lock")
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    dataFile.delete(swallowIOExceptions = true)
    lockFile.delete(swallowIOExceptions = true)
    super.afterEach()
  }

  behavior of "UniqueBoundedCounter"

  it should "initialize counter to the start value if the file is new" in {
    val initial = 50
    val counter = new UniqueBoundedCounter(
      dataFile,
      startValue = initial,
      maxValue = 100,
    )(testLogger)

    val result = counter.get()
    result.success.value should be(initial)
  }

  it should "use the existing value if the file already exists" in {
    val initial = 123
    val maxValue = 200
    // Create and write an initial value manually first (simulate existing file)
    val counter1 =
      new UniqueBoundedCounter(dataFile, startValue = initial, maxValue = maxValue)(
        testLogger
      )
    counter1.incrementAndGet()

    // Create a new instance pointing to the same file, with a different initialValue
    val counter2 =
      new UniqueBoundedCounter(dataFile, startValue = 166, maxValue = maxValue)(
        testLogger
      )

    // Get should return the value written previously, ignoring the new start value
    counter2.get().success.value should be(initial + 1)
  }

  it should "increment correctly" in {
    val initial = 10
    val counter = new UniqueBoundedCounter(
      dataFile,
      startValue = initial,
      maxValue = 100,
    )(testLogger)
    counter.incrementAndGet().success.value should be(initial + 1)
    counter.incrementAndGet().success.value should be(initial + 2)
    counter.get().success.value should be(initial + 2)
  }

  it should "wrap around correctly when maximum value is reached" in {
    val initial = 2
    val maxVal = 5
    val counter = new UniqueBoundedCounter(
      dataFile,
      startValue = initial,
      maxValue = maxVal,
    )(testLogger)

    counter.get().success.value should be(initial)
    counter.incrementAndGet().success.value should be(initial + 1)
    counter.incrementAndGet().success.value should be(initial + 2)
    counter.incrementAndGet().success.value should be(maxVal)
    counter.incrementAndGet().success.value should be(initial)
  }

  it should "generate unique counters concurrently without lock exceptions" in {
    val numThreads = Threading.detectNumberOfThreads(noTracingLogger).unwrap
    val incrementsPerThread = 10000
    val totalIncrements = numThreads * incrementsPerThread
    val startValue = 1000
    // Use Int.MaxValue to effectively prevent wrap-around during this concurrency test
    val counter = new UniqueBoundedCounter(dataFile, startValue, Int.MaxValue)(testLogger)

    val obtainedValues = ConcurrentHashMap.newKeySet[Int]()

    val futures: Seq[Future[Unit]] = (1 to numThreads).map { threadId =>
      Future {
        // Set a descriptive thread name for logging/debugging
        val currentThread = Thread.currentThread()
        currentThread.setName(s"${getClass.getSimpleName}_lock-contention-test_worker-$threadId")
        for (i <- 1 to incrementsPerThread) {
          val result: Try[Int] = counter.incrementAndGet()
          result match {
            case Success(value) =>
              // Attempt to add the obtained value. add() returns false if it was already present
              if (!obtainedValues.add(value)) {
                fail(
                  s"Duplicate value detected: $value by thread ${currentThread.getName} increment $i"
                )
              }
            case Failure(e) =>
              // If any increment fails (e.g., lock timeout after retries), fail the future/test
              fail(
                s"Concurrent increment failed for thread ${currentThread.getName} increment $i: ${e.getMessage}",
                e,
              )
          }
        }
      }
    }

    // Wait long enough; may run slower when run with other tests concurrently
    Await.result(Future.sequence(futures), 3.minutes)

    val finalValue = counter.get()
    finalValue.success.value should be(startValue + totalIncrements)
  }
}
