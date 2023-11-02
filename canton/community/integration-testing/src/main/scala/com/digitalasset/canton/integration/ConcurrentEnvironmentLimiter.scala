// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Semaphore, TimeUnit}
import scala.util.control.NonFatal

/** Although our integration tests are designed not to conflict with one another when running concurrently,
  * practically due to how resource heavy their setups are you will start encountering problems if we run
  * too many canton environments at once.
  * To prevent this we have a global limit on how many environments can be started concurrently.
  * NOTE: The limit is per class loader, therefore when running both enterprise and community integration tests, each
  * has their own limit as sbt creates a class loader per sub-project.
  * This defaults to 1 but can be overridden using the system property [[ConcurrentEnvironmentLimiter.IntegrationTestConcurrencyLimit]].
  * Integration test setups should create their environments within [[ConcurrentEnvironmentLimiter.create]]
  * and destroy them within [[ConcurrentEnvironmentLimiter.destroy]].
  * A semaphore is used to block creations until a permit is available, and then the permit is released after the environment is destroyed.
  *
  * Due to this approach tests will start but then be blocked until a permit is available. In some cases for many minutes.
  * This may well activate a slow poke notification in ScalaTest, however these are left in place to support discovery
  * of any tests that fail to halt entirely or run for an abnormally long amount of time.
  */
object ConcurrentEnvironmentLimiter extends LazyLogging {

  private sealed trait State
  private object New extends State {
    override def toString: String = "New"
  }
  private object Queued extends State {
    override def toString: String = "Queued"
  }
  private object Running extends State {
    override def toString: String = "Running"
  }
  private object Failed extends State {
    override def toString: String = "Failed"
  }
  private object Done extends State {
    override def toString: String = "Done"
  }

  val IntegrationTestConcurrencyLimit = "canton-test.integration.concurrency"

  private val concurrencyLimit: Int = System.getProperty(IntegrationTestConcurrencyLimit, "3").toInt

  /** Configured to be fair so earlier started tests will be first to get environments */
  private val semaphore = new Semaphore(concurrencyLimit, true)

  /** contains a map from test name to state (queue, run) */
  private val active = new AtomicReference[Map[String, (State, Long)]](Map.empty)

  private def change(name: String, state: State, purge: Boolean = false): Unit = {
    val now = System.nanoTime()
    val current = active
      .getAndUpdate { cur =>
        if (purge) cur.removed(name)
        else cur.updated(name, (state, now))
      }
    val (currentState, currentTime) = current.getOrElse(name, (New, now))
    val currentSet = current.keySet
    val numSet = if (purge) (currentSet - name) else (currentSet + name)
    logger.debug(s"${name}: $currentState => $state after ${TimeUnit.NANOSECONDS
        .toSeconds(now - currentTime)}s (${numSet.size} pending)")
  }

  private def getNumPermits(permits: PositiveInt): Int =
    Math.max(1, Math.min(permits.value, concurrencyLimit))

  /** Block an environment creation until a permit is available. */
  def create[A](name: String, permits: PositiveInt)(block: => A): A = {
    change(name, Queued)
    val numPermits = getNumPermits(permits)
    scala.concurrent.blocking {
      semaphore.acquire(numPermits)
    }
    change(name, Running)
    try block
    catch {
      // creations can easily fail and throw
      // capture these and immediately release the permit as the destroy method will not be called
      case NonFatal(e) =>
        semaphore.release(numPermits)
        change(name, Failed, purge = true)
        throw e
    }
  }

  /** Attempt to destroy an environment and ensure that the permit is released */
  def destroy[A](name: String, permits: PositiveInt)(block: => A): A =
    try block
    finally {
      semaphore.release(getNumPermits(permits))
      change(name, Done, purge = true)
    }
}
