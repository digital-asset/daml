// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.concurrent.ExecutionContext
import scala.util.Try

trait BisectHandle {
  def completed(): Unit
  def next(stage: String): BisectHandle
}

trait BisectUtil {
  def track[T](stage: String)(res: => T): T
  def trackAsync[F[_], T](stage: String)(res: => F[T])(implicit
      ec: ExecutionContext,
      F: Thereafter[F],
  ): F[T]

  def start(stage: String): BisectHandle
}

object BisectUtil {
  object Noop extends BisectUtil {
    override def track[T](stage: String)(res: => T): T = res
    def trackAsync[F[_], T](stage: String)(res: => F[T])(implicit
        ec: ExecutionContext,
        F: Thereafter[F],
    ): F[T] = res
    override def start(stage: String): BisectHandle = new BisectHandle {
      override def completed(): Unit = ()
      override def next(nextStage: String): BisectHandle = this
    }
  }
  def measure(
      name: String,
      loggerFactory: NamedLoggerFactory,
      report: PositiveInt = PositiveInt.tryCreate(60),
  ): BisectUtil =
    new BisectUtilImpl(name, loggerFactory, report)

  /** small helper class to debug sequential performance issues
    *
    * profiling doesn't help usually to debug a sequential pipeline. this class here can be used to
    * easily bisect the issues.
    *
    * often you might have a pipeline stage1 -> stage2 (stepA, stepB) -> stage3 -> ... -> stageN the
    * stages normally only take a very small time - so averaging over many invocations helps.
    *
    * just generate the util and then wrap the calls bisect.track("stage1")(stage1) etc. or val h1 =
    * bisect.start("stage1") stage1 h2 = h1.next("stage2") stage2 h2.completed() etc.
    *
    * every "report" calls (default 500 - you might want to increase this) the current timings are
    * printed to the log.
    *
    * just run a test and look at the last report call which tells you where the time was spent
    */
  private class BisectUtilImpl(
      name: String,
      override protected val loggerFactory: NamedLoggerFactory,
      report: PositiveInt = PositiveInt.tryCreate(500),
  ) extends NamedLogging
      with BisectUtil {
    import Thereafter.syntax.*

    @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
    override def track[T](stage: String)(res: => T): T = {
      val handle = start(stage)
      Try(res).thereafter(_ => handle.completed()).get
    }

    def trackAsync[F[_], T](stage: String)(res: => F[T])(implicit
        ec: ExecutionContext,
        F: Thereafter[F],
    ): F[T] = {
      val handle = start(stage)
      F.thereafter(res)(_ => handle.completed())
    }

    private val counter = new AtomicInteger(0)
    private val measurements = new AtomicReference[Map[String, Long]](Map())

    override def start(stage: String): BisectHandle =
      new BisectHandle {
        private val startTime = System.nanoTime()
        private val isComplete = new AtomicBoolean(false)

        private def update() = {
          val elapsed = System.nanoTime() - startTime
          if (isComplete.getAndSet(true)) {
            throw new IllegalStateException(s"Stage '$stage' already completed")
          }
          measurements.updateAndGet { current =>
            current.updatedWith(stage)(timeO => Some(elapsed + timeO.getOrElse(0L)))
          }
          ()
        }

        override def completed(): Unit = {
          update()
          if (counter.incrementAndGet() % report.value == 0) {
            val str = measurements
              .get()
              .toSeq
              .sortBy { case (_, time) => -time }
              .map { case (stage, time) =>
                s"$stage: ${time / 1000000L} ms"
              }
              .mkString("\n  ")
            noTracingLogger.info(s"Bisect report of $name:\n  $str")
          }
        }

        override def next(nextStage: String): BisectHandle = {
          completed()
          start(nextStage)
        }
      }
  }
}
