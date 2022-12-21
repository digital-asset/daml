// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.perf.util

import com.daml.ledger.api.perf.util.reporter.JMeterReporter
import org.scalameter.KeyValue
import org.scalameter.api._
import org.scalameter.execution.{LocalExecutor, SeparateJvmsExecutor}
import org.scalameter.picklers.Implicits._
import org.scalameter.reporting.RegressionReporter

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/** Contains common elements for ScalaMeter tests that we expect to reuse.
  * Values are lazy to make sure that their usage doesn't result in NPEs.
  * They are also transient to avoid serializing them when [[SeparateJvmsExecutor]] is used.
  */
// we extend this, but Bench is sealed apparently
@SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
abstract class PerformanceTest extends Bench[Double] {

  protected def doWarmup: Boolean = true

  protected def localExecution: Boolean = false

  protected def asyncTimeout: FiniteDuration = 5.minutes

  private def createExecutor =
    if (localExecution) LocalExecutor.apply[Double] _ else (SeparateJvmsExecutor.apply[Double] _)

  @transient protected lazy val warmer: Warmer =
    if (doWarmup) Executor.Warmer.Default() else Executor.Warmer.Zero

  @transient protected lazy val aggregator: Aggregator[Double] = Aggregator.median[Double]

  @transient lazy val executor: Executor[Double] =
    createExecutor(warmer, aggregator, measurer)

  @transient lazy val measurer: Measurer[Double] = Measurer.Default()

  @transient lazy val reporter: Reporter[Double] = Reporter.Composite(
    RegressionReporter[Double](
      RegressionReporter.Tester.Accepter(),
      RegressionReporter.Historian.ExponentialBackoff(),
    ),
    new JMeterReporter[Double](this.getClass),
  )

  @transient lazy val persistor = Persistor.None

  protected def daConfig: Seq[org.scalameter.KeyValue] =
    Seq[org.scalameter.KeyValue](
      KeyValue(exec.independentSamples -> 1),
      KeyValue(exec.minWarmupRuns -> 5),
      KeyValue(exec.benchRuns -> 20),
      KeyValue(exec.jvmflags -> List("-Xmx4096m", "-Xms4096m")),
      KeyValue(verbose -> true),
    )

  protected def await[T](f: => Future[T]): T = {
    Await.result(f, asyncTimeout)
  }

}
