// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import org.slf4j.Logger

import java.time.Instant
import java.util.{Timer, TimerTask}
import scala.concurrent.duration.Duration

class MetricalStreamObserver[T](
    streamName: String,
    reportingPeriod: Duration,
    metrics: List[Metric[T]],
    logger: Logger,
) extends ObserverWithResult[T](logger) {

  private val timer = new Timer(true)
  timer.schedule(new PeriodicalReportingTask, 0, reportingPeriod.toMillis)

  private val startTime = Instant.now()

  override def onNext(value: T): Unit = {
    // TODO: remove sleep
    Thread.sleep(100)
    metrics.foreach(_.onNext(value))
    super.onNext(value)
  }

  override def onCompleted(): Unit = {
    logger.info(namedMessage(summary(totalDurationSeconds)))
    super.onCompleted()
  }

  private def summary(durationSeconds: Double): String = {
    val reports = metrics.flatMap { metric =>
      metric.completeInfo(durationSeconds) match {
        case Nil => Nil
        case results => List(s"""${metric.name}:
                                |${results.map(r => s"  $r").mkString("\n")}""".stripMargin)
      }
    }
    val reportWidth = 80
    val bar = "=" * reportWidth
    s"""
         |$bar
         |Stream: $streamName
         |Total duration: $durationSeconds [s]
         |${reports.mkString("\n")}
         |$bar""".stripMargin
  }

  private def totalDurationSeconds: Double =
    (Instant.now().toEpochMilli - startTime.toEpochMilli) / 1000.0

  private class PeriodicalReportingTask extends TimerTask {
    override def run(): Unit = {
      val periodicUpdates = metrics.map(_.periodicUpdate())
      logger.info(namedMessage(periodicUpdates.mkString(", ")))
    }
  }

  private def namedMessage(message: String) = s"[$streamName] $message"

}
