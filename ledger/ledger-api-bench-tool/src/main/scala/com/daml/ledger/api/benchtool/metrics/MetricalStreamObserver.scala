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
    val duration = totalDurationSeconds
    val reports = metrics.flatMap(_.completeInfo(duration).toList)
    logger.info(namedMessage(s"Summary: ${reports.mkString(", ")}"))
    super.onCompleted()
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
