// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import org.slf4j.Logger

import java.time.Instant
import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

class MetricalStreamObserver[T](reportingPeriod: Duration, logger: Logger)(
    countingFunction: T => Int,
    sizingFunction: T => Int,
) extends ObserverWithResult[T](logger) {

  private val timer = new Timer(true)
  timer.schedule(new PeriodicalReportingTask(reportingPeriod.toMillis), 0, reportingPeriod.toMillis)

  private val transactionCount = new AtomicInteger()
  private val sizeRateList: ListBuffer[Double] = ListBuffer.empty
  private val currentSizeBucket = new AtomicInteger()
  private val startTime = Instant.now()

  override def onNext(value: T): Unit = {
    Thread.sleep(100)
    transactionCount.addAndGet(countingFunction(value))
    currentSizeBucket.addAndGet(sizingFunction(value))
    super.onNext(value)
  }

  override def onCompleted(): Unit = {
    val duration = totalDurationSeconds
    val rate = transactionCount.get() / duration
    val sizeRate =
      if (sizeRateList.nonEmpty) s"${rounded(sizeRateList.sum / sizeRateList.length)}"
      else "-"

    logger.info(
      s"Processed ${transactionCount
        .get()} transactions in ${rounded(duration)} [s], average rate: ${rounded(rate)} [tx/s], $sizeRate} [MB/s]"
    )
    super.onCompleted()
  }

  private def totalDurationSeconds: Double =
    (Instant.now().toEpochMilli - startTime.toEpochMilli) / 1000.0

  private class PeriodicalReportingTask(periodMillis: Long) extends TimerTask {
    private val lastCount = new AtomicInteger()

    override def run(): Unit = {
      val count = transactionCount.get()
      val rate = (count - lastCount.get()) * 1000.0 / periodMillis
      val sizeRate = currentSizeBucket.get() * 1000.0 / 1024 / 1024 / periodMillis
      currentSizeBucket.set(0)
      sizeRateList += sizeRate
      logger.info(
        s"Rate: ${rounded(rate)} [tx/s], ${rounded(sizeRate)} [MB/s], total count: ${transactionCount.get()}."
      )
      lastCount.set(count)
    }
  }

  private def rounded(value: Double): String = "%.2f".format(value)

}
