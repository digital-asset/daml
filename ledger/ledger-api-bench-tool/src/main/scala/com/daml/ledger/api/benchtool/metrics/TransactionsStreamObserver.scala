// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.v1.transaction_service.GetTransactionsResponse
import org.slf4j.Logger

import java.time.Instant
import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicInteger

class TransactionsStreamObserver(reportingPeriod: Long, logger: Logger)
    extends LogOnlyObserver[GetTransactionsResponse](logger) {

  private val timer = new Timer(true)
  timer.schedule(new LogTransactionCountTask(reportingPeriod), 0, reportingPeriod)

  private val transactionCount = new AtomicInteger()
  private val startTime = Instant.now()

  override def onNext(value: GetTransactionsResponse): Unit = {
    Thread.sleep(100)
    transactionCount.addAndGet(value.transactions.length)
    super.onNext(value)
  }

  override def onCompleted(): Unit = {
    val duration = totalDurationSeconds
    val rate = transactionCount.get() / duration
    logger.info(
      s"Processed ${transactionCount.get()} transactions in $duration [s], average rate: $rate [tx/s]"
    )
    super.onCompleted()
  }

  private def totalDurationSeconds: Double =
    (Instant.now().toEpochMilli - startTime.toEpochMilli) / 1000.0

  private class LogTransactionCountTask(periodMillis: Long) extends TimerTask {
    private val lastCount = new AtomicInteger()

    override def run(): Unit = {
      val count = transactionCount.get()
      val rate = (count - lastCount.get()) * 1000.0 / periodMillis
      logger.info(s"Rate: $rate [tx/s], total count: ${transactionCount.get()}.")
      lastCount.set(count)
    }
  }

}
