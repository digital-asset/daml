// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer

trait Metric[T] {

  def onNext(value: T): Unit

  // TODO: state?
  def periodicUpdate(): String

  def completeInfo(totalDurationSeconds: Double): String

}

object Metric {
  // TODO: use this in all places
  private def rounded(value: Double): String = "%.2f".format(value)

  case class TransactionCountingMetric[T](periodMillis: Long, countingFunction: T => Int)
      extends Metric[T] {
    private val counter = new AtomicInteger()
    private val lastCount = new AtomicInteger()

    override def onNext(value: T): Unit = {
      counter.addAndGet(countingFunction(value))
      ()
    }

    override def periodicUpdate(): String = {
      val count = counter.get()
      val rate = (count - lastCount.get()) * 1000.0 / periodMillis
      lastCount.set(counter.get())
      s"total count: $count [tx], rate: $rate [tx/s]"
    }

    override def completeInfo(totalDurationSeconds: Double): String = {
      val count = counter.get()
      val rate = count * 1000.0 / periodMillis
      s"total count: $count [tx], rate: $rate [tx/s]"
    }
  }

  case class TransactionSizingMetric[T](periodMillis: Long, sizingFunction: T => Int)
      extends Metric[T] {
    private val currentSizeBucket = new AtomicInteger()
    private val sizeRateList: ListBuffer[Double] = ListBuffer.empty

    override def onNext(value: T): Unit = {
      currentSizeBucket.addAndGet(sizingFunction(value))
      ()
    }

    override def periodicUpdate(): String = {
      val sizeRate = currentSizeBucket.get() * 1000.0 / periodMillis / 1024 / 1024
      sizeRateList += sizeRate
      currentSizeBucket.set(0)
      s"size rate: $sizeRate [MB/s]"
    }

    override def completeInfo(totalDurationSeconds: Double): String = {
      val sizeRate =
        if (sizeRateList.nonEmpty) s"${rounded(sizeRateList.sum / sizeRateList.length)}"
        else "not available"
      s"size rate: $sizeRate [MB/s]"
    }
  }
}
