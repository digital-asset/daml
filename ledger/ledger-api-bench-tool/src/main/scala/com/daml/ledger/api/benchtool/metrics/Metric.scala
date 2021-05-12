// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import com.google.protobuf.timestamp.Timestamp

trait Metric[T] {

  def onNext(value: T): Unit

  def periodicUpdate(): String

  def completeInfo(totalDurationSeconds: Double): List[String]

  def name: String = getClass.getSimpleName

}

// TODO: add thread safety
object Metric {
  private def rounded(value: Double): String = "%.2f".format(value)

  case class TransactionCountMetric[T](periodMillis: Long, countingFunction: T => Int)
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
      s"rate: ${rounded(rate)} [tx/s], count: $count [tx]"
    }

    override def completeInfo(totalDurationSeconds: Double): List[String] = {
      val count = counter.get()
      val rate = count * 1000.0 / periodMillis
      List(
        s"rate: ${rounded(rate)} [tx/s]",
        s"count: $count [tx]",
      )
    }
  }

  case class TransactionSizeMetric[T](periodMillis: Long, sizingFunction: T => Int)
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
      s"size rate (interval): ${rounded(sizeRate)} [MB/s]"
    }

    override def completeInfo(totalDurationSeconds: Double): List[String] = {
      val sizeRate: String =
        if (sizeRateList.nonEmpty) s"${rounded(sizeRateList.sum / sizeRateList.length)}"
        else "not available"
      List(s"size rate: $sizeRate [MB/s]")
    }
  }

  case class ConsumptionDelayMetric[T](recordTimeFunction: T => Seq[Timestamp]) extends Metric[T] {

    private val delaysInCurrentInterval: ListBuffer[Duration] = ListBuffer.empty

    override def onNext(value: T): Unit = {
      val now = Instant.now()
      recordTimeFunction(value).foreach { recordTime =>
        val delay = Duration.between(
          Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong),
          now,
        )
        delaysInCurrentInterval.append(delay)
      }
    }

    override def periodicUpdate(): String = {
      val meanDelay: Option[Duration] =
        if (delaysInCurrentInterval.nonEmpty)
          Some(
            delaysInCurrentInterval
              .reduceLeft(_.plus(_))
              .dividedBy(delaysInCurrentInterval.length.toLong)
          )
        else None
      delaysInCurrentInterval.clear()
      s"mean delay (interval): ${meanDelay.map(_.getSeconds.toString).getOrElse("-")} [s]"
    }

    override def completeInfo(totalDurationSeconds: Double): List[String] = Nil

  }

  case class ConsumptionSpeedMetric[T](periodMillis: Long, recordTimeFunction: T => Seq[Timestamp])
      extends Metric[T] {

    private var firstRecordTime: Option[Instant] = None
    private var lastRecordTime: Option[Instant] = None

    override def onNext(value: T): Unit = {
      val recordTimes = recordTimeFunction(value)
      if (firstRecordTime.isEmpty) {
        firstRecordTime = recordTimes.headOption.map { recordTime =>
          Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong)
        }
      }
      lastRecordTime = recordTimes.lastOption.map { recordTime =>
        Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong)
      }
    }

    override def periodicUpdate(): String = {
      val speed: Option[Double] = (firstRecordTime, lastRecordTime) match {
        case (Some(first), Some(last)) =>
          Some((last.toEpochMilli - first.toEpochMilli) * 1.0 / periodMillis)
        case _ =>
          None
      }
      firstRecordTime = None
      lastRecordTime = None
      s"speed (interval): ${speed.map(rounded).getOrElse("-")} [-]"
    }

    override def completeInfo(totalDurationSeconds: Double): List[String] = Nil
  }
}
