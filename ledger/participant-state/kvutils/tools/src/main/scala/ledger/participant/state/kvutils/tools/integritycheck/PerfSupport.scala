// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.text.{DecimalFormat, NumberFormat}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.daml.platform.indexer.poc.PerfSupport._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutorService, Future}

object PerfSupport {

  def everyMillis(millis: Long, afterMillis: Long, runAfterShutdown: Boolean = false)(
      block: => Unit
  ): () => Unit = {
    val ex = new ScheduledThreadPoolExecutor(1)
    val f = ex.scheduleAtFixedRate(() => block, afterMillis, millis, TimeUnit.MILLISECONDS)
    () => {
      f.cancel(true)
      ex.shutdownNow()
      if (runAfterShutdown) block
      ()
    }
  }


  def runOnWorkerWithMetrics[IN, OUT](
      workerEC: ExecutionContextExecutorService,
      counters: Counter*
  )(block: IN => OUT): IN => Future[OUT] = in =>
    Future {
      withMetrics(counters: _*)(block(in))
    }(workerEC)

  case class CountedCounter() extends Counter {
    private var count = 0L
    private var times = 0L

    def add(l: Long): Unit = synchronized {
      count += l
      times += 1
    }

    // returns (all added, times added called)
    def retrieveAndReset: (Long, Long) = synchronized {
      val result = (count, times)
      count = 0
      times = 0
      result
    }

    def retrieveAverage: Option[Long] = {
      val (count, times) = retrieveAndReset
      if (times == 0) None
      else Some(count / times)
    }

  }

  case class OneTenHundredCounter() extends Counter {
    private val tenQueue = mutable.Queue.empty[Long]
    private val hundredQueue = mutable.Queue.empty[Long]
    private var count = 0L
    private var tenCount = 0L
    private var hundredCount = 0L

    def add(l: Long): Unit = synchronized {
      count += l
    }

    def retrieveAndReset: (Long, Long, Long) = {
      val lastCount = synchronized {
        val result = count
        count = 0
        result
      }

      tenQueue += lastCount
      tenCount += lastCount
      val lastTen = if (tenQueue.size > 10) {
        tenCount -= tenQueue.dequeue()
        tenCount
      } else 0

      hundredQueue += lastCount
      hundredCount += lastCount
      val lastHundred = if (hundredQueue.size > 100) {
        hundredCount -= hundredQueue.dequeue()
        hundredCount
      } else 0

      (lastCount, lastTen, lastHundred)
    }
  }

  case class Histogramm(buckets: Vector[Long]) extends Counter {
    assert(buckets == buckets.sorted)
    private val size = buckets.size
    private val acc = Array.fill(size + 1)(0)

    def add(l: Long): Unit = {
      buckets.view.zipWithIndex.find(_._1 > l) match {
        case Some((_, i)) =>
          synchronized {
            acc.update(i, acc(i) + 1)
          }
        case None =>
          synchronized {
            acc.update(size, acc(size) + 1)
          }
      }
    }

    def retrieveAndReset: Vector[Int] = synchronized {
      val r = acc.toVector
      acc.view.indices.foreach(acc.update(_, 0))
      r
    }

  }

  private val nf = NumberFormat.getIntegerInstance.asInstanceOf[DecimalFormat]
  nf.setGroupingUsed(true)
  private val symbols = nf.getDecimalFormatSymbols
  symbols.setGroupingSeparator(' ')
  nf.setDecimalFormatSymbols(symbols)

  implicit class NanoRender(nano: Long) {
    def renderNano: String = {
      val plain = nf.format(nano / 1000)
      s"${plain.padRight(11)}μ"
    }
  }

  implicit class StringHelperOps(val s: String) extends AnyVal {
    def padLeft(size: Int): String = s"$s${pad(s, size)}"
    def padRight(size: Int): String = s"${pad(s, size)}$s"

    private def pad(from: String, to: Int): String =
      if (from.length >= to) ""
      else Iterator.continually(' ').take(to - from.length).mkString
  }

  implicit class ChainOps[A](val in: A) extends AnyVal {
    def pipeIf(cond: A => Boolean)(f: A => A): A =
      if (cond(in)) f(in) else in

    def pipe[B](f: A => B): B = f(in)

    def tap[U](f: A => U): A = {
      f(in)
      in
    }
  }

  case class TimedLogger(marker: String) {
    private val start = System.nanoTime()
    private var last = start
    log("Started")

    def log(s: String): Unit = synchronized {
      val now = System.nanoTime()
      val sinceLast = now - last
      last = now
      println(s"$now Δ ${sinceLast.renderNano} $marker> $s")
    }
  }

  val dtFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
  def now: String = LocalDateTime.now().format(dtFormatter)
  private lazy val mainLogger = TimedLogger("")
  def log(s: String): Unit = mainLogger.log(s)

  implicit class WaitforitOps[T](val f: Future[T]) {
    def waitforit: T = Await.result(f, Duration(100, "hour"))
  }
}
