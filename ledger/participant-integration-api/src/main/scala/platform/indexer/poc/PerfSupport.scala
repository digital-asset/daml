// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.poc

import com.daml.platform.indexer.poc.PerfSupport.{CountedCounter, Histogramm, OneTenHundredCounter}

import scala.collection.mutable

object PerfSupport {

  trait Counter {
    def add(l: Long): Unit
  }

  def withMetrics[T](counters: Counter*)(block: => T): T = {
    val start = System.nanoTime()
    val result = block
    val took = System.nanoTime() - start
    counters.foreach(_.add(took))
    result
  }

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

  implicit class ChainOps[A](val in: A) extends AnyVal {
    def pipeIf(cond: A => Boolean)(f: A => A): A =
      if (cond(in)) f(in) else in

    def pipe[B](f: A => B): B = f(in)

    def tap[U](f: A => U): A = {
      f(in)
      in
    }
  }
}

object StaticMetrics {
  val batchCounter: CountedCounter = CountedCounter()
  val mappingCPU: OneTenHundredCounter = OneTenHundredCounter()
  val seqMappingCPU: OneTenHundredCounter = OneTenHundredCounter()
  val ingestionCPU: OneTenHundredCounter = OneTenHundredCounter()
  val dbCallHistrogram: Histogramm = Histogramm(
    Vector(100000, 1000000, 10000000, 100000000, 1000000000, 10000000000L)
  )
}
