// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.util.concurrent.Executors

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.BufferSlice.Prefix
import com.daml.platform.store.cache.EventsBuffer.{RequestOffBufferBounds, UnorderedException}
import org.scalatest.Succeeded
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.Searching.{Found, InsertionPoint}
import scala.collection.immutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

class EventsBufferSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  private val BufferElements = Vector(2, 3, 5, 8, 13).map(idx => idx -> idx * 2)
  private val LastOffset = BufferElements.last._1

  "push" when {
    "max buffer size reached" should {
      "drop oldest" in withBuffer(3L) { buffer =>
        buffer.slice(0, LastOffset) shouldBe Prefix(BufferElements.drop(2))
        buffer.push(21, 42)
        buffer.slice(0, 21) shouldBe Prefix(BufferElements.drop(3) :+ 21 -> 42)
      }
    }

    "element with smaller offset added" should {
      "throw" in withBuffer(3L) { buffer =>
        intercept[UnorderedException[Int]] {
          buffer.push(1, 2)
        }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: 13 vs 1"
      }
    }

    "element with equal offset added" should {
      "throw" in withBuffer(3L) { buffer =>
        intercept[UnorderedException[Int]] {
          buffer.push(13, 2)
        }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: 13 vs 13"
      }
    }

    "range end with equal offset added" should {
      "accept it" in withBuffer(3L) { buffer =>
        buffer.push(13, Int.MaxValue)
        buffer.slice(0, 13) shouldBe Prefix(BufferElements.drop(2))
      }
    }

    "range end with greater offset added" should {
      "not allow new element with lower offset" in withBuffer(3) { buffer =>
        buffer.push(15, Int.MaxValue)
        buffer.slice(0, 13) shouldBe Prefix(BufferElements.drop(2))
        intercept[UnorderedException[Int]] {
          buffer.push(14, 28)
        }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: 15 vs 14"
      }
    }
  }

  "getEvents" when {
    "called with inclusive range" should {
      "return the full buffer contents" in withBuffer() { buffer =>
        buffer.slice(0, 13) shouldBe Prefix(BufferElements)
      }
    }

    "called with range end before buffer range" should {
      "not include elements past the requested end inclusive" in withBuffer() { buffer =>
        buffer.slice(0, 12) shouldBe Prefix(BufferElements.dropRight(1))
        buffer.slice(0, 8) shouldBe Prefix(BufferElements.dropRight(1))
      }
    }

    "called with range start exclusive past the buffer start range" in withBuffer() { buffer =>
      buffer.slice(4, 13) shouldBe BufferSlice.Inclusive(BufferElements.drop(2))
      buffer.slice(5, 13) shouldBe BufferSlice.Inclusive(BufferElements.drop(3))
    }

    "called with endInclusive exceeding buffer range" should {
      "fail with exception" in withBuffer() { buffer =>
        intercept[RequestOffBufferBounds[Int]] {
          buffer.slice(4, 15)
        }.getMessage shouldBe s"Request endInclusive (15) is higher than bufferEnd (13)"
      }
    }

    "called after push  from a different thread" should {
      "always see the most recent updates" in withBuffer(1000, Vector.empty) { buffer =>
        (0 until 1000).foreach(idx => buffer.push(idx, idx)) // fill buffer to max size

        val pushExecutor, sliceExecutor =
          ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

        (0 until 1000).foreach { idx =>
          val expected = ((idx + 901) to (1000 + idx)).map(idx => idx -> idx)

          implicit val ec: ExecutionContextExecutorService = pushExecutor

          Await.result(
            // Simulate different thread accesses for push/slice
            awaitable = {
              for {
                _ <- Future(buffer.push(1000 + idx, 1000 + idx))(pushExecutor)
                _ <- Future(buffer.slice(900 + idx, 1000 + idx))(sliceExecutor)
                  .map(_.slice should contain theSameElementsInOrderAs expected)(sliceExecutor)
              } yield Succeeded
            },
            atMost = 1.seconds,
          )
        }
        Succeeded
      }
    }
  }

  "prune" when {
    "element found" should {
      "prune inclusive" in withBuffer() { buffer =>
        buffer.prune(5)
        buffer.slice(0, LastOffset) shouldBe Prefix(Vector(8 -> 16, 13 -> 26))
      }
    }

    "element not present" should {
      "prune inclusive" in withBuffer() { buffer =>
        buffer.prune(6)
        buffer.slice(0, LastOffset) shouldBe Prefix(Vector(8 -> 16, 13 -> 26))
      }
    }

    "element before series" should {
      "not prune" in withBuffer() { buffer =>
        buffer.prune(1)
        buffer.slice(0, LastOffset) shouldBe Prefix(BufferElements)
      }
    }

    "element after series" should {
      "prune all" in withBuffer() { buffer =>
        buffer.prune(15)
        buffer.slice(0, LastOffset) shouldBe BufferSlice.Empty
      }
    }

    "one element in buffer" should {
      "prune all" in withBuffer(1, Vector(1 -> 2)) { buffer =>
        buffer.prune(1)
        buffer.slice(0, 1) shouldBe BufferSlice.Empty
      }
    }
  }

  "binarySearch" should {
    import EventsBuffer.SearchableByVector
    val series = Vector(9, 10, 13).map(el => el -> el.toString)

    "work on singleton series" in {
      Vector(7).searchBy(5, identity) shouldBe InsertionPoint(0)
      Vector(7).searchBy(7, identity) shouldBe Found(0)
      Vector(7).searchBy(8, identity) shouldBe InsertionPoint(1)
    }

    "work on non-empty series" in {
      series.searchBy(8, _._1) shouldBe InsertionPoint(0)
      series.searchBy(10, _._1) shouldBe Found(1)
      series.searchBy(12, _._1) shouldBe InsertionPoint(2)
      series.searchBy(13, _._1) shouldBe Found(2)
      series.searchBy(14, _._1) shouldBe InsertionPoint(3)
    }

    "work on empty series" in {
      Vector.empty[Int].searchBy(1337, identity) shouldBe InsertionPoint(0)
    }
  }

  private def withBuffer(
      maxBufferSize: Long = 5L,
      elems: immutable.Vector[(Int, Int)] = BufferElements,
  )(test: EventsBuffer[Int, Int] => Assertion): Assertion = {
    val buffer = new EventsBuffer[Int, Int](
      maxBufferSize,
      new Metrics(new MetricRegistry),
      "integers",
      _ == Int.MaxValue, // Signifies ledger end
    )
    elems.foreach { case (offset, event) => buffer.push(offset, event) }
    test(buffer)
  }
}
