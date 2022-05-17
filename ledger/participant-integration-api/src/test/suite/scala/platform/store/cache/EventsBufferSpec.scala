// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.util.concurrent.Executors
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.BufferSlice.{Inclusive, Suffix}
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
  private val offsetIdx = Vector(2, 4, 6, 8, 10)
  private val BeginOffset = offset(0L)
  private val offsets @ Seq(offset1, offset2, offset3, offset4, offset5) =
    offsetIdx.map(i => offset(i.toLong))
  private val bufferElements @ Seq(_, entry2, entry3, entry4) =
    offsets.zip(offsetIdx.map(_ * 2)).take(4)

  private val LastOffset = offset4
  private val IdentityFilter: Int => Option[Int] = Some(_)

  "push" when {
    "max buffer size reached" should {
      "drop oldest" in withBuffer(3L) { buffer =>
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe Suffix(
          bufferedStartExclusive = offset2,
          slice = Vector(entry3, entry4),
          continueFrom = None,
        )
        buffer.push(offset5, 21)
        buffer.slice(BeginOffset, offset5, IdentityFilter) shouldBe Suffix(
          bufferedStartExclusive = offset3,
          slice = Vector(entry4, offset5 -> 21),
          continueFrom = None,
        )
      }
    }

    "element with smaller offset added" should {
      "throw" in withBuffer(3L) { buffer =>
        intercept[UnorderedException[Int]] {
          buffer.push(offset1, 2)
        }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: $offset4 vs $offset1"
      }
    }

    "element with equal offset added" should {
      "throw" in withBuffer(3L) { buffer =>
        intercept[UnorderedException[Int]] {
          buffer.push(offset4, 2)
        }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: $offset4 vs $offset4"
      }
    }

    "range end with equal offset added" should {
      "accept it" in withBuffer(3L) { buffer =>
        buffer.push(LastOffset, Int.MaxValue)
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe Suffix(
          bufferedStartExclusive = offset2,
          slice = Vector(entry3, entry4),
          continueFrom = None,
        )
      }
    }

    "range end with greater offset added" should {
      "not allow new element with lower offset" in withBuffer(3) { buffer =>
        buffer.push(offset(15), Int.MaxValue)
        intercept[UnorderedException[Int]] {
          buffer.push(offset(14), 28)
        }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: ${offset(15)} vs ${offset(14)}"
      }
    }
  }

  "slice" when {
    "filters" in withBuffer() { buffer =>
      buffer.slice(offset1, offset4, Some(_).filterNot(_ == entry3._2)) shouldBe Inclusive(
        Vector(entry2, entry4),
        None,
      )
    }

    "called with startExclusive gteq than the buffer start" should {
      "return an Inclusive slice" in withBuffer() { buffer =>
        buffer.slice(offset1, succ(offset3), IdentityFilter) shouldBe BufferSlice.Inclusive(
          Vector(entry2, entry3),
          None,
        )
        buffer.slice(offset1, offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
          Vector(entry2, entry3, entry4),
          None,
        )
        buffer.slice(succ(offset1), offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
          Vector(entry2, entry3, entry4),
          None,
        )
      }

      "return an Inclusive chunk result if resulting slice is bigger than maxFetchSize" in withBuffer(
        maxFetchSize = 2
      ) { buffer =>
        buffer.slice(offset1, offset4, IdentityFilter) shouldBe Inclusive(
          Vector(entry2, entry3),
          Some(offset3),
        )
      }
    }

    "called with endInclusive lteq startExclusive" should {
      "return an empty Inclusive slice if startExclusive is gteq buffer start" in withBuffer() {
        buffer =>
          buffer.slice(offset1, offset1, IdentityFilter) shouldBe Inclusive(Vector.empty, None)
          buffer.slice(offset2, offset1, IdentityFilter) shouldBe Inclusive(Vector.empty, None)
      }
      "return an empty Suffix slice if startExclusive is before buffer start" in withBuffer(
        maxBufferSize = 2L
      ) { buffer =>
        buffer.slice(offset1, offset1, IdentityFilter) shouldBe Suffix(offset1, Vector.empty, None)
        buffer.slice(offset2, offset1, IdentityFilter) shouldBe Suffix(offset1, Vector.empty, None)
      }
    }

    "called with startExclusive before the buffer start" should {
      "return a Suffix slice" in withBuffer() { buffer =>
        buffer.slice(Offset.beforeBegin, offset3, IdentityFilter) shouldBe Suffix(
          offset1,
          Vector(entry2, entry3),
          None,
        )
        buffer.slice(Offset.beforeBegin, succ(offset3), IdentityFilter) shouldBe Suffix(
          offset1,
          Vector(entry2, entry3),
          None,
        )
      }

      "return a chunked Suffix slice if resulting slice is bigger than maxFetchSize" in withBuffer(
        maxFetchSize = 2
      ) { buffer =>
        buffer.slice(Offset.beforeBegin, offset4, IdentityFilter) shouldBe Suffix(
          offset1,
          Vector(entry2, entry3),
          Some(offset3),
        )
      }
    }

    "called with endInclusive exceeding buffer range" should {
      val (toBeBuffered, Vector((notBufferedOffset, _))) = bufferElements.splitAt(3)
      "fail with exception" in withBuffer(elems = toBeBuffered) { buffer =>
        intercept[RequestOffBufferBounds[Int]] {
          buffer.slice(offset3, notBufferedOffset, IdentityFilter)
        }.getMessage shouldBe s"Request endInclusive ($offset4) is higher than bufferEnd ($offset3)"
      }
    }

    "called after push from a different thread" should {
      "always see the most recent updates" in withBuffer(1000, Vector.empty, maxFetchSize = 1000) {
        buffer =>
          (0 until 1000).foreach(idx =>
            buffer.push(offset(idx.toLong), idx)
          ) // fill buffer to max size

          val pushExecutor, sliceExecutor =
            ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

          (0 until 1000).foreach { idx =>
            val expected = ((idx + 901) to (1000 + idx)).map(idx => offset(idx.toLong) -> idx)

            implicit val ec: ExecutionContextExecutorService = pushExecutor

            Await.result(
              // Simulate different thread accesses for push/slice
              awaitable = {
                for {
                  _ <- Future(buffer.push(offset((1000 + idx).toLong), 1000 + idx))(pushExecutor)
                  _ <- Future(
                    buffer.slice(
                      offset((900 + idx).toLong),
                      offset((1000 + idx).toLong),
                      IdentityFilter,
                    )
                  )(
                    sliceExecutor
                  )
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
        buffer.prune(offset3)
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe Suffix(
          offset4,
          bufferElements.drop(4),
          None,
        )
      }
    }

    "element not present" should {
      "prune inclusive" in withBuffer() { buffer =>
        buffer.prune(offset(6))
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe Suffix(
          offset4,
          bufferElements.drop(4),
          None,
        )
      }
    }

    "element before series" should {
      "not prune" in withBuffer() { buffer =>
        buffer.prune(offset(1))
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe Suffix(
          offset1,
          bufferElements.drop(1),
          None,
        )
      }
    }

    "element after series" should {
      "prune all" in withBuffer() { buffer =>
        buffer.prune(offset5)
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe Suffix(
          LastOffset,
          Vector.empty,
          None,
        )
      }
    }

    "one element in buffer" should {
      "prune all" in withBuffer(1, Vector(offset(1) -> 2)) { buffer =>
        buffer.prune(offset(1))
        buffer.slice(BeginOffset, offset(1), IdentityFilter) shouldBe Suffix(
          offset(1),
          Vector.empty,
          None,
        )
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
      elems: immutable.Vector[(Offset, Int)] = bufferElements,
      maxFetchSize: Int = 10,
  )(test: EventsBuffer[Int] => Assertion): Assertion = {
    val buffer = new EventsBuffer[Int](
      maxBufferSize,
      new Metrics(new MetricRegistry),
      "integers",
      _ == Int.MaxValue, // Signifies ledger end
      maxBufferedChunkSize = maxFetchSize,
    )
    elems.foreach { case (offset, event) => buffer.push(offset, event) }
    test(buffer)
  }

  private def offset(idx: Long): Offset = {
    val base = BigInt(1L) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }

  private def succ(offset: Offset): Offset = {
    val bigInt = BigInt(offset.toByteArray)
    Offset.fromByteArray((bigInt + 1).toByteArray)
  }
}
