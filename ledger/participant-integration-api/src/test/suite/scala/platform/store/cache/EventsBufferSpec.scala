// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.util.concurrent.Executors
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.BufferSlice.{Inclusive, LastBufferChunkSuffix}
import com.daml.platform.store.cache.EventsBuffer.{RequestOffBufferBounds, UnorderedException}
import org.scalatest.Succeeded
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.Searching.{Found, InsertionPoint}
import scala.collection.{View, immutable}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

class EventsBufferSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  private val offsetIdx = Vector(2, 4, 6, 8, 10)
  private val BeginOffset = offset(0L)
  private val offsets @ Seq(offset1, offset2, offset3, offset4, offset5) =
    offsetIdx.map(i => offset(i.toLong))
  private val bufferElements @ Seq(entry1, entry2, entry3, entry4) =
    offsets.zip(offsetIdx.map(_ * 2)).take(4)

  private val LastOffset = offset4
  private val IdentityFilter: Int => Option[Int] = Some(_)

  "push" when {
    "max buffer size reached" should {
      "drop oldest" in withBuffer(3) { buffer =>
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
          bufferedStartExclusive = offset2,
          slice = Vector(entry3, entry4),
        )
        buffer.push(offset5, 21)
        buffer.slice(BeginOffset, offset5, IdentityFilter) shouldBe LastBufferChunkSuffix(
          bufferedStartExclusive = offset3,
          slice = Vector(entry4, offset5 -> 21),
        )
      }
    }

    "element with smaller offset added" should {
      "throw" in withBuffer(3) { buffer =>
        intercept[UnorderedException[Int]] {
          buffer.push(offset1, 2)
        }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: $offset4 vs $offset1"
      }
    }

    "element with equal offset added" should {
      "throw" in withBuffer(3) { buffer =>
        intercept[UnorderedException[Int]] {
          buffer.push(offset4, 2)
        }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: $offset4 vs $offset4"
      }
    }

    "range end with equal offset added" should {
      "accept it" in withBuffer(3) { buffer =>
        buffer.push(LastOffset, Int.MaxValue)
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
          bufferedStartExclusive = offset2,
          slice = Vector(entry3, entry4),
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
        Vector(entry2, entry4)
      )
    }

    "called with startExclusive gteq than the buffer start" should {
      "return an Inclusive slice" in withBuffer() { buffer =>
        buffer.slice(offset1, succ(offset3), IdentityFilter) shouldBe BufferSlice.Inclusive(
          Vector(entry2, entry3)
        )
        buffer.slice(offset1, offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
          Vector(entry2, entry3, entry4)
        )
        buffer.slice(succ(offset1), offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
          Vector(entry2, entry3, entry4)
        )
      }

      "return an Inclusive chunk result if resulting slice is bigger than maxFetchSize" in withBuffer(
        maxFetchSize = 2
      ) { buffer =>
        buffer.slice(offset1, offset4, IdentityFilter) shouldBe Inclusive(
          Vector(entry2, entry3)
        )
      }
    }

    "called with endInclusive lteq startExclusive" should {
      "return an empty Inclusive slice if startExclusive is gteq buffer start" in withBuffer() {
        buffer =>
          buffer.slice(offset1, offset1, IdentityFilter) shouldBe Inclusive(Vector.empty)
          buffer.slice(offset2, offset1, IdentityFilter) shouldBe Inclusive(Vector.empty)
      }
      "return an empty LastBufferChunkSuffix slice if startExclusive is before buffer start" in withBuffer(
        maxBufferSize = 2
      ) { buffer =>
        buffer.slice(offset1, offset1, IdentityFilter) shouldBe LastBufferChunkSuffix(
          offset1,
          Vector.empty,
        )
        buffer.slice(offset2, offset1, IdentityFilter) shouldBe LastBufferChunkSuffix(
          offset1,
          Vector.empty,
        )
      }
    }

    "called with startExclusive before the buffer start" should {
      "return a LastBufferChunkSuffix slice" in withBuffer() { buffer =>
        buffer.slice(Offset.beforeBegin, offset3, IdentityFilter) shouldBe LastBufferChunkSuffix(
          offset1,
          Vector(entry2, entry3),
        )
        buffer.slice(
          Offset.beforeBegin,
          succ(offset3),
          IdentityFilter,
        ) shouldBe LastBufferChunkSuffix(
          offset1,
          Vector(entry2, entry3),
        )
      }

      "return a the last filtered chunk as LastBufferChunkSuffix slice if resulting slice is bigger than maxFetchSize" in withBuffer(
        maxFetchSize = 2
      ) { buffer =>
        buffer.slice(Offset.beforeBegin, offset4, IdentityFilter) shouldBe LastBufferChunkSuffix(
          offset2,
          Vector(entry3, entry4),
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
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
          offset4,
          bufferElements.drop(4),
        )
      }
    }

    "element not present" should {
      "prune inclusive" in withBuffer() { buffer =>
        buffer.prune(offset(6))
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
          offset4,
          bufferElements.drop(4),
        )
      }
    }

    "element before series" should {
      "not prune" in withBuffer() { buffer =>
        buffer.prune(offset(1))
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
          offset1,
          bufferElements.drop(1),
        )
      }
    }

    "element after series" should {
      "prune all" in withBuffer() { buffer =>
        buffer.prune(offset5)
        buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
          LastOffset,
          Vector.empty,
        )
      }
    }

    "one element in buffer" should {
      "prune all" in withBuffer(1, Vector(offset(1) -> 2)) { buffer =>
        buffer.prune(offset(1))
        buffer.slice(BeginOffset, offset(1), IdentityFilter) shouldBe LastBufferChunkSuffix(
          offset(1),
          Vector.empty,
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

  "indexAfter" should {
    "yield the index gt the searched entry" in {
      EventsBuffer.indexAfter(InsertionPoint(3)) shouldBe 3
      EventsBuffer.indexAfter(Found(3)) shouldBe 4
    }
  }

  "filterAndChunkSlice" should {
    "return an Inclusive result with filter" in {
      val input = Vector(entry1, entry2, entry3, entry4).view

      EventsBuffer.filterAndChunkSlice[Int, Int](
        sliceView = input,
        filter = Option(_).filterNot(_ == entry2._2),
        maxChunkSize = 3,
      ) shouldBe Vector(entry1, entry3, entry4)

      EventsBuffer.filterAndChunkSlice[Int, Int](
        sliceView = View.empty,
        filter = Some(_),
        maxChunkSize = 3,
      ) shouldBe Vector.empty
    }
  }

  "lastFilteredChunk" should {
    val input = Vector(entry1, entry2, entry3, entry4)

    "return a LastBufferChunkSuffix with the last maxChunkSize-sized chunk from the slice with filter" in {
      EventsBuffer.lastFilteredChunk[Int, Int](
        bufferSlice = input,
        filter = Option(_).filterNot(_ == entry2._2),
        maxChunkSize = 1,
      ) shouldBe LastBufferChunkSuffix(entry3._1, Vector(entry4))

      EventsBuffer.lastFilteredChunk[Int, Int](
        bufferSlice = input,
        filter = Option(_).filterNot(_ == entry2._2),
        maxChunkSize = 2,
      ) shouldBe LastBufferChunkSuffix(entry1._1, Vector(entry3, entry4))

      EventsBuffer.lastFilteredChunk[Int, Int](
        bufferSlice = input,
        filter = Option(_).filterNot(_ == entry2._2),
        maxChunkSize = 3,
      ) shouldBe LastBufferChunkSuffix(entry1._1, Vector(entry3, entry4))

      EventsBuffer.lastFilteredChunk[Int, Int](
        bufferSlice = input,
        filter = Some(_), // No filter
        maxChunkSize = 4,
      ) shouldBe LastBufferChunkSuffix(entry1._1, Vector(entry2, entry3, entry4))
    }

    "use the slice head as bufferedStartExclusive when filter yields an empty result slice" in {
      EventsBuffer.lastFilteredChunk[Int, Int](
        bufferSlice = input,
        filter = _ => None,
        maxChunkSize = 2,
      ) shouldBe LastBufferChunkSuffix(entry1._1, Vector.empty)
    }
  }

  private def withBuffer(
      maxBufferSize: Int = 5,
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
