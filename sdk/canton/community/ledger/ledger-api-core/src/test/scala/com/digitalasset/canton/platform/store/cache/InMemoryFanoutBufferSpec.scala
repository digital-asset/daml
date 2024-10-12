// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.syntax.bifunctor.toBifunctorOps
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer.BufferSlice.LastBufferChunkSuffix
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer.{
  BufferSlice,
  UnorderedException,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.daml.lf.data.Time
import org.scalatest.Succeeded
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.util.concurrent.Executors
import scala.collection.Searching.{Found, InsertionPoint}
import scala.collection.{View, immutable}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

class InMemoryFanoutBufferSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with BaseTest {
  private val offsetIdx = Vector(2, 4, 6, 8, 10)
  private val BeginOffset = offset(0L)
  private val offsets = offsetIdx.map(i => offset(i.toLong))
  private val someDomainId = DomainId.tryFromString("some::domain-id")

  private val IdentityFilter: Traced[TransactionLogUpdate] => Option[TransactionLogUpdate] =
    tracedUpdate => Some(tracedUpdate.value)

  inside(offsets) { case Seq(offset1, offset2, offset3, offset4, offset5) =>
    val txAccepted1 = Traced(txAccepted(1L, offset1))
    val txAccepted2 = Traced(txAccepted(2L, offset2))
    val txAccepted3 = Traced(txAccepted(3L, offset3))
    val txAccepted4 = Traced(txAccepted(4L, offset4))
    val bufferValues = Seq(txAccepted1, txAccepted2, txAccepted3, txAccepted4)
    val txAccepted5 = Traced(txAccepted(5L, offset5))
    val bufferElements = offsets.zip(bufferValues)
    val LastOffset = offset4

    inside(bufferElements) { case Seq(entry1, entry2, entry3, entry4) =>
      "push" when {
        "max buffer size reached" should {
          "drop oldest" in withBuffer(3) { buffer =>
            // Assert data structure sizes
            buffer._bufferLog.size shouldBe 3
            buffer._lookupMap.size shouldBe 3

            buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              bufferedStartExclusive = offset2,
              slice = Vector(entry3, entry4).map(untrace),
            )

            // Assert that all the entries are visible by lookup
            verifyLookupPresent(buffer, txAccepted2, txAccepted3, txAccepted4)

            buffer.push(offset5, txAccepted5)
            // Assert data structure sizes respect their limits after pushing a new element
            buffer._bufferLog.size shouldBe 3
            buffer._lookupMap.size shouldBe 3

            buffer.slice(BeginOffset, offset5, IdentityFilter) shouldBe LastBufferChunkSuffix(
              bufferedStartExclusive = offset3,
              slice = Vector(entry4, offset5 -> txAccepted5).map(untrace),
            )

            // Assert that the new entry is visible by lookup
            verifyLookupPresent(buffer, txAccepted5)
            // Assert oldest entry is evicted
            verifyLookupAbsent(buffer, txAccepted2)
          }
        }

        "element with smaller offset added" should {
          "throw" in withBuffer(3) { buffer =>
            intercept[UnorderedException[Int]] {
              buffer.push(offset1, txAccepted2)
            }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: $offset4 vs $offset1"
          }
        }

        "element with equal offset added" should {
          "throw" in withBuffer(3) { buffer =>
            intercept[UnorderedException[Int]] {
              buffer.push(offset4, txAccepted2)
            }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: $offset4 vs $offset4"
          }
        }

        "maxBufferSize is 0" should {
          "not enqueue the update" in withBuffer(0) { buffer =>
            buffer.push(offset5, txAccepted5)
            buffer.slice(BeginOffset, offset5, IdentityFilter) shouldBe LastBufferChunkSuffix(
              bufferedStartExclusive = offset5,
              slice = Vector.empty,
            )
            buffer._bufferLog shouldBe empty
          }
        }

        "maxBufferSize is -1" should {
          "not enqueue the update" in withBuffer(-1) { buffer =>
            buffer.push(offset5, txAccepted5)
            buffer.slice(BeginOffset, offset5, IdentityFilter) shouldBe LastBufferChunkSuffix(
              bufferedStartExclusive = offset5,
              slice = Vector.empty,
            )
            buffer._bufferLog shouldBe empty
          }
        }

        s"does not update the lookupMap with ${TransactionLogUpdate.TransactionRejected.getClass.getSimpleName}" in withBuffer(
          4
        ) { buffer =>
          // Assert that all the entries are visible by lookup
          verifyLookupPresent(buffer, txAccepted1, txAccepted2, txAccepted3, txAccepted4)

          // Enqueue a rejected transaction
          buffer.push(offset5, Traced(txRejected(5L, offset5)))

          // Assert the last element is evicted on full buffer
          verifyLookupAbsent(buffer, txAccepted1)

          // Assert that the buffer does not include the rejected transaction
          buffer._lookupMap should contain theSameElementsAs Map(
            txAccepted2.value.updateId -> txAccepted2,
            txAccepted3.value.updateId -> txAccepted3,
            txAccepted4.value.updateId -> txAccepted4,
          )
        }
      }

      "slice" when {
        "filters" in withBuffer() { buffer =>
          buffer.slice(offset1, offset4, Some(_).filterNot(_ == entry3._2)) shouldBe BufferSlice
            .Inclusive(
              Vector(entry2, entry4)
            )
        }

        "called with startExclusive gteq than the buffer start" should {
          "return an Inclusive slice" in withBuffer() { buffer =>
            buffer.slice(offset1, succ(offset3), IdentityFilter) shouldBe BufferSlice.Inclusive(
              Vector(entry2, entry3).map(untrace)
            )
            buffer.slice(offset1, offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
              Vector(entry2, entry3, entry4).map(untrace)
            )
            buffer.slice(succ(offset1), offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
              Vector(entry2, entry3, entry4).map(untrace)
            )
          }

          "return an Inclusive chunk result if resulting slice is bigger than maxFetchSize" in withBuffer(
            maxFetchSize = 2
          ) { buffer =>
            buffer.slice(offset1, offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
              Vector(entry2, entry3).map(untrace)
            )
          }
        }

        "called with endInclusive lteq startExclusive" should {
          "return an empty Inclusive slice if startExclusive is gteq buffer start" in withBuffer() {
            buffer =>
              buffer.slice(offset1, offset1, IdentityFilter) shouldBe BufferSlice.Inclusive(
                Vector.empty
              )
              buffer.slice(offset2, offset1, IdentityFilter) shouldBe BufferSlice.Inclusive(
                Vector.empty
              )
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
            buffer.slice(
              Offset.beforeBegin,
              offset3,
              IdentityFilter,
            ) shouldBe LastBufferChunkSuffix(
              offset1,
              Vector(entry2, entry3).map(untrace),
            )
            buffer.slice(
              Offset.beforeBegin,
              succ(offset3),
              IdentityFilter,
            ) shouldBe LastBufferChunkSuffix(
              offset1,
              Vector(entry2, entry3).map(untrace),
            )
          }

          "return a the last filtered chunk as LastBufferChunkSuffix slice if resulting slice is bigger than maxFetchSize" in withBuffer(
            maxFetchSize = 2
          ) { buffer =>
            buffer.slice(
              Offset.beforeBegin,
              offset4,
              IdentityFilter,
            ) shouldBe LastBufferChunkSuffix(
              offset2,
              Vector(entry3, entry4).map(untrace),
            )
          }
        }

        "called after push from a different thread" should {
          "always see the most recent updates" in withBuffer(
            1000,
            Vector.empty,
            maxFetchSize = 1000,
          ) { buffer =>
            (0 until 1000).foreach { idx =>
              val updateOffset = offset(idx.toLong)
              buffer.push(updateOffset, Traced(txAccepted(idx.toLong, updateOffset)))
            } // fill buffer to max size

            val pushExecutor, sliceExecutor =
              ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

            (0 until 1000).foreach { idx =>
              val expected = ((idx + 901) to (1000 + idx)).map { idx =>
                val updateOffset = offset(idx.toLong)
                updateOffset -> txAccepted(idx.toLong, updateOffset)
              }

              implicit val ec: ExecutionContextExecutorService = pushExecutor

              Await.result(
                // Simulate different thread accesses for push/slice
                awaitable = {
                  val lastInsertedIdx = (1000 + idx).toLong
                  val updateOffset = offset(lastInsertedIdx)
                  for {
                    _ <- Future(
                      buffer.push(updateOffset, Traced(txAccepted(lastInsertedIdx, updateOffset)))
                    )(
                      pushExecutor
                    )
                    _ <- Future(
                      buffer.slice(
                        offset((900 + idx).toLong),
                        offset(lastInsertedIdx),
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
            verifyLookupPresent(buffer, txAccepted1, txAccepted2, txAccepted3, txAccepted4)

            buffer.prune(offset3)

            buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset4,
              bufferElements.drop(4),
            )

            verifyLookupAbsent(
              buffer,
              txAccepted1,
              txAccepted2,
              txAccepted3,
            )
            verifyLookupPresent(buffer, txAccepted4)
          }
        }

        "element not present" should {
          "prune inclusive" in withBuffer() { buffer =>
            verifyLookupPresent(buffer, txAccepted1, txAccepted2, txAccepted3, txAccepted4)

            buffer.prune(offset(6))
            buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset4,
              bufferElements.drop(4),
            )
            verifyLookupAbsent(
              buffer,
              txAccepted1,
              txAccepted2,
              txAccepted3,
            )
            verifyLookupPresent(buffer, txAccepted4)
          }
        }

        "element before series" should {
          "not prune" in withBuffer() { buffer =>
            verifyLookupPresent(buffer, txAccepted1, txAccepted2, txAccepted3, txAccepted4)

            buffer.prune(offset(1))
            buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset1,
              bufferElements.drop(1).map(untrace),
            )

            verifyLookupPresent(buffer, txAccepted1, txAccepted2, txAccepted3, txAccepted4)
          }
        }

        "element after series" should {
          "prune all" in withBuffer() { buffer =>
            verifyLookupPresent(buffer, txAccepted1, txAccepted2, txAccepted3, txAccepted4)

            buffer.prune(offset5)
            buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              LastOffset,
              Vector.empty,
            )

            verifyLookupAbsent(
              buffer,
              txAccepted1,
              txAccepted2,
              txAccepted3,
              txAccepted4,
            )
          }
        }

        "one element in buffer" should {
          "prune all" in withBuffer(1, Vector(offset(1) -> txAccepted2)) { buffer =>
            verifyLookupPresent(buffer, txAccepted2)

            buffer.prune(offset(1))
            buffer.slice(BeginOffset, offset(1), IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset(1),
              Vector.empty,
            )

            verifyLookupAbsent(buffer, txAccepted2)
          }
        }
      }

      "flush" should {
        "remove all entries from the buffer" in withBuffer(3) { buffer =>
          verifyLookupPresent(buffer, txAccepted2, txAccepted3, txAccepted4)

          buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
            bufferedStartExclusive = offset2,
            slice = Vector(entry3, entry4).map(untrace),
          )

          buffer.flush()

          buffer._bufferLog shouldBe Vector.empty[(Offset, Int)]
          buffer._lookupMap shouldBe Map.empty
          buffer.slice(BeginOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
            bufferedStartExclusive = LastOffset,
            slice = Vector.empty,
          )
          verifyLookupAbsent(
            buffer,
            txAccepted2,
            txAccepted3,
            txAccepted4,
          )
        }
      }

      "indexAfter" should {
        "yield the index gt the searched entry" in {
          InMemoryFanoutBuffer.indexAfter(InsertionPoint(3)) shouldBe 3
          InMemoryFanoutBuffer.indexAfter(Found(3)) shouldBe 4
        }
      }

      "filterAndChunkSlice" should {
        "return an Inclusive result with filter" in {
          val input = Vector(entry1, entry2, entry3, entry4).view

          InMemoryFanoutBuffer.filterAndChunkSlice[TransactionLogUpdate](
            sliceView = input,
            filter = tracedUpdate => Option(tracedUpdate.value).filterNot(_ == entry2._2.value),
            maxChunkSize = 3,
          ) shouldBe Vector(entry1, entry3, entry4).map(untrace)

          InMemoryFanoutBuffer.filterAndChunkSlice[TransactionLogUpdate](
            sliceView = View.empty,
            filter = tu => Some(tu.value),
            maxChunkSize = 3,
          ) shouldBe Vector.empty
        }
      }

      "lastFilteredChunk" should {
        val input = Vector(entry1, entry2, entry3, entry4)

        "return a LastBufferChunkSuffix with the last maxChunkSize-sized chunk from the slice with filter" in {
          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = tu => Option(tu.value).filterNot(_ == entry2._2.value),
            maxChunkSize = 1,
          ) shouldBe LastBufferChunkSuffix(entry3._1, Vector(entry4).map(untrace))

          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = tu => Option(tu.value).filterNot(_ == entry2._2.value),
            maxChunkSize = 2,
          ) shouldBe LastBufferChunkSuffix(entry1._1, Vector(entry3, entry4).map(untrace))

          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = tu => Option(tu.value).filterNot(_ == entry2._2.value),
            maxChunkSize = 3,
          ) shouldBe LastBufferChunkSuffix(entry1._1, Vector(entry3, entry4).map(untrace))

          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = tu => Some(tu.value), // No filter
            maxChunkSize = 4,
          ) shouldBe LastBufferChunkSuffix(entry1._1, Vector(entry2, entry3, entry4).map(untrace))
        }

        "use the slice head as bufferedStartExclusive when filter yields an empty result slice" in {
          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = _ => None,
            maxChunkSize = 2,
          ) shouldBe LastBufferChunkSuffix(entry1._1, Vector.empty)
        }
      }
    }

    def withBuffer(
        maxBufferSize: Int = 5,
        elems: immutable.Vector[(Offset, Traced[TransactionLogUpdate])] = bufferElements,
        maxFetchSize: Int = 10,
    )(test: InMemoryFanoutBuffer => Assertion): Assertion = {
      val buffer = new InMemoryFanoutBuffer(
        maxBufferSize,
        LedgerApiServerMetrics.ForTesting,
        maxBufferedChunkSize = maxFetchSize,
        loggerFactory = loggerFactory,
      )
      elems.foreach { case (offset, event) => buffer.push(offset, event) }
      test(buffer)
    }
  }

  private def offset(idx: Long): Offset = {
    val base = BigInt(1L) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }

  private def succ(offset: Offset): Offset = {
    val bigInt = BigInt(offset.toByteArray)
    Offset.fromByteArray((bigInt + 1).toByteArray)
  }

  private def txAccepted(idx: Long, offset: Offset) =
    TransactionLogUpdate.TransactionAccepted(
      updateId = s"tx-$idx",
      workflowId = s"workflow-$idx",
      effectiveAt = Time.Timestamp.Epoch,
      offset = offset,
      events = Vector.empty,
      completionStreamResponse = None,
      commandId = "",
      domainId = someDomainId.toProtoPrimitive,
      recordTime = Time.Timestamp.Epoch,
    )

  private def txRejected(idx: Long, offset: Offset) =
    TransactionLogUpdate.TransactionRejected(
      offset = offset,
      completionStreamResponse = CompletionStreamResponse.defaultInstance.withCompletion(
        Completion(
          actAs = Seq(s"submitter-$idx")
        )
      ),
    )

  private def verifyLookupPresent(
      buffer: InMemoryFanoutBuffer,
      txs: Traced[TransactionLogUpdate.TransactionAccepted]*
  ): Assertion =
    txs.foldLeft(succeed) {
      case (Succeeded, tx) =>
        buffer.lookup(tx.value.updateId) shouldBe Some(tx)
      case (failed, _) => failed
    }

  private def verifyLookupAbsent(
      buffer: InMemoryFanoutBuffer,
      txs: Traced[TransactionLogUpdate.TransactionAccepted]*
  ): Assertion =
    txs.foldLeft(succeed) {
      case (Succeeded, Traced(tx)) =>
        buffer.lookup(tx.updateId) shouldBe None
      case (failed, _) => failed
    }

  private def untrace(
      offsetWithTracedUpdate: (Offset, Traced[TransactionLogUpdate])
  ): (Offset, TransactionLogUpdate) =
    offsetWithTracedUpdate.bimap(identity, _.value)
}
