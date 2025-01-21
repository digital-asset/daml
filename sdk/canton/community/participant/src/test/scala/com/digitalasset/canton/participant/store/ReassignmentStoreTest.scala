// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.{CantonTimestamp, Offset, ViewType}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentData.*
import com.digitalasset.canton.participant.protocol.reassignment.{
  IncompleteReassignmentData,
  ReassignmentData,
  ReassignmentDataHelpers,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.store.ReassignmentStore.*
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  suffixedId,
}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  LfContractId,
  ReassignmentId,
  RequestId,
  SerializableContract,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, MonadUtil}
import com.digitalasset.canton.{BaseTest, LfPartyId, RequestCounter, SequencerCounter}
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, EitherValues}

import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait ReassignmentStoreTest {
  this: AsyncWordSpec & BaseTest =>

  import ReassignmentStoreTest.*

  @unused
  private implicit val _ec: ExecutionContext = ec

  private implicit def toOffset(i: Long): Offset = Offset.tryFromLong(i)

  protected def reassignmentStore(mk: IndexedSynchronizer => ReassignmentStore): Unit = {
    val reassignmentData = mkReassignmentData(reassignment10, mediator1)
    val reassignmentData2 = mkReassignmentData(reassignment11, mediator1)
    val reassignmentData3 = mkReassignmentData(reassignment20, mediator1)

    def reassignmentDataFor(
        reassignmentId: ReassignmentId,
        contract: SerializableContract,
        unassignmentGlobalOffset: Option[Offset] = None,
    ): ReassignmentData = mkReassignmentData(
      reassignmentId,
      mediator1,
      contract = contract,
      unassignmentGlobalOffset = unassignmentGlobalOffset,
    )

    val unassignmentResult = mkUnassignmentResult(reassignmentData)
    val withUnassignmentResult =
      reassignmentData.copy(unassignmentResult = Some(unassignmentResult))
    val toc = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(3))

    "lookup" should {
      "find previously stored reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")
          lookup10 <- valueOrFail(store.lookup(reassignment10).failOnShutdown)(
            "lookup failed to find the stored reassignment"
          )
        } yield assert(lookup10 == reassignmentData, "lookup finds the stored data")
      }

      "not invent reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")
          lookup10 <- store.lookup(reassignment11).value.failOnShutdown
        } yield assert(
          lookup10 == Left(UnknownReassignmentId(reassignment11)),
          "lookup finds the stored data",
        )
      }
    }

    "find" should {
      "filter by party" in {
        val store = mk(indexedTargetSynchronizer)

        val aliceReassignment = mkReassignmentData(
          reassignment10,
          mediator1,
          LfPartyId.assertFromString("alice"),
        )
        val bobReassignment = mkReassignmentData(
          reassignment11,
          mediator1,
          LfPartyId.assertFromString("bob"),
        )
        val eveReassignment = mkReassignmentData(
          reassignment20,
          mediator2,
          LfPartyId.assertFromString("eve"),
        )

        for {
          _ <- valueOrFail(store.addReassignment(aliceReassignment).failOnShutdown)(
            "add alice failed"
          )
          _ <- valueOrFail(store.addReassignment(bobReassignment).failOnShutdown)("add bob failed")
          _ <- valueOrFail(store.addReassignment(eveReassignment).failOnShutdown)("add eve failed")
          lookup <- store
            .find(None, None, Some(LfPartyId.assertFromString("bob")), 10)
            .failOnShutdown
        } yield {
          assert(lookup.toList == List(bobReassignment))
        }
      }

      "filter by timestamp" in {
        val store = mk(indexedTargetSynchronizer)

        val reassignment1 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer1, CantonTimestamp.ofEpochMilli(100L)),
          mediator1,
        )
        val reassignment2 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer1, CantonTimestamp.ofEpochMilli(200L)),
          mediator1,
        )
        val reassignment3 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer1, CantonTimestamp.ofEpochMilli(300L)),
          mediator1,
        )

        for {

          _ <- valueOrFail(store.addReassignment(reassignment1).failOnShutdown)("add1 failed")
          _ <- valueOrFail(store.addReassignment(reassignment2).failOnShutdown)("add2 failed")
          _ <- valueOrFail(store.addReassignment(reassignment3).failOnShutdown)("add3 failed")
          lookup <- store
            .find(None, Some(CantonTimestamp.Epoch.plusMillis(200L)), None, 10)
            .failOnShutdown
        } yield {
          assert(lookup.toList == List(reassignment2))
        }
      }
      "filter by synchronizer" in {
        val store = mk(indexedTargetSynchronizer)

        val reassignment1 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer1, CantonTimestamp.ofEpochMilli(100L)),
          mediator1,
        )
        val reassignment2 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer2, CantonTimestamp.ofEpochMilli(200L)),
          mediator2,
        )

        for {
          _ <- valueOrFail(store.addReassignment(reassignment1).failOnShutdown)("add1 failed")
          _ <- valueOrFail(store.addReassignment(reassignment2).failOnShutdown)("add2 failed")
          lookup <- store.find(Some(sourceSynchronizer2), None, None, 10).failOnShutdown
        } yield {
          assert(lookup.toList == List(reassignment2))
        }
      }
      "limit the number of results" in {
        val store = mk(indexedTargetSynchronizer)

        val reassignmentData10 = mkReassignmentData(reassignment10, mediator1)
        val reassignmentData11 = mkReassignmentData(reassignment11, mediator1)
        val reassignmentData20 = mkReassignmentData(reassignment20, mediator2)

        for {

          _ <- valueOrFail(store.addReassignment(reassignmentData10).failOnShutdown)(
            "first add failed"
          )
          _ <- valueOrFail(store.addReassignment(reassignmentData11).failOnShutdown)(
            "second add failed"
          )
          _ <- valueOrFail(store.addReassignment(reassignmentData20).failOnShutdown)(
            "third add failed"
          )
          lookup <- store.find(None, None, None, 2).failOnShutdown
        } yield {
          assert(lookup.length == 2)
        }
      }
      "apply filters conjunctively" in {
        val store = mk(indexedTargetSynchronizer)

        // Correct timestamp
        val reassignment1 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer1, CantonTimestamp.Epoch.plusMillis(200L)),
          mediator1,
          LfPartyId.assertFromString("party1"),
        )
        // Correct submitter
        val reassignment2 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer1, CantonTimestamp.Epoch.plusMillis(100L)),
          mediator1,
          LfPartyId.assertFromString("party2"),
        )
        // Correct synchronizer
        val reassignment3 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer2, CantonTimestamp.Epoch.plusMillis(100L)),
          mediator2,
          LfPartyId.assertFromString("party2"),
        )
        // Correct reassignment
        val reassignment4 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer2, CantonTimestamp.Epoch.plusMillis(200L)),
          mediator2,
          LfPartyId.assertFromString("party2"),
        )

        for {

          _ <- valueOrFail(store.addReassignment(reassignment1).failOnShutdown)("first add failed")
          _ <- valueOrFail(store.addReassignment(reassignment2).failOnShutdown)("second add failed")
          _ <- valueOrFail(store.addReassignment(reassignment3).failOnShutdown)("third add failed")
          _ <- valueOrFail(store.addReassignment(reassignment4).failOnShutdown)("fourth add failed")
          lookup <- store
            .find(
              Some(sourceSynchronizer2),
              Some(CantonTimestamp.Epoch.plusMillis(200L)),
              Some(LfPartyId.assertFromString("party2")),
              10,
            )
            .failOnShutdown
        } yield {
          assert(lookup.toList == List(reassignment4))
        }

      }
    }

    "findAfter" should {

      def populate(store: ReassignmentStore): Future[List[ReassignmentData]] = {
        val reassignment1 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer1, CantonTimestamp.Epoch.plusMillis(200L)),
          mediator1,
          LfPartyId.assertFromString("party1"),
        )
        val reassignment2 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer1, CantonTimestamp.Epoch.plusMillis(100L)),
          mediator1,
          LfPartyId.assertFromString("party2"),
        )
        val reassignment3 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer2, CantonTimestamp.Epoch.plusMillis(100L)),
          mediator2,
          LfPartyId.assertFromString("party2"),
        )
        val reassignment4 = mkReassignmentData(
          ReassignmentId(sourceSynchronizer2, CantonTimestamp.Epoch.plusMillis(200L)),
          mediator2,
          LfPartyId.assertFromString("party2"),
        )

        for {
          _ <- valueOrFail(store.addReassignment(reassignment1).failOnShutdown)("first add failed")
          _ <- valueOrFail(store.addReassignment(reassignment2).failOnShutdown)("second add failed")
          _ <- valueOrFail(store.addReassignment(reassignment3).failOnShutdown)("third add failed")
          _ <- valueOrFail(store.addReassignment(reassignment4).failOnShutdown)("fourth add failed")
        } yield (List(reassignment1, reassignment2, reassignment3, reassignment4))
      }

      "order pending reassignments" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          reassignments <- populate(store)
          lookup <- store.findAfter(None, 10).failOnShutdown
        } yield {
          val List(reassignment1, reassignment2, reassignment3, reassignment4) =
            reassignments: @unchecked
          assert(lookup == Seq(reassignment2, reassignment3, reassignment1, reassignment4))
        }

      }
      "give pending reassignments after the given timestamp" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          reassignments <- populate(store)
          List(reassignment1, reassignment2, reassignment3, reassignment4) =
            reassignments: @unchecked
          lookup <- store
            .findAfter(
              requestAfter = Some(
                reassignment2.reassignmentId.unassignmentTs -> reassignment2.sourceSynchronizer
              ),
              10,
            )
            .failOnShutdown
        } yield {
          assert(lookup == Seq(reassignment3, reassignment1, reassignment4))
        }
      }
      "give no pending reassignments when empty" in {
        val store = mk(indexedTargetSynchronizer)
        for { lookup <- store.findAfter(None, 10).failOnShutdown } yield {
          lookup shouldBe empty
        }
      }
      "limit the results" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          reassignments <- populate(store)
          lookup <- store.findAfter(None, 2).failOnShutdown
        } yield {
          val List(_reassignment1, reassignment2, reassignment3, _reassignment4) =
            reassignments: @unchecked
          assert(lookup == Seq(reassignment2, reassignment3))
        }
      }
      "exclude completed reassignments" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          reassignments <- populate(store)
          List(reassignment1, reassignment2, reassignment3, reassignment4) =
            reassignments: @unchecked
          checked <- store
            .completeReassignment(
              reassignment2.reassignmentId,
              TimeOfChange(RequestCounter(3), CantonTimestamp.Epoch.plusSeconds(3)),
            )
            .value
            .failOnShutdown
          lookup <- store.findAfter(None, 10).failOnShutdown
        } yield {
          assert(checked.successful)
          assert(lookup == Seq(reassignment3, reassignment1, reassignment4))
        }

      }
    }

    "add unassignment/in global offsets" should {

      val reassignmentId = reassignmentData.reassignmentId

      val unassignmentOffset = UnassignmentGlobalOffset(10L)
      val assignmentOffset = AssignmentGlobalOffset(15L)

      val reassignmentDataOnlyUnassignment =
        reassignmentData.copy(reassignmentGlobalOffset =
          Some(UnassignmentGlobalOffset(unassignmentOffset.offset))
        )
      val reassignmentDataReassignmentComplete = reassignmentData.copy(reassignmentGlobalOffset =
        Some(
          ReassignmentGlobalOffsets.create(unassignmentOffset.offset, assignmentOffset.offset).value
        )
      )

      "allow batch updates" in {
        val store = mk(indexedTargetSynchronizer)

        val data = (1L until 13).flatMap { i =>
          val tid = reassignmentId.copy(unassignmentTs = CantonTimestamp.ofEpochSecond(i))
          val reassignmentData = reassignmentDataFor(tid, contract)

          val mod = 4

          if (i % mod == 0)
            Seq((UnassignmentGlobalOffset(i * 10), reassignmentData))
          else if (i % mod == 1)
            Seq((AssignmentGlobalOffset(i * 10), reassignmentData))
          else if (i % mod == 2)
            Seq((ReassignmentGlobalOffsets.create(i * 10, i * 10 + 1).value, reassignmentData))
          else
            Seq(
              (UnassignmentGlobalOffset(i * 10), reassignmentData),
              (AssignmentGlobalOffset(i * 10 + 1), reassignmentData),
            )

        }

        for {
          _ <- valueOrFail(MonadUtil.sequentialTraverse_(data) { case (_, reassignmentData) =>
            store.addReassignment(reassignmentData).failOnShutdown
          })("add reassignments")

          offsets = data.map { case (offset, reassignmentData) =>
            reassignmentData.reassignmentId -> offset
          }

          _ <- store.addReassignmentsOffsets(offsets).valueOrFailShutdown("adding offsets")

          result <- valueOrFail(offsets.toList.parTraverse { case (reassignmentId, _) =>
            store.lookup(reassignmentId).failOnShutdown
          })("query reassignments")
        } yield {
          result.lengthCompare(offsets) shouldBe 0

          forEvery(result.zip(offsets)) {
            case (retrievedReassignmentData, (reassignmentId, expectedOffset)) =>
              withClue(s"got unexpected data for reassignment $reassignmentId") {
                expectedOffset match {
                  case UnassignmentGlobalOffset(out) =>
                    retrievedReassignmentData.unassignmentGlobalOffset shouldBe Some(out)

                  case AssignmentGlobalOffset(in) =>
                    retrievedReassignmentData.assignmentGlobalOffset shouldBe Some(in)

                  case ReassignmentGlobalOffsets(out, in) =>
                    retrievedReassignmentData.unassignmentGlobalOffset shouldBe Some(out)
                    retrievedReassignmentData.assignmentGlobalOffset shouldBe Some(in)
                }
              }
          }
        }
      }

      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFailShutdown(
              "add unassignment offset 1"
            )

          lookupOnlyUnassignment1 <- valueOrFail(store.lookup(reassignmentId).failOnShutdown)(
            "lookup reassignment data"
          )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFailShutdown(
              "add unassignment offset 2"
            )

          lookupOnlyUnassignment2 <- valueOrFail(store.lookup(reassignmentId).failOnShutdown)(
            "lookup reassignment data"
          )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFailShutdown(
              "add assignment offset 1"
            )

          lookup1 <- valueOrFail(store.lookup(reassignmentId).failOnShutdown)(
            "lookup reassignment data"
          )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFailShutdown(
              "add assignment offset 2"
            )

          lookup2 <- valueOrFail(store.lookup(reassignmentId).failOnShutdown)(
            "lookup reassignment data"
          )

        } yield {
          lookupOnlyUnassignment1 shouldBe reassignmentDataOnlyUnassignment
          lookupOnlyUnassignment2 shouldBe reassignmentDataOnlyUnassignment

          lookup1 shouldBe reassignmentDataReassignmentComplete
          lookup2 shouldBe reassignmentDataReassignmentComplete
        }
      }

      "return an error if assignment offset is the same as the unassignment" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFailShutdown(
              "add unassignment offset"
            )

          failedAdd <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(unassignmentOffset.offset))
            )
            .value
            .failOnShutdown
        } yield failedAdd.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]
      }

      "return an error if unassignment offset is the same as the assignment" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFailShutdown(
              "add assignment offset"
            )

          failedAdd <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(assignmentOffset.offset))
            )
            .value
            .failOnShutdown
        } yield failedAdd.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]
      }

      "return an error if the new value differs from the old one" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFailShutdown(
              "add unassignment offset 1"
            )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFailShutdown(
              "add unassignment offset 2"
            )

          lookup1 <- valueOrFail(store.lookup(reassignmentId).failOnShutdown)(
            "lookup reassignment data"
          )

          successfulAddOutOffset <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .value
            .failOnShutdown
          failedAddOutOffset <- store
            .addReassignmentsOffsets(
              Map(
                reassignmentId -> UnassignmentGlobalOffset(
                  Offset.tryFromLong(unassignmentOffset.offset.unwrap - 1)
                )
              )
            )
            .value
            .failOnShutdown

          successfulAddInOffset <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .value
            .failOnShutdown
          failedAddInOffset <- store
            .addReassignmentsOffsets(
              Map(
                reassignmentId -> AssignmentGlobalOffset(
                  Offset.tryFromLong(assignmentOffset.offset.unwrap - 1)
                )
              )
            )
            .value
            .failOnShutdown

          lookup2 <- valueOrFail(store.lookup(reassignmentId).failOnShutdown)(
            "lookup reassignment data"
          )

        } yield {
          successfulAddOutOffset.value shouldBe ()
          failedAddOutOffset.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]

          successfulAddInOffset.value shouldBe ()
          failedAddInOffset.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]

          lookup1 shouldBe reassignmentDataReassignmentComplete
          lookup2 shouldBe reassignmentDataReassignmentComplete
        }
      }
    }

    "incomplete" should {
      val limit = NonNegativeInt.tryCreate(10)

      def assertIsIncomplete(
          incompletes: Seq[IncompleteReassignmentData],
          expectedReassignmentData: ReassignmentData,
      ): Assertion =
        incompletes.map(_.toReassignmentData) shouldBe Seq(expectedReassignmentData)

      "list incomplete reassignments (unassignment done)" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = reassignmentData.reassignmentId

        val unassignmentOsset = 10L
        val assignmentOffset = 20L

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")
          lookupNoOffset <- store.findIncomplete(None, Long.MaxValue, None, limit).failOnShutdown

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOsset))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )
          lookupBeforeUnassignment <- store
            .findIncomplete(
              None,
              unassignmentOsset - 1,
              None,
              limit,
            )
            .failOnShutdown
          lookupAtUnassignment <- store
            .findIncomplete(None, unassignmentOsset, None, limit)
            .failOnShutdown

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )

          lookupBeforeAssignment <- store
            .findIncomplete(
              None,
              assignmentOffset - 1,
              None,
              limit,
            )
            .failOnShutdown
          lookupAtAssignment <- store
            .findIncomplete(None, assignmentOffset, None, limit)
            .failOnShutdown
          lookupAfterAssignment <- store
            .findIncomplete(
              None,
              assignmentOffset,
              None,
              limit,
            )
            .failOnShutdown
        } yield {
          lookupNoOffset shouldBe empty

          lookupBeforeUnassignment shouldBe empty

          assertIsIncomplete(
            lookupAtUnassignment,
            reassignmentData.copy(reassignmentGlobalOffset =
              Some(UnassignmentGlobalOffset(unassignmentOsset))
            ),
          )

          assertIsIncomplete(
            lookupBeforeAssignment,
            reassignmentData.copy(reassignmentGlobalOffset =
              Some(UnassignmentGlobalOffset(unassignmentOsset))
            ),
          )

          lookupAtAssignment shouldBe empty
          lookupAfterAssignment shouldBe empty
        }
      }

      "list incomplete reassignments (assignment done)" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = reassignmentData.reassignmentId

        val assignmentOffset = 10L
        val unassignmentOffset = 20L

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")
          lookupNoOffset <- store.findIncomplete(None, Long.MaxValue, None, limit).failOnShutdown

          _ <-
            store
              .addReassignmentsOffsets(
                Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
              )
              .valueOrFailShutdown(
                "add assignment offset failed"
              )
          lookupBeforeAssignment <- store
            .findIncomplete(
              None,
              assignmentOffset - 1,
              None,
              limit,
            )
            .failOnShutdown
          lookupAtAssignment <- store
            .findIncomplete(None, assignmentOffset, None, limit)
            .failOnShutdown

          _ <-
            store
              .addReassignmentsOffsets(
                Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
              )
              .valueOrFailShutdown(
                "add unassignment offset failed"
              )

          lookupBeforeUnassignment <- store
            .findIncomplete(
              None,
              unassignmentOffset - 1,
              None,
              limit,
            )
            .failOnShutdown
          lookupAtUnassignment <- store
            .findIncomplete(None, unassignmentOffset, None, limit)
            .failOnShutdown
          lookupAfterUnassignment <- store
            .findIncomplete(
              None,
              unassignmentOffset,
              None,
              limit,
            )
            .failOnShutdown
        } yield {
          lookupNoOffset shouldBe empty

          lookupBeforeAssignment shouldBe empty

          assertIsIncomplete(
            lookupAtAssignment,
            reassignmentData.copy(reassignmentGlobalOffset =
              Some(AssignmentGlobalOffset(assignmentOffset))
            ),
          )

          assertIsIncomplete(
            lookupBeforeUnassignment,
            reassignmentData.copy(reassignmentGlobalOffset =
              Some(AssignmentGlobalOffset(assignmentOffset))
            ),
          )

          lookupAtUnassignment shouldBe empty
          lookupAfterUnassignment shouldBe empty
        }
      }

      "take stakeholders filter into account" in {
        val store = mk(indexedTargetSynchronizer)

        val alice = ReassignmentStoreTest.alice
        val bob = ReassignmentStoreTest.bob

        val aliceContract = ReassignmentStoreTest.contract(ReassignmentStoreTest.coidAbs1, alice)
        val bobContract = ReassignmentStoreTest.contract(ReassignmentStoreTest.coidAbs2, bob)

        val unassignmentOffset = 42L

        val contracts = Seq(aliceContract, bobContract, aliceContract, bobContract)
        val reassignmentsData = contracts.zipWithIndex.map { case (contract, idx) =>
          val reassignmentId =
            ReassignmentId(sourceSynchronizer1, CantonTimestamp.Epoch.plusSeconds(idx.toLong))

          reassignmentDataFor(
            reassignmentId,
            contract,
            unassignmentGlobalOffset = Some(unassignmentOffset),
          )
        }
        val stakeholders = contracts.map(_.metadata.stakeholders)

        val addReassignmentsET = reassignmentsData.parTraverse(store.addReassignment)

        def lift(stakeholder: LfPartyId, others: LfPartyId*): Option[NonEmpty[Set[LfPartyId]]] =
          Option(NonEmpty(Set, stakeholder, others*))

        def stakeholdersOf(
            incompleteReassignments: Seq[IncompleteReassignmentData]
        ): Seq[Set[LfPartyId]] =
          incompleteReassignments.map(_.contract.metadata.stakeholders)

        for {
          _ <- valueOrFail(addReassignmentsET.failOnShutdown)("add failed")

          lookupNone <- store.findIncomplete(None, unassignmentOffset, None, limit).failOnShutdown
          lookupAll <- store
            .findIncomplete(
              None,
              unassignmentOffset,
              lift(alice, bob),
              limit,
            )
            .failOnShutdown

          lookupAlice <- store
            .findIncomplete(None, unassignmentOffset, lift(alice), limit)
            .failOnShutdown
          lookupBob <- store
            .findIncomplete(None, unassignmentOffset, lift(bob), limit)
            .failOnShutdown
        } yield {
          stakeholdersOf(lookupNone) should contain theSameElementsAs stakeholders
          stakeholdersOf(lookupAll) should contain theSameElementsAs stakeholders
          stakeholdersOf(lookupAlice) should contain theSameElementsAs Seq(Set(alice), Set(alice))
          stakeholdersOf(lookupBob) should contain theSameElementsAs Seq(Set(bob), Set(bob))
        }
      }

      "take synchronizer filter into account" in {
        val store = mk(indexedTargetSynchronizer)
        val offset = 10L

        val reassignment =
          reassignmentData.copy(reassignmentGlobalOffset = Some(AssignmentGlobalOffset(offset)))

        for {
          _ <- valueOrFail(store.addReassignment(reassignment).failOnShutdown)("add")

          lookup1a <- store
            .findIncomplete(Some(sourceSynchronizer2), offset, None, limit)
            .failOnShutdown // Wrong synchronizer
          lookup1b <- store
            .findIncomplete(Some(sourceSynchronizer1), offset, None, limit)
            .failOnShutdown
          lookup1c <- store.findIncomplete(None, offset, None, limit).failOnShutdown
        } yield {
          lookup1a shouldBe empty
          assertIsIncomplete(lookup1b, reassignment)
          assertIsIncomplete(lookup1c, reassignment)
        }
      }

      "limit the results" in {
        val store = mk(indexedTargetSynchronizer)
        val offset = 42L

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add")
          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentData.reassignmentId -> UnassignmentGlobalOffset(offset))
            )
            .valueOrFailShutdown("add out offset")

          lookup0 <- store.findIncomplete(None, offset, None, NonNegativeInt.zero).failOnShutdown
          lookup1 <- store.findIncomplete(None, offset, None, NonNegativeInt.one).failOnShutdown

        } yield {
          lookup0 shouldBe empty
          lookup1 should have size 1
        }
      }
    }

    "find first incomplete" should {

      "find incomplete reassignments (unassignment done)" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = reassignmentData.reassignmentId

        val unassignmentOffset = 10L
        val assignmentOffset = 20L
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )
          lookupAfterUnassignment <- store.findEarliestIncomplete().failOnShutdown

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )

          lookupAfterAssignment <- store.findEarliestIncomplete().failOnShutdown
        } yield {
          inside(lookupAfterUnassignment) { case Some((offset, _, _)) =>
            offset shouldBe Offset.tryFromLong(unassignmentOffset)
          }
          lookupAfterAssignment shouldBe None
        }
      }

      "find incomplete reassignments (assignment done)" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = reassignmentData.reassignmentId

        val unassignmentOffset = 10L
        val assignmentOffset = 20L

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )
          lookupAfterAssignment <- store.findEarliestIncomplete().failOnShutdown

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )
          lookupAfterUnassignment <- store.findEarliestIncomplete().failOnShutdown

        } yield {
          inside(lookupAfterAssignment) { case Some((offset, _, _)) =>
            offset shouldBe Offset.tryFromLong(assignmentOffset)
          }
          lookupAfterUnassignment shouldBe None
        }
      }

      "returns None when reassignment store is empty or each reassignment is either complete or has no offset information" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId1 = reassignmentData.reassignmentId
        val reassignmentId3 = reassignmentData3.reassignmentId

        val unassignmentOffset1 = 10L
        val assignmentOffset1 = 20L

        val unassignmentOffset3 = 30L
        val assignmentOffset3 = 35L

        for {
          lookupEmpty <- store.findEarliestIncomplete().failOnShutdown
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addReassignment(reassignmentData3).failOnShutdown)("add failed")

          lookupAllInFlight <- store.findEarliestIncomplete().failOnShutdown

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId1 -> AssignmentGlobalOffset(assignmentOffset1))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId1 -> UnassignmentGlobalOffset(unassignmentOffset1))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )

          lookupInFlightOrComplete <- store.findEarliestIncomplete().failOnShutdown

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId3 -> AssignmentGlobalOffset(assignmentOffset3))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId3 -> UnassignmentGlobalOffset(unassignmentOffset3))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )
          lookupAllComplete <- store.findEarliestIncomplete().failOnShutdown

        } yield {
          lookupEmpty shouldBe None
          lookupAllInFlight shouldBe None
          lookupInFlightOrComplete shouldBe None
          lookupAllComplete shouldBe None
        }
      }

      "works in complex scenario" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId1 = reassignmentData.reassignmentId
        val reassignmentId2 = reassignmentData2.reassignmentId
        val reassignmentId3 = reassignmentData3.reassignmentId

        val unassignmentOffset1 = 10L
        val assignmentOffset1 = 20L

        // reassignment 2 is incomplete
        val unassignmentOffset2 = 12L

        val unassignmentOffset3 = 30L
        val assignmentOffset3 = 35L

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addReassignment(reassignmentData2).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addReassignment(reassignmentData3).failOnShutdown)("add failed")

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId1 -> AssignmentGlobalOffset(assignmentOffset1))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId1 -> UnassignmentGlobalOffset(unassignmentOffset1))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId2 -> AssignmentGlobalOffset(unassignmentOffset2))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId3 -> AssignmentGlobalOffset(assignmentOffset3))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId3 -> UnassignmentGlobalOffset(unassignmentOffset3))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )

          lookupEnd <- store.findEarliestIncomplete().failOnShutdown

        } yield {
          inside(lookupEnd) { case Some((offset, _, _)) =>
            offset shouldBe Offset.tryFromLong(unassignmentOffset2)
          }
        }
      }
    }

    "addReassignment" should {
      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)(
            "first add failed"
          )
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)(
            "second add failed"
          )
        } yield succeed
      }

      "detect modified reassignment data" in {
        val store = mk(indexedTargetSynchronizer)
        val modifiedContract =
          asSerializable(
            reassignmentData.contract.contractId,
            contractInstance(),
            contract.metadata,
            CantonTimestamp.ofEpochMilli(1),
          )
        val reassignmentDataModified = reassignmentData.copy(contract = modifiedContract)

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)(
            "first add failed"
          )
          add2 <- store.addReassignment(reassignmentDataModified).failOnShutdown.value
        } yield assert(
          add2 == Left(ReassignmentDataAlreadyExists(reassignmentData, reassignmentDataModified)),
          "second add failed",
        )
      }

      "handle unassignment results" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(withUnassignmentResult).failOnShutdown)(
            "first add failed"
          )
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)(
            "second add failed"
          )
          lookup2 <- valueOrFail(store.lookup(reassignment10).failOnShutdown)(
            "UnassignmentResult missing"
          )
          _ <- valueOrFail(store.addReassignment(withUnassignmentResult).failOnShutdown)(
            "third add failed"
          )
        } yield assert(lookup2 == withUnassignmentResult, "UnassignmentResult remains")
      }

      "add several reassignments" in {
        val store = mk(indexedTargetSynchronizer)

        val reassignmentData10 = mkReassignmentData(reassignment10, mediator1)
        val reassignmentData11 = mkReassignmentData(reassignment11, mediator1)
        val reassignmentData20 = mkReassignmentData(reassignment20, mediator2)

        for {

          _ <- valueOrFail(store.addReassignment(reassignmentData10).failOnShutdown)(
            "first add failed"
          )
          _ <- valueOrFail(store.addReassignment(reassignmentData11).failOnShutdown)(
            "second add failed"
          )
          _ <- valueOrFail(store.addReassignment(reassignmentData20).failOnShutdown)(
            "third add failed"
          )
          lookup10 <- valueOrFail(store.lookup(reassignment10).failOnShutdown)(
            "first reassignment not found"
          )
          lookup11 <- valueOrFail(store.lookup(reassignment11).failOnShutdown)(
            "second reassignment not found"
          )
          lookup20 <- valueOrFail(store.lookup(reassignment20).failOnShutdown)(
            "third reassignment not found"
          )
        } yield {
          lookup10 shouldBe reassignmentData10
          lookup11 shouldBe reassignmentData11
          lookup20 shouldBe reassignmentData20
        }
      }

      "complain about reassignments for a different synchronizer" in {
        val store = mk(IndexedSynchronizer.tryCreate(sourceSynchronizer1.unwrap, 2))
        loggerFactory.assertInternalError[IllegalArgumentException](
          store.addReassignment(reassignmentData),
          _.getMessage shouldBe s"Synchronizer ${Target(sourceSynchronizer1.unwrap)}: Reassignment store cannot store reassignment for synchronizer $targetSynchronizerId",
        )
      }
    }

    "addUnassignmentResult" should {

      "report missing reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          missing <- store.addUnassignmentResult(unassignmentResult).failOnShutdown.value
        } yield missing shouldBe Left(UnknownReassignmentId(reassignment10))
      }

      "add the result" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          lookup <- valueOrFail(store.lookup(reassignment10).failOnShutdown)(
            "reassignment not found"
          )
        } yield assert(
          lookup == reassignmentData.copy(unassignmentResult = Some(unassignmentResult)),
          "result is stored",
        )
      }

      "report mismatching results" in {
        val store = mk(indexedTargetSynchronizer)
        val modifiedUnassignmentResult = {
          val updatedContent = unassignmentResult.result
            .focus(_.content.timestamp)
            .replace(CantonTimestamp.ofEpochSecond(2))

          DeliveredUnassignmentResult.create(updatedContent).value
        }

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          modified <- store.addUnassignmentResult(modifiedUnassignmentResult).failOnShutdown.value
          lookup <- valueOrFail(store.lookup(reassignment10).failOnShutdown)(
            "reassignment not found"
          )
        } yield {
          assert(
            modified == Left(
              UnassignmentResultAlreadyExists(
                reassignment10,
                unassignmentResult,
                modifiedUnassignmentResult,
              )
            ),
            "modified result is flagged",
          )
          assert(
            lookup == reassignmentData.copy(unassignmentResult = Some(unassignmentResult)),
            "result is not overwritten stored",
          )
        }
      }
    }

    "completeReassignment" should {
      "mark the reassignment as completed" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, toc))("completion failed")
          lookup <- store.lookup(reassignment10).value
        } yield lookup shouldBe Left(ReassignmentCompleted(reassignment10, toc))
      }.failOnShutdown

      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, toc))(
            "first completion failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, toc))(
            "second completion failed"
          )
        } yield succeed
      }.failOnShutdown

      "be allowed before the result" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
          _ <- valueOrFail(store.completeReassignment(reassignment10, toc))(
            "first completion failed"
          )
          lookup1 <- store.lookup(reassignment10).value
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          lookup2 <- store.lookup(reassignment10).value
          _ <- valueOrFail(store.completeReassignment(reassignment10, toc))(
            "second completion failed"
          )
        } yield {
          lookup1 shouldBe Left(ReassignmentCompleted(reassignment10, toc))
          lookup2 shouldBe Left(ReassignmentCompleted(reassignment10, toc))
        }
      }.failOnShutdown

      "detect mismatches" in {
        val store = mk(indexedTargetSynchronizer)
        val toc2 = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(4))
        val modifiedReassignmentData =
          reassignmentData.copy(unassignmentRequestCounter = RequestCounter(100))
        val modifiedUnassignmentResult = {
          val updatedContent =
            unassignmentResult.result.focus(_.content.counter).replace(SequencerCounter(120))

          DeliveredUnassignmentResult.create(updatedContent).value
        }

        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, toc))(
            "first completion failed"
          )
          complete2 <- store.completeReassignment(reassignment10, toc2).value
          add2 <- store.addReassignment(modifiedReassignmentData).value
          addResult2 <- store.addUnassignmentResult(modifiedUnassignmentResult).value
        } yield {
          complete2 shouldBe Checked.continue(ReassignmentAlreadyCompleted(reassignment10, toc2))
          add2 shouldBe Left(
            ReassignmentDataAlreadyExists(withUnassignmentResult, modifiedReassignmentData)
          )
          addResult2 shouldBe Left(
            UnassignmentResultAlreadyExists(
              reassignment10,
              unassignmentResult,
              modifiedUnassignmentResult,
            )
          )
        }
      }.failOnShutdown

      "store the first completion" in {
        val store = mk(indexedTargetSynchronizer)
        val toc2 = TimeOfChange(RequestCounter(1), CantonTimestamp.ofEpochSecond(4))
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, toc2))(
            "later completion failed"
          )
          complete2 <- store.completeReassignment(reassignment10, toc).value
          lookup <- store.lookup(reassignment10).value
        } yield {
          complete2 shouldBe Checked.continue(ReassignmentAlreadyCompleted(reassignment10, toc))
          lookup shouldBe Left(ReassignmentCompleted(reassignment10, toc2))
        }
      }.failOnShutdown
    }

    "delete" should {
      "remove the reassignment" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- store.deleteReassignment(reassignment10)
          lookup <- store.lookup(reassignment10).value
        } yield lookup shouldBe Left(UnknownReassignmentId(reassignment10))
      }.failOnShutdown

      "purge completed reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, toc))("completion failed")
          _ <- store.deleteReassignment(reassignment10)
        } yield succeed
      }.failOnShutdown

      "ignore unknown reassignment IDs" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          () <- store.deleteReassignment(reassignment10).failOnShutdown
        } yield succeed
      }

      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addReassignment(reassignmentData))("add failed")
          _ <- store.deleteReassignment(reassignment10)
          _ <- store.deleteReassignment(reassignment10)
        } yield succeed
      }.failOnShutdown
    }

    "reassignment stores should be isolated" in {
      val storeTarget = mk(indexedTargetSynchronizer)
      val store1 = mk(IndexedSynchronizer.tryCreate(sourceSynchronizer1.unwrap, 2))
      for {
        _ <- valueOrFail(storeTarget.addReassignment(reassignmentData))("add failed")
        found <- store1.lookup(reassignmentData.reassignmentId).value
      } yield found shouldBe Left(UnknownReassignmentId(reassignmentData.reassignmentId))
    }.failOnShutdown

    "deleteCompletionsSince" should {
      "remove the completions from the criterion on" in {
        val store = mk(indexedTargetSynchronizer)
        val toc1 = TimeOfChange(RequestCounter(1), CantonTimestamp.ofEpochSecond(5))
        val toc2 = TimeOfChange(RequestCounter(2), CantonTimestamp.ofEpochSecond(7))

        val aliceReassignment =
          mkReassignmentData(reassignment10, mediator1, LfPartyId.assertFromString("alice"))
        val bobReassignment = mkReassignmentData(
          reassignment11,
          mediator1,
          LfPartyId.assertFromString("bob"),
        )
        val eveReassignment = mkReassignmentData(
          reassignment20,
          mediator2,
          LfPartyId.assertFromString("eve"),
        )

        for {
          _ <- valueOrFail(store.addReassignment(aliceReassignment))(
            "add alice failed"
          )
          _ <- valueOrFail(store.addReassignment(bobReassignment))("add bob failed")
          _ <- valueOrFail(store.addReassignment(eveReassignment))("add eve failed")
          _ <- valueOrFail(store.completeReassignment(reassignment10, toc))(
            "completion alice failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment11, toc1))(
            "completion bob failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment20, toc2))(
            "completion eve failed"
          )
          _ <- store.deleteCompletionsSince(RequestCounter(1))
          alice <- leftOrFail(store.lookup(reassignment10))("alice must still be completed")
          bob <- valueOrFail(store.lookup(reassignment11))("bob must not be completed")
          eve <- valueOrFail(store.lookup(reassignment20))("eve must not be completed")
          _ <- valueOrFail(store.completeReassignment(reassignment11, toc2))(
            "second completion bob failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment20, toc1))(
            "second completion eve failed"
          )
        } yield {
          alice shouldBe ReassignmentCompleted(reassignment10, toc)
          bob shouldBe bobReassignment
          eve shouldBe eveReassignment
        }
      }.failOnShutdown
    }
  }
}

object ReassignmentStoreTest extends EitherValues with NoTracing {

  val alice = LfPartyId.assertFromString("alice")
  val bob = LfPartyId.assertFromString("bob")

  private def contract(id: LfContractId, signatory: LfPartyId): SerializableContract =
    asSerializable(
      contractId = id,
      contractInstance = contractInstance(),
      ledgerTime = CantonTimestamp.Epoch,
      metadata = ContractMetadata.tryCreate(Set.empty, Set(signatory), None),
    )

  val coidAbs1 = suffixedId(1, 0)
  val coidAbs2 = suffixedId(2, 0)
  val contract = asSerializable(
    contractId = coidAbs1,
    contractInstance = contractInstance(),
    ledgerTime = CantonTimestamp.Epoch,
  )

  val synchronizer1 = SynchronizerId(UniqueIdentifier.tryCreate("synchronizer1", "SYNCHRONIZER1"))
  val sourceSynchronizer1 = Source(
    SynchronizerId(UniqueIdentifier.tryCreate("synchronizer1", "SYNCHRONIZER1"))
  )
  val targetSynchronizer1 = Target(
    SynchronizerId(UniqueIdentifier.tryCreate("synchronizer1", "SYNCHRONIZER1"))
  )
  val mediator1 = MediatorGroupRecipient(MediatorGroupIndex.zero)

  val synchronizer2 = SynchronizerId(UniqueIdentifier.tryCreate("synchronizer2", "SYNCHRONIZER2"))
  val sourceSynchronizer2 = Source(
    SynchronizerId(UniqueIdentifier.tryCreate("synchronizer2", "SYNCHRONIZER2"))
  )
  val targetSynchronizer2 = Target(
    SynchronizerId(UniqueIdentifier.tryCreate("synchronizer2", "SYNCHRONIZER2"))
  )
  val mediator2 = MediatorGroupRecipient(MediatorGroupIndex.one)

  val indexedTargetSynchronizer =
    IndexedSynchronizer.tryCreate(
      SynchronizerId(UniqueIdentifier.tryCreate("target", "SYNCHRONIZER")),
      1,
    )
  val targetSynchronizerId = Target(indexedTargetSynchronizer.synchronizerId)
  val targetSynchronizer = Target(
    SynchronizerId(UniqueIdentifier.tryCreate("target", "SYNCHRONIZER"))
  )

  val reassignment10 = ReassignmentId(sourceSynchronizer1, CantonTimestamp.Epoch)
  val reassignment11 = ReassignmentId(sourceSynchronizer1, CantonTimestamp.ofEpochMilli(1))
  val reassignment20 = ReassignmentId(sourceSynchronizer2, CantonTimestamp.Epoch)

  val loggerFactoryNotUsed = NamedLoggerFactory.unnamedKey("test", "NotUsed-ReassignmentStoreTest")
  val ec: ExecutionContext = DirectExecutionContext(
    loggerFactoryNotUsed.getLogger(ReassignmentStoreTest.getClass)
  )
  private implicit val _ec: ExecutionContext = ec

  lazy val crypto = SymbolicCrypto.create(
    BaseTest.testedReleaseProtocolVersion,
    DefaultProcessingTimeouts.testing,
    loggerFactoryNotUsed,
  )
  val sequencerKey = crypto.generateSymbolicSigningKey()

  def sign(str: String): Signature = {
    val hash =
      crypto.pureCrypto
        .build(TestHash.testHashPurpose)
        .addWithoutLengthPrefix(str)
        .finish()
    crypto.sign(hash, sequencerKey.id, SigningKeyUsage.ProtocolOnly)
  }

  val seedGenerator = new SeedGenerator(crypto.pureCrypto)

  def mkReassignmentDataForSynchronizer(
      reassignmentId: ReassignmentId,
      sourceMediator: MediatorGroupRecipient,
      submittingParty: LfPartyId = LfPartyId.assertFromString("submitter"),
      targetSynchronizerId: Target[SynchronizerId],
      contract: SerializableContract = contract,
      unassignmentGlobalOffset: Option[Offset] = None,
  ): ReassignmentData = {

    val identityFactory = TestingTopology()
      .withSynchronizers(reassignmentId.sourceSynchronizer.unwrap)
      .build(loggerFactoryNotUsed)

    val helpers = ReassignmentDataHelpers(
      contract,
      reassignmentId.sourceSynchronizer,
      targetSynchronizerId,
      identityFactory,
    )

    val unassignmentRequest = helpers.unassignmentRequest(
      submittingParty,
      DefaultTestIdentities.participant1,
      sourceMediator,
    )()

    helpers.reassignmentData(reassignmentId, unassignmentRequest)(unassignmentGlobalOffset)
  }

  private def mkReassignmentData(
      reassignmentId: ReassignmentId,
      sourceMediator: MediatorGroupRecipient,
      submitter: LfPartyId = LfPartyId.assertFromString("submitter"),
      contract: SerializableContract = contract,
      unassignmentGlobalOffset: Option[Offset] = None,
  ): ReassignmentData =
    mkReassignmentDataForSynchronizer(
      reassignmentId,
      sourceMediator,
      submitter,
      targetSynchronizerId,
      contract,
      unassignmentGlobalOffset,
    )

  def mkUnassignmentResult(reassignmentData: ReassignmentData): DeliveredUnassignmentResult = {
    val requestId = RequestId(reassignmentData.unassignmentTs)

    val mediatorMessage =
      reassignmentData.unassignmentRequest.tree.mediatorMessage(Signature.noSignature)
    val result = ConfirmationResultMessage.create(
      mediatorMessage.synchronizerId,
      ViewType.UnassignmentViewType,
      requestId,
      mediatorMessage.rootHash,
      Verdict.Approve(BaseTest.testedProtocolVersion),
      mediatorMessage.allInformees,
      BaseTest.testedProtocolVersion,
    )
    val signedResult =
      SignedProtocolMessage.from(
        result,
        BaseTest.testedProtocolVersion,
        sign("UnassignmentResult-mediator"),
      )
    val batch =
      Batch.of(BaseTest.testedProtocolVersion, signedResult -> RecipientsTest.testInstance)
    val deliver = Deliver.create(
      SequencerCounter(1),
      CantonTimestamp.ofEpochMilli(10),
      reassignmentData.sourceSynchronizer.unwrap,
      Some(MessageId.tryCreate("1")),
      batch,
      Some(reassignmentData.unassignmentTs),
      BaseTest.testedProtocolVersion,
      Option.empty[TrafficReceipt],
    )
    val content = SignedContent(
      deliver,
      sign("UnassignmentResult-sequencer"),
      None,
      BaseTest.testedProtocolVersion,
    )

    DeliveredUnassignmentResult.create(content).value
  }
}
