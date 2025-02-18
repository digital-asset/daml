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
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentData.*
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentData,
  IncompleteReassignmentData,
  ReassignmentDataHelpers,
  UnassignmentData,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.store.ReassignmentStore.*
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
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, FailOnShutdown, LfPartyId, SequencerCounter}
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, EitherValues}

import scala.annotation.unused
import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

trait ReassignmentStoreTest extends FailOnShutdown {
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
    ): UnassignmentData = mkReassignmentData(
      reassignmentId,
      mediator1,
      contract = contract,
    )

    val unassignmentResult = mkUnassignmentResult(reassignmentData)
    val withUnassignmentResult =
      reassignmentData.copy(unassignmentResult = Some(unassignmentResult))
    val ts = CantonTimestamp.ofEpochSecond(3)

    "lookup" should {
      "find previously stored reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          lookup10 <- valueOrFail(store.lookup(reassignment10))(
            "lookup failed to find the stored reassignment"
          )
        } yield assert(lookup10 == reassignmentData, "lookup finds the stored data")
      }

      "not invent reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          lookup10 <- store.lookup(reassignment11).value
        } yield assert(
          lookup10 == Left(UnknownReassignmentId(reassignment11)),
          "lookup finds the stored data",
        )
      }
    }

    "findAfter" should {

      def populate(store: ReassignmentStore): FutureUnlessShutdown[List[UnassignmentData]] = {
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
          _ <- valueOrFail(store.addUnassignmentData(reassignment1))("first add failed")
          _ <- valueOrFail(store.addUnassignmentData(reassignment2))("second add failed")
          _ <- valueOrFail(store.addUnassignmentData(reassignment3))("third add failed")
          _ <- valueOrFail(store.addUnassignmentData(reassignment4))("fourth add failed")
        } yield (List(reassignment1, reassignment2, reassignment3, reassignment4))
      }

      "order pending reassignments" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          reassignments <- populate(store)
          lookup <- store.findAfter(None, 10)
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

        } yield {
          assert(lookup == Seq(reassignment3, reassignment1, reassignment4))
        }
      }
      "give no pending reassignments when empty" in {
        val store = mk(indexedTargetSynchronizer)
        for { lookup <- store.findAfter(None, 10) } yield {
          lookup shouldBe empty
        }
      }
      "limit the results" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          reassignments <- populate(store)
          lookup <- store.findAfter(None, 2)
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
              CantonTimestamp.Epoch.plusSeconds(3),
            )
            .value

          lookup <- store.findAfter(None, 10)
        } yield {
          assert(checked.successful)
          assert(lookup == Seq(reassignment3, reassignment1, reassignment4))
        }

      }
    }

    "addAssignmentDataIfAbsent" should {
      val data = reassignmentData
      val assignmentData = AssignmentData(
        data.reassignmentId,
        data.contract,
        data.sourceProtocolVersion,
      )

      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
        } yield succeed
      }

      "AddAssignmentData doesn't update if conflicting data" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          _ <- valueOrFail(
            store
              .addAssignmentDataIfAbsent(
                assignmentData.copy(sourceProtocolVersion = Source(ProtocolVersion.dev))
              )
          )("addAssignmentDataIfAbsent")
          entry <- store.findReassignmentEntry(data.reassignmentId).value
        } yield entry.map(_.sourceProtocolVersion) shouldBe Right(data.sourceProtocolVersion)
      }

      "AddAssignmentData doesn't update the entry once the reassignment data is inserted" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- store.addUnassignmentData(data).value
          entry11 <- store.findReassignmentEntry(data.reassignmentId).value
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          entry12 <- store.findReassignmentEntry(data.reassignmentId).value

          _ <- store.addUnassignmentResult(unassignmentResult).value
          entry21 <- store.findReassignmentEntry(data.reassignmentId).value
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          entry22 <- store.findReassignmentEntry(data.reassignmentId).value

          _ <- store.completeReassignment(data.reassignmentId, ts).value
          entry31 <- store.findReassignmentEntry(data.reassignmentId).value
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          entry32 <- store.findReassignmentEntry(data.reassignmentId).value
        } yield {
          entry11 shouldBe entry12
          entry21 shouldBe entry22
          entry31 shouldBe entry32
        }
      }

      "addUnassignment is called after addAssignmentData with conflicting data" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          modifiedReassignmentData = ReassignmentStoreTest.mkReassignmentDataForSynchronizer(
            data.reassignmentId,
            data.sourceMediator,
            data.unassignmentRequest.submitter,
            targetSynchronizer,
            contract,
            sourcePV = ProtocolVersion.create("6", allowDeleted = true).value,
          )
          _ <- store.addUnassignmentData(modifiedReassignmentData).value
          entry <- store.findReassignmentEntry(data.reassignmentId).value
        } yield entry shouldBe Right(
          ReassignmentEntry(modifiedReassignmentData, None, None)
        )
      }

      "complete the assignment before the unassignment" in {
        val store = mk(indexedTargetSynchronizer)
        (for {
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          entry1 <- store.findReassignmentEntry(data.reassignmentId).value
          lookup1 <- store.lookup(data.reassignmentId).value
          _ <- store.completeReassignment(data.reassignmentId, ts).value
          lookup2 <- store.lookup(data.reassignmentId).value

          _ <- store.addUnassignmentData(data).value
          lookup3 <- store.lookup(data.reassignmentId).value

          _ <- store.addUnassignmentResult(unassignmentResult).value
          lookup4 <- store.lookup(data.reassignmentId).value
          entry4 <- store.findReassignmentEntry(data.reassignmentId).value
        } yield {
          lookup1 shouldBe Left(
            AssignmentStartingBeforeUnassignment(reassignmentData.reassignmentId)
          )
          entry1 shouldBe Right(
            ReassignmentEntry(
              data.reassignmentId,
              data.sourceProtocolVersion,
              data.contract,
              None,
              CantonTimestamp.Epoch,
              None,
              None,
              None,
            )
          )
          lookup2 shouldBe Left(ReassignmentCompleted(reassignmentData.reassignmentId, ts))
          lookup3 shouldBe Left(ReassignmentCompleted(reassignmentData.reassignmentId, ts))

          lookup4 shouldBe Left(ReassignmentCompleted(reassignmentData.reassignmentId, ts))
          entry4 shouldBe Right(
            ReassignmentEntry(
              data.copy(unassignmentResult = Some(unassignmentResult)),
              None,
              Some(ts),
            )
          )
        })

      }
    }

    "add unassignment/in global offsets" should {

      val reassignmentId = reassignmentData.reassignmentId

      val unassignmentOffset = UnassignmentGlobalOffset(10L)
      val assignmentOffset = AssignmentGlobalOffset(15L)

      val reassignmentEntryOnlyUnassignment =
        ReassignmentEntry(
          reassignmentData,
          Some(UnassignmentGlobalOffset(unassignmentOffset.offset)),
          None,
        )

      val reassignmentEntryReassignmentComplete =
        reassignmentEntryOnlyUnassignment.copy(reassignmentGlobalOffset =
          Some(
            ReassignmentGlobalOffsets
              .create(unassignmentOffset.offset, assignmentOffset.offset)
              .value
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
            store.addUnassignmentData(reassignmentData)
          })("add reassignments")

          offsets = data.map { case (offset, reassignmentData) =>
            reassignmentData.reassignmentId -> offset
          }

          _ <- store.addReassignmentsOffsets(offsets).value

          result <- valueOrFail(offsets.toList.parTraverse { case (reassignmentId, _) =>
            store.findReassignmentEntry(reassignmentId)
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
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFail(
              "add unassignment offset 1"
            )

          lookupOnlyUnassignment1 <- valueOrFail(store.findReassignmentEntry(reassignmentId))(
            "lookup reassignment data"
          )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFail(
              "add unassignment offset 2"
            )

          lookupOnlyUnassignment2 <- valueOrFail(store.findReassignmentEntry(reassignmentId))(
            "lookup reassignment data"
          )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFail(
              "add assignment offset 1"
            )

          lookup1 <- valueOrFail(store.findReassignmentEntry(reassignmentId))(
            "lookup reassignment data"
          )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFail(
              "add assignment offset 2"
            )

          lookup2 <- valueOrFail(store.findReassignmentEntry(reassignmentId))(
            "lookup reassignment data"
          )

        } yield {
          lookupOnlyUnassignment1 shouldBe reassignmentEntryOnlyUnassignment
          lookupOnlyUnassignment2 shouldBe reassignmentEntryOnlyUnassignment

          lookup1 shouldBe reassignmentEntryReassignmentComplete
          lookup2 shouldBe reassignmentEntryReassignmentComplete
        }
      }

      "return an error if assignment offset is the same as the unassignment" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFail(
              "add unassignment offset"
            )

          failedAdd <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(unassignmentOffset.offset))
            )
            .value

        } yield failedAdd.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]
      }

      "return an error if unassignment offset is the same as the assignment" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFail(
              "add assignment offset"
            )

          failedAdd <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(assignmentOffset.offset))
            )
            .value

        } yield failedAdd.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]
      }

      "return an error if the new value differs from the old one" in {
        val store = mk(indexedTargetSynchronizer)

        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFail(
              "add unassignment offset 1"
            )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFail(
              "add unassignment offset 2"
            )

          lookup1 <- valueOrFail(store.findReassignmentEntry(reassignmentId))(
            "lookup reassignment data"
          )

          successfulAddOutOffset <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .value

          failedAddOutOffset <- store
            .addReassignmentsOffsets(
              Map(
                reassignmentId -> UnassignmentGlobalOffset(
                  Offset.tryFromLong(unassignmentOffset.offset.unwrap - 1)
                )
              )
            )
            .value

          successfulAddInOffset <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .value

          failedAddInOffset <- store
            .addReassignmentsOffsets(
              Map(
                reassignmentId -> AssignmentGlobalOffset(
                  Offset.tryFromLong(assignmentOffset.offset.unwrap - 1)
                )
              )
            )
            .value

          lookup2 <- valueOrFail(store.findReassignmentEntry(reassignmentId))(
            "lookup reassignment data"
          )

        } yield {
          successfulAddOutOffset.value shouldBe ()
          failedAddOutOffset.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]

          successfulAddInOffset.value shouldBe ()
          failedAddInOffset.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]

          lookup1 shouldBe reassignmentEntryReassignmentComplete
          lookup2 shouldBe reassignmentEntryReassignmentComplete
        }
      }
    }

    "findIncomplete" should {
      val limit = NonNegativeInt.tryCreate(10)

      def assertIsIncomplete(
          incompletes: Seq[IncompleteReassignmentData],
          expectedReassignmentEntry: ReassignmentEntry,
          queryOffset: Offset,
      ): Assertion = {
        val expectedIncomplete = IncompleteReassignmentData.tryCreate(
          expectedReassignmentEntry.sourceSynchronizer,
          expectedReassignmentEntry.unassignmentTs,
          expectedReassignmentEntry.reassignmentGlobalOffset,
          queryOffset,
        )
        incompletes.map(_.reassignmentId) shouldBe Seq(expectedReassignmentEntry.reassignmentId)
        incompletes shouldBe Seq(expectedIncomplete)
      }

      "list incomplete reassignments (unassignment done)" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = reassignmentData.reassignmentId

        val unassignmentOsset = 10L
        val assignmentOffset = 20L

        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          lookupNoOffset <- store.findIncomplete(None, Long.MaxValue, None, limit)

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOsset))
            )
            .valueOrFail(
              "add unassignment offset failed"
            )
          lookupBeforeUnassignment <- store
            .findIncomplete(
              None,
              unassignmentOsset - 1,
              None,
              limit,
            )

          lookupAtUnassignment <- store
            .findIncomplete(None, unassignmentOsset, None, limit)

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
            )
            .valueOrFail(
              "add assignment offset failed"
            )

          lookupBeforeAssignment <- store
            .findIncomplete(
              None,
              assignmentOffset - 1,
              None,
              limit,
            )

          lookupAtAssignment <- store
            .findIncomplete(None, assignmentOffset, None, limit)

          lookupAfterAssignment <- store
            .findIncomplete(
              None,
              assignmentOffset,
              None,
              limit,
            )

        } yield {
          lookupNoOffset shouldBe empty

          lookupBeforeUnassignment shouldBe empty

          assertIsIncomplete(
            lookupAtUnassignment,
            ReassignmentEntry(
              reassignmentData,
              Some(UnassignmentGlobalOffset(unassignmentOsset)),
              None,
            ),
            unassignmentOsset,
          )

          assertIsIncomplete(
            lookupBeforeAssignment,
            ReassignmentEntry(
              reassignmentData,
              Some(UnassignmentGlobalOffset(unassignmentOsset)),
              None,
            ),
            assignmentOffset - 1,
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
        val assignmentData = AssignmentData(
          reassignmentData.reassignmentId,
          reassignmentData.contract,
          reassignmentData.sourceProtocolVersion,
        )

        for {
          _ <- valueOrFail(store.addAssignmentDataIfAbsent(assignmentData))("add failed")
          lookupNoOffset <- store.findIncomplete(None, Long.MaxValue, None, limit)
          _ <- store.completeReassignment(assignmentData.reassignmentId, ts).value
          _ <-
            store
              .addReassignmentsOffsets(
                Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
              )
              .valueOrFail(
                "add assignment offset failed"
              )

          lookupBeforeAssignment <- store
            .findIncomplete(
              None,
              assignmentOffset - 1,
              None,
              limit,
            )

          lookupAtAssignment <- store
            .findIncomplete(None, assignmentOffset, None, limit)

          _ <- store.addUnassignmentData(reassignmentData).value
          _ <- store.addUnassignmentResult(unassignmentResult).value

          _ <-
            store
              .addReassignmentsOffsets(
                Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
              )
              .valueOrFail(
                "add unassignment offset failed"
              )

          lookupBeforeUnassignment <- store
            .findIncomplete(
              None,
              unassignmentOffset - 1,
              None,
              limit,
            )

          lookupAtUnassignment <- store
            .findIncomplete(None, unassignmentOffset, None, limit)

          lookupAfterUnassignment <- store
            .findIncomplete(
              None,
              unassignmentOffset,
              None,
              limit,
            )

        } yield {
          lookupNoOffset shouldBe empty

          lookupBeforeAssignment shouldBe empty

          assertIsIncomplete(
            lookupAtAssignment,
            ReassignmentEntry(
              reassignmentData,
              Some(AssignmentGlobalOffset(assignmentOffset)),
              None,
            ),
            assignmentOffset,
          )

          assertIsIncomplete(
            lookupBeforeUnassignment,
            ReassignmentEntry(
              reassignmentData,
              Some(AssignmentGlobalOffset(assignmentOffset)),
              None,
            ),
            unassignmentOffset - 1,
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
          )
        }

        val addReassignmentsET = reassignmentsData.parTraverse(store.addUnassignmentData)

        def lift(stakeholder: LfPartyId, others: LfPartyId*): Option[NonEmpty[Set[LfPartyId]]] =
          Option(NonEmpty(Set, stakeholder, others*))

        for {
          _ <- valueOrFail(addReassignmentsET)("add failed")
          _ <- store
            .addReassignmentsOffsets(
              reassignmentsData
                .map(_.reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
                .toMap
            )
            .value

          lookupNone <- store.findIncomplete(None, unassignmentOffset, None, limit)

          lookupAll <- store
            .findIncomplete(
              None,
              unassignmentOffset,
              lift(alice, bob),
              limit,
            )

          lookupAlice <- store
            .findIncomplete(None, unassignmentOffset, lift(alice), limit)

          lookupBob <- store
            .findIncomplete(None, unassignmentOffset, lift(bob), limit)

        } yield {
          lookupNone.map(_.reassignmentId) should contain theSameElementsAs reassignmentsData.map(
            _.reassignmentId
          )
          lookupAll.map(_.reassignmentId) should contain theSameElementsAs reassignmentsData.map(
            _.reassignmentId
          )
          lookupAlice.map(_.reassignmentId) should contain theSameElementsAs reassignmentsData
            .filter(_.contract == aliceContract)
            .map(_.reassignmentId)
          lookupBob.map(_.reassignmentId) should contain theSameElementsAs reassignmentsData
            .filter(_.contract == bobContract)
            .map(_.reassignmentId)
        }
      }

      "take synchronizer filter into account" in {
        val store = mk(indexedTargetSynchronizer)
        val offset = 10L

        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add")
          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentData.reassignmentId -> AssignmentGlobalOffset(offset))
            )
            .valueOrFail("add out offset")
          entry <- store
            .findReassignmentEntry(reassignmentData.reassignmentId)
            .valueOrFail("lookup")

          lookup1a <- store
            .findIncomplete(Some(sourceSynchronizer2), offset, None, limit) // Wrong synchronizer
          lookup1b <- store
            .findIncomplete(Some(sourceSynchronizer1), offset, None, limit)

          lookup1c <- store.findIncomplete(None, offset, None, limit)
        } yield {
          lookup1a shouldBe empty
          assertIsIncomplete(lookup1b, entry, offset)
          assertIsIncomplete(lookup1c, entry, offset)
        }
      }

      "limit the results" in {
        val store = mk(indexedTargetSynchronizer)
        val offset = 42L

        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add")
          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentData.reassignmentId -> UnassignmentGlobalOffset(offset))
            )
            .valueOrFail("add out offset")

          lookup0 <- store.findIncomplete(None, offset, None, NonNegativeInt.zero)
          lookup1 <- store.findIncomplete(None, offset, None, NonNegativeInt.one)

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
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
            )
            .valueOrFail(
              "add unassignment offset failed"
            )
          lookupAfterUnassignment <- store.findEarliestIncomplete()

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
            )
            .valueOrFail(
              "add assignment offset failed"
            )

          lookupAfterAssignment <- store.findEarliestIncomplete()
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
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
            )
            .valueOrFail(
              "add assignment offset failed"
            )
          lookupAfterAssignment <- store.findEarliestIncomplete()

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
            )
            .valueOrFail(
              "add unassignment offset failed"
            )
          lookupAfterUnassignment <- store.findEarliestIncomplete()

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
          lookupEmpty <- store.findEarliestIncomplete()
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData3))("add failed")

          lookupAllInFlight <- store.findEarliestIncomplete()

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId1 -> AssignmentGlobalOffset(assignmentOffset1))
            )
            .valueOrFail(
              "add assignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId1 -> UnassignmentGlobalOffset(unassignmentOffset1))
            )
            .valueOrFail(
              "add unassignment offset failed"
            )

          lookupInFlightOrComplete <- store.findEarliestIncomplete()

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId3 -> AssignmentGlobalOffset(assignmentOffset3))
            )
            .valueOrFail(
              "add assignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId3 -> UnassignmentGlobalOffset(unassignmentOffset3))
            )
            .valueOrFail(
              "add unassignment offset failed"
            )
          lookupAllComplete <- store.findEarliestIncomplete()

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
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData2))("add failed")
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData3))("add failed")

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId1 -> AssignmentGlobalOffset(assignmentOffset1))
            )
            .valueOrFail(
              "add assignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId1 -> UnassignmentGlobalOffset(unassignmentOffset1))
            )
            .valueOrFail(
              "add unassignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId2 -> AssignmentGlobalOffset(unassignmentOffset2))
            )
            .valueOrFail(
              "add unassignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId3 -> AssignmentGlobalOffset(assignmentOffset3))
            )
            .valueOrFail(
              "add assignment offset failed"
            )

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId3 -> UnassignmentGlobalOffset(unassignmentOffset3))
            )
            .valueOrFail(
              "add unassignment offset failed"
            )

          lookupEnd <- store.findEarliestIncomplete()

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
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))(
            "first add failed"
          )
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))(
            "second add failed"
          )
        } yield succeed
      }

      "detect modified reassignment data" in {
        val store = mk(indexedTargetSynchronizer)
        val modifiedUnassignmentDecisionTime = CantonTimestamp.ofEpochMilli(100)

        val reassignmentDataModified =
          reassignmentData.copy(unassignmentDecisionTime = modifiedUnassignmentDecisionTime)

        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))(
            "first add failed"
          )
          add2 <- store.addUnassignmentData(reassignmentDataModified).value
        } yield assert(
          add2 == Left(ReassignmentDataAlreadyExists(reassignmentData, reassignmentDataModified)),
          "second add failed",
        )
      }

      "handle unassignment results" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(withUnassignmentResult))(
            "first add failed"
          )
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))(
            "second add failed"
          )
          lookup2 <- valueOrFail(store.lookup(reassignment10))(
            "UnassignmentResult missing"
          )
          _ <- valueOrFail(store.addUnassignmentData(withUnassignmentResult))(
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

          _ <- valueOrFail(store.addUnassignmentData(reassignmentData10))(
            "first add failed"
          )
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData11))(
            "second add failed"
          )
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData20))(
            "third add failed"
          )
          lookup10 <- valueOrFail(store.lookup(reassignment10))(
            "first reassignment not found"
          )
          lookup11 <- valueOrFail(store.lookup(reassignment11))(
            "second reassignment not found"
          )
          lookup20 <- valueOrFail(store.lookup(reassignment20))(
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
          store.addUnassignmentData(reassignmentData),
          _.getMessage shouldBe s"Synchronizer ${Target(sourceSynchronizer1.unwrap)}: Reassignment store cannot store reassignment for synchronizer $targetSynchronizerId",
        )
      }
    }

    "addUnassignmentResult" should {

      "report missing reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          missing <- store.addUnassignmentResult(unassignmentResult).value
        } yield missing shouldBe Left(UnknownReassignmentId(reassignment10))
      }

      "add the result" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          lookup <- valueOrFail(store.lookup(reassignment10))(
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
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          modified <- store.addUnassignmentResult(modifiedUnassignmentResult).value
          lookup <- valueOrFail(store.lookup(reassignment10))(
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
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, ts))("completion failed")
          lookup <- store.lookup(reassignment10).value
        } yield lookup shouldBe Left(ReassignmentCompleted(reassignment10, ts))
      }

      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, ts))(
            "first completion failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, ts))(
            "second completion failed"
          )
        } yield succeed
      }

      "be allowed before the result" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.completeReassignment(reassignment10, ts))(
            "first completion failed"
          )
          lookup1 <- store.lookup(reassignment10).value
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          lookup2 <- store.lookup(reassignment10).value
          _ <- valueOrFail(store.completeReassignment(reassignment10, ts))(
            "second completion failed"
          )
        } yield {
          lookup1 shouldBe Left(ReassignmentCompleted(reassignment10, ts))
          lookup2 shouldBe Left(ReassignmentCompleted(reassignment10, ts))
        }
      }

      "detect mismatches" in {
        val store = mk(indexedTargetSynchronizer)
        val ts2 = CantonTimestamp.ofEpochSecond(4)
        val modifiedReassignmentData =
          reassignmentData.copy(unassignmentDecisionTime = CantonTimestamp.ofEpochSecond(100))
        val modifiedUnassignmentResult = {
          val updatedContent =
            unassignmentResult.result.focus(_.content.counter).replace(SequencerCounter(120))

          DeliveredUnassignmentResult.create(updatedContent).value
        }

        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, ts))(
            "first completion failed"
          )
          complete2 <- store.completeReassignment(reassignment10, ts2).value
          add2 <- store.addUnassignmentData(modifiedReassignmentData).value
          addResult2 <- store.addUnassignmentResult(modifiedUnassignmentResult).value
        } yield {
          complete2 shouldBe Checked.continue(ReassignmentAlreadyCompleted(reassignment10, ts2))
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
      }

      "store the first completion" in {
        val store = mk(indexedTargetSynchronizer)
        val ts2 = CantonTimestamp.ofEpochSecond(4)
        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, ts2))(
            "later completion failed"
          )
          complete2 <- store.completeReassignment(reassignment10, ts).value
          lookup <- store.lookup(reassignment10).value
        } yield {
          complete2 shouldBe Checked.continue(ReassignmentAlreadyCompleted(reassignment10, ts))
          lookup shouldBe Left(ReassignmentCompleted(reassignment10, ts2))
        }
      }
    }

    "delete" should {
      "remove the reassignment" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- store.deleteReassignment(reassignment10)
          lookup <- store.lookup(reassignment10).value
        } yield lookup shouldBe Left(UnknownReassignmentId(reassignment10))
      }

      "purge completed reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult))(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment10, ts))("completion failed")
          _ <- store.deleteReassignment(reassignment10)
        } yield succeed
      }

      "ignore unknown reassignment IDs" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          () <- store.deleteReassignment(reassignment10)
        } yield succeed
      }

      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(reassignmentData))("add failed")
          _ <- store.deleteReassignment(reassignment10)
          _ <- store.deleteReassignment(reassignment10)
        } yield succeed
      }
    }

    "reassignment stores should be isolated" in {
      val storeTarget = mk(indexedTargetSynchronizer)
      val store1 = mk(IndexedSynchronizer.tryCreate(sourceSynchronizer1.unwrap, 2))
      for {
        _ <- valueOrFail(storeTarget.addUnassignmentData(reassignmentData))("add failed")
        found <- store1.lookup(reassignmentData.reassignmentId).value
      } yield found shouldBe Left(UnknownReassignmentId(reassignmentData.reassignmentId))
    }

    "deleteCompletionsSince" should {
      "remove the completions from the criterion on" in {
        val store = mk(indexedTargetSynchronizer)
        val ts1 = CantonTimestamp.ofEpochSecond(5)
        val ts2 = CantonTimestamp.ofEpochSecond(7)

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
          _ <- valueOrFail(store.addUnassignmentData(aliceReassignment))(
            "add alice failed"
          )
          _ <- valueOrFail(store.addUnassignmentData(bobReassignment))("add bob failed")
          _ <- valueOrFail(store.addUnassignmentData(eveReassignment))("add eve failed")
          _ <- valueOrFail(store.completeReassignment(reassignment10, ts))(
            "completion alice failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment11, ts1))(
            "completion bob failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment20, ts2))(
            "completion eve failed"
          )
          _ <- store.deleteCompletionsSince(ts1)
          alice <- leftOrFail(store.lookup(reassignment10))("alice must still be completed")
          bob <- valueOrFail(store.lookup(reassignment11))("bob must not be completed")
          eve <- valueOrFail(store.lookup(reassignment20))("eve must not be completed")
          _ <- valueOrFail(store.completeReassignment(reassignment11, ts2))(
            "second completion bob failed"
          )
          _ <- valueOrFail(store.completeReassignment(reassignment20, ts1))(
            "second completion eve failed"
          )
        } yield {
          alice shouldBe ReassignmentCompleted(reassignment10, ts)
          bob shouldBe bobReassignment
          eve shouldBe eveReassignment
        }
      }
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
  val sequencerKey = crypto.generateSymbolicSigningKey(usage = SigningKeyUsage.ProtocolOnly)

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
      sourcePV: ProtocolVersion = BaseTest.testedProtocolVersion,
  ): UnassignmentData = {

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
      sourcePV,
    )()

    helpers.reassignmentData(reassignmentId, unassignmentRequest)
  }

  private def mkReassignmentData(
      reassignmentId: ReassignmentId,
      sourceMediator: MediatorGroupRecipient,
      submitter: LfPartyId = LfPartyId.assertFromString("submitter"),
      contract: SerializableContract = contract,
  ): UnassignmentData =
    mkReassignmentDataForSynchronizer(
      reassignmentId,
      sourceMediator,
      submitter,
      targetSynchronizerId,
      contract,
    )

  def mkUnassignmentResult(reassignmentData: UnassignmentData): DeliveredUnassignmentResult = {
    val requestId = RequestId(reassignmentData.unassignmentTs)

    val mediatorMessage =
      reassignmentData.unassignmentRequest.tree.mediatorMessage(Signature.noSignature)
    val result = ConfirmationResultMessage.create(
      mediatorMessage.synchronizerId,
      ViewType.UnassignmentViewType,
      requestId,
      mediatorMessage.rootHash,
      Verdict.Approve(BaseTest.testedProtocolVersion),
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
