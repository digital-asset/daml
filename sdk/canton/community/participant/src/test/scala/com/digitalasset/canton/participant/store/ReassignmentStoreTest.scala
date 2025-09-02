// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.UnassignmentData.{
  AssignmentGlobalOffset,
  ReassignmentGlobalOffsets,
  UnassignmentGlobalOffset,
}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractsReassignmentBatch,
  Offset,
  UnassignmentData,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentData,
  IncompleteReassignmentData,
  ReassignmentDataHelpers,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.store.ReassignmentStore.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{contractInstance, suffixedId}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  ContractMetadata,
  ExampleContractFactory,
  ExampleTransactionFactory,
  LfContractId,
  ReassignmentId,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, MonadUtil}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, LfPartyId, LfTimestamp}
import com.digitalasset.daml.lf.transaction.CreationTime
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, EitherValues}

import scala.annotation.unused
import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

trait ReassignmentStoreTest extends AsyncWordSpec with FailOnShutdown with BaseTest {
  import ReassignmentStoreTest.*
  import CantonTimestamp.{Epoch, ofEpochSecond}

  private implicit def toOffset(i: Long): Offset = Offset.tryFromLong(i)

  protected def reassignmentStore(mk: IndexedSynchronizer => ReassignmentStore): Unit = {
    val unassignmentData = mkUnassignmentData(sourceSynchronizer1, Epoch, mediator1)
    val unassignmentData2 =
      mkUnassignmentData(sourceSynchronizer1, ofEpochSecond(1), mediator1)
    val unassignmentData3 =
      mkUnassignmentData(sourceSynchronizer2, ofEpochSecond(1), mediator1)

    def unassignmentDataFor(
        sourceSynchronizer: Source[PhysicalSynchronizerId],
        unassignmentTs: CantonTimestamp,
        contract: ContractInstance,
    ): UnassignmentData = mkUnassignmentData(
      sourceSynchronizer,
      unassignmentTs,
      mediator1,
      contract = contract,
    )

    val ts = CantonTimestamp.ofEpochSecond(3)

    "lookup" should {
      "find previously stored reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          lookup10 <- valueOrFail(store.lookup(unassignmentData.reassignmentId))(
            "lookup failed to find the stored reassignment"
          )
        } yield assert(lookup10 == unassignmentData, "lookup finds the stored data")
      }

      "not invent reassignments" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          lookup10 <- store.lookup(reassignment11).value
        } yield assert(
          lookup10 == Left(UnknownReassignmentId(reassignment11)),
          "lookup finds the stored data",
        )
      }
    }

    "findAfter" should {

      def populate(store: ReassignmentStore): FutureUnlessShutdown[List[UnassignmentData]] = {
        val reassignment1 = mkUnassignmentData(
          sourceSynchronizer1,
          ofEpochSecond(200),
          mediator1,
          LfPartyId.assertFromString("party1"),
        )
        val reassignment2 = mkUnassignmentData(
          sourceSynchronizer1,
          ofEpochSecond(100),
          mediator1,
          LfPartyId.assertFromString("party2"),
        )
        val reassignment3 = mkUnassignmentData(
          sourceSynchronizer2,
          ofEpochSecond(100),
          mediator2,
          LfPartyId.assertFromString("party2"),
        )
        val reassignment4 = mkUnassignmentData(
          sourceSynchronizer2,
          ofEpochSecond(200),
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
                reassignment2.unassignmentTs -> reassignment2.sourcePSId
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
      val data = unassignmentData
      val assignmentData = AssignmentData(
        data.reassignmentId,
        data.sourcePSId,
        data.contractsBatch,
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

        val updatedBatch = ContractsReassignmentBatch
          .create(assignmentData.contracts.contracts.map { reassign =>
            val newCid = ExampleContractFactory.buildContractId(77)
            (
              ExampleContractFactory.modify(reassign.contract, contractId = Some(newCid)),
              reassign.counter,
            )
          })
          .value
        val updatedAssignmentData = assignmentData.focus(_.contracts).replace(updatedBatch)

        for {
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          _ <- valueOrFail(
            store.addAssignmentDataIfAbsent(updatedAssignmentData)
          )("addAssignmentDataIfAbsent")
          entry <- store.findReassignmentEntry(data.reassignmentId).value
        } yield entry.map(_.contracts) shouldBe Right(data.contractsBatch.contracts.map(_.contract))
      }

      "AddAssignmentData doesn't update the entry once the reassignment data is inserted" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- store.addUnassignmentData(data).value
          entry11 <- store.findReassignmentEntry(data.reassignmentId).value
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          entry12 <- store.findReassignmentEntry(data.reassignmentId).value

          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          entry22 <- store.findReassignmentEntry(data.reassignmentId).value

          _ <- store.completeReassignment(data.reassignmentId, ts).value
          entry31 <- store.findReassignmentEntry(data.reassignmentId).value
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          entry32 <- store.findReassignmentEntry(data.reassignmentId).value
        } yield {
          entry11 shouldBe entry12
          entry31 shouldBe entry32
        }
      }

      "addUnassignment is called after addAssignmentData with conflicting data" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- store.addAssignmentDataIfAbsent(assignmentData).value
          modifiedUnassignmentData = ReassignmentStoreTest.mkUnassignmentDataForSynchronizer(
            sourceMediator = mediator1,
            data.submitterMetadata.submitter,
            data.sourcePSId,
            targetSynchronizer,
            contract,
          )
          _ <- store.addUnassignmentData(modifiedUnassignmentData).value
          entry <- store.findReassignmentEntry(data.reassignmentId).value
        } yield entry shouldBe Right(
          ReassignmentEntry(modifiedUnassignmentData, None, None)
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

        } yield {
          lookup1 shouldBe Left(
            AssignmentStartingBeforeUnassignment(unassignmentData.reassignmentId)
          )
          entry1 shouldBe Right(
            ReassignmentEntry(
              data.reassignmentId,
              data.sourcePSId,
              NonEmpty.mk(Seq, contract),
              None,
              None,
              CantonTimestamp.Epoch,
              None,
            )
          )
          lookup2 shouldBe Left(ReassignmentCompleted(unassignmentData.reassignmentId, ts))
          lookup3 shouldBe Left(ReassignmentCompleted(unassignmentData.reassignmentId, ts))

        })

      }
    }

    "add unassignment/in global offsets" should {

      val reassignmentId = unassignmentData.reassignmentId

      val unassignmentOffset = UnassignmentGlobalOffset(10L)
      val assignmentOffset = AssignmentGlobalOffset(15L)

      val reassignmentEntryOnlyUnassignment =
        ReassignmentEntry(
          unassignmentData,
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
          val reassignmentData =
            unassignmentDataFor(unassignmentData.sourcePSId, ofEpochSecond(i), contract)

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
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add")

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
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add")

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
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add")

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
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add")

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
          expectedReassignmentEntries: Seq[ReassignmentEntry],
          queryOffset: Offset,
      ): Assertion = {
        val expectedIncomplete = expectedReassignmentEntries.map { expectedReassignmentEntry =>
          IncompleteReassignmentData.tryCreate(
            expectedReassignmentEntry.reassignmentId,
            expectedReassignmentEntry.unassignmentData,
            expectedReassignmentEntry.reassignmentGlobalOffset,
            queryOffset,
          )
        }
        incompletes.map(
          _.reassignmentId
        ) should contain theSameElementsAs expectedReassignmentEntries.map(_.reassignmentId)
        incompletes should contain theSameElementsAs expectedIncomplete
      }

      "list incomplete reassignments (unassignment done)" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId1 = unassignmentData.reassignmentId
        val reassignmentId2 = unassignmentData2.reassignmentId

        val unassignmentOsset = 10L
        val assignmentOffset = 20L

        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add 1 failed")
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData2))("add 2 failed")
          lookupNoOffset <- store.findIncomplete(None, Long.MaxValue, None, limit)

          _ <- store
            .addReassignmentsOffsets(
              Map(
                reassignmentId1 -> UnassignmentGlobalOffset(unassignmentOsset),
                reassignmentId2 -> UnassignmentGlobalOffset(unassignmentOsset),
              )
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
              Map(
                reassignmentId1 -> AssignmentGlobalOffset(assignmentOffset),
                reassignmentId2 -> AssignmentGlobalOffset(assignmentOffset),
              )
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
            Seq(
              ReassignmentEntry(
                unassignmentData,
                Some(UnassignmentGlobalOffset(unassignmentOsset)),
                None,
              ),
              ReassignmentEntry(
                unassignmentData2,
                Some(UnassignmentGlobalOffset(unassignmentOsset)),
                None,
              ),
            ),
            unassignmentOsset,
          )

          assertIsIncomplete(
            lookupBeforeAssignment,
            Seq(
              ReassignmentEntry(
                unassignmentData,
                Some(UnassignmentGlobalOffset(unassignmentOsset)),
                None,
              ),
              ReassignmentEntry(
                unassignmentData2,
                Some(UnassignmentGlobalOffset(unassignmentOsset)),
                None,
              ),
            ),
            assignmentOffset - 1,
          )

          lookupAtAssignment shouldBe empty
          lookupAfterAssignment shouldBe empty
        }
      }

      "list incomplete reassignments (assignment done)" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = unassignmentData.reassignmentId

        val assignmentOffset = 10L
        val unassignmentOffset = 20L
        val assignmentData = AssignmentData(
          unassignmentData.reassignmentId,
          unassignmentData.sourcePSId,
          unassignmentData.contractsBatch,
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

          _ <- store.addUnassignmentData(unassignmentData).value

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
            Seq(
              ReassignmentEntry(
                assignmentData,
                Some(AssignmentGlobalOffset(assignmentOffset)),
                CantonTimestamp.Epoch,
                None,
              )
            ),
            assignmentOffset,
          )

          assertIsIncomplete(
            lookupBeforeUnassignment,
            Seq(
              ReassignmentEntry(
                unassignmentData,
                Some(AssignmentGlobalOffset(assignmentOffset)),
                None,
              )
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
          unassignmentDataFor(
            sourceSynchronizer1,
            ofEpochSecond(idx.toLong),
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
            .filter(_.contractsBatch.contracts.map(_.contract).contains(aliceContract))
            .map(_.reassignmentId)
          lookupBob.map(_.reassignmentId) should contain theSameElementsAs reassignmentsData
            .filter(_.contractsBatch.contracts.map(_.contract).contains(bobContract))
            .map(_.reassignmentId)
        }
      }

      "take synchronizer filter into account" in {
        val store = mk(indexedTargetSynchronizer)
        val offset = 10L

        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add")
          _ <- store
            .addReassignmentsOffsets(
              Map(unassignmentData.reassignmentId -> AssignmentGlobalOffset(offset))
            )
            .valueOrFail("add out offset")
          entry <- store
            .findReassignmentEntry(unassignmentData.reassignmentId)
            .valueOrFail("lookup")

          lookup1a <- store
            .findIncomplete(Some(sourceSynchronizer2), offset, None, limit) // Wrong synchronizer
          lookup1b <- store
            .findIncomplete(Some(sourceSynchronizer1), offset, None, limit)

          lookup1c <- store.findIncomplete(None, offset, None, limit)
        } yield {
          lookup1a shouldBe empty
          assertIsIncomplete(lookup1b, Seq(entry), offset)
          assertIsIncomplete(lookup1c, Seq(entry), offset)
        }
      }

      "limit the results" in {
        val store = mk(indexedTargetSynchronizer)
        val offset = 42L

        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add")
          _ <- store
            .addReassignmentsOffsets(
              Map(unassignmentData.reassignmentId -> UnassignmentGlobalOffset(offset))
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
        val reassignmentId = unassignmentData.reassignmentId

        val unassignmentOffset = 10L
        val assignmentOffset = 20L
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")

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
        val reassignmentId = unassignmentData.reassignmentId

        val unassignmentOffset = 10L
        val assignmentOffset = 20L

        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")

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
        val reassignmentId1 = unassignmentData.reassignmentId
        val reassignmentId3 = unassignmentData3.reassignmentId

        val unassignmentOffset1 = 10L
        val assignmentOffset1 = 20L

        val unassignmentOffset3 = 30L
        val assignmentOffset3 = 35L

        for {
          lookupEmpty <- store.findEarliestIncomplete()
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData3))("add failed")

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
        val reassignmentId1 = unassignmentData.reassignmentId
        val reassignmentId2 = unassignmentData2.reassignmentId
        val reassignmentId3 = unassignmentData3.reassignmentId

        val unassignmentOffset1 = 10L
        val assignmentOffset1 = 20L

        // reassignment 2 is incomplete
        val unassignmentOffset2 = 12L

        val unassignmentOffset3 = 30L
        val assignmentOffset3 = 35L

        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData2))("add failed")
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData3))("add failed")

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

    "addUnassignmentData" should {
      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))(
            "first add failed"
          )
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))(
            "second add failed"
          )
        } yield succeed
      }

      "detect modified reassignment data" in {
        val store = mk(indexedTargetSynchronizer)

        val updatedUnassignmentData =
          unassignmentData2.focus(_.unassignmentTs).modify(_.immediateSuccessor)
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))(
            "first add failed"
          )
          _ <- store.addUnassignmentData(updatedUnassignmentData).value
          entry <- store
            .findReassignmentEntry(unassignmentData.reassignmentId)
            .valueOrFail("lookup")
        } yield entry shouldBe ReassignmentEntry(unassignmentData, None, None)
      }

      "add several reassignments" in {
        val store = mk(indexedTargetSynchronizer)

        val unassignmentData10 =
          mkUnassignmentData(sourceSynchronizer1, Epoch, mediator1)
        val unassignmentData11 =
          mkUnassignmentData(sourceSynchronizer1, ofEpochSecond(1), mediator1)
        val unassignmentData12 =
          mkUnassignmentData(sourceSynchronizer2, ofEpochSecond(1), mediator2)

        for {

          _ <- valueOrFail(store.addUnassignmentData(unassignmentData10))(
            "first add failed"
          )
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData11))(
            "second add failed"
          )
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData12))(
            "third add failed"
          )
          lookup10 <- valueOrFail(store.lookup(unassignmentData10.reassignmentId))(
            "first reassignment not found"
          )
          lookup11 <- valueOrFail(store.lookup(unassignmentData11.reassignmentId))(
            "second reassignment not found"
          )
          lookup20 <- valueOrFail(store.lookup(unassignmentData12.reassignmentId))(
            "third reassignment not found"
          )
        } yield {
          lookup10 shouldBe unassignmentData10
          lookup11 shouldBe unassignmentData11
          lookup20 shouldBe unassignmentData12
        }
      }

      "complain about reassignments for a different synchronizer" in {
        val store = mk(IndexedSynchronizer.tryCreate(sourceSynchronizer1.unwrap, 2))
        loggerFactory.assertInternalError[IllegalArgumentException](
          store.addUnassignmentData(unassignmentData),
          _.getMessage shouldBe s"Synchronizer ${Target(sourceSynchronizer1.unwrap.logical)}: Reassignment store cannot store reassignment for synchronizer $targetSynchronizerId",
        )
      }
    }

    "completeReassignment" should {
      "mark the reassignment as completed" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          _ <- valueOrFail(store.completeReassignment(unassignmentData.reassignmentId, ts))(
            "completion failed"
          )
          lookup <- store.lookup(unassignmentData.reassignmentId).value
        } yield lookup shouldBe Left(ReassignmentCompleted(unassignmentData.reassignmentId, ts))
      }

      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          _ <- valueOrFail(store.completeReassignment(unassignmentData.reassignmentId, ts))(
            "first completion failed"
          )
          _ <- valueOrFail(store.completeReassignment(unassignmentData.reassignmentId, ts))(
            "second completion failed"
          )
        } yield succeed
      }

      "be allowed before the result" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = unassignmentData.reassignmentId
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          _ <- valueOrFail(store.completeReassignment(reassignmentId, ts))(
            "first completion failed"
          )
          lookup1 <- store.lookup(reassignmentId).value
          lookup2 <- store.lookup(reassignmentId).value
          _ <- valueOrFail(store.completeReassignment(reassignmentId, ts))(
            "second completion failed"
          )
        } yield {
          lookup1 shouldBe Left(ReassignmentCompleted(reassignmentId, ts))
          lookup2 shouldBe Left(ReassignmentCompleted(reassignmentId, ts))
        }
      }

      "store the first completion" in {
        val store = mk(indexedTargetSynchronizer)
        val ts2 = CantonTimestamp.ofEpochSecond(4)
        val reassignmentId = unassignmentData.reassignmentId
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          _ <- valueOrFail(store.completeReassignment(reassignmentId, ts2))(
            "later completion failed"
          )
          complete2 <- store.completeReassignment(reassignmentId, ts).value
          lookup <- store.lookup(reassignmentId).value
        } yield {
          complete2 shouldBe Checked.continue(ReassignmentAlreadyCompleted(reassignmentId, ts))
          lookup shouldBe Left(ReassignmentCompleted(reassignmentId, ts2))
        }
      }
    }

    "delete" should {
      "remove the reassignment" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = unassignmentData.reassignmentId
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          _ <- store.deleteReassignment(reassignmentId)
          lookup <- store.lookup(reassignmentId).value
        } yield lookup shouldBe Left(UnknownReassignmentId(reassignmentId))
      }

      "ignore unknown reassignment IDs" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = unassignmentData.reassignmentId
        for {
          () <- store.deleteReassignment(reassignmentId)
        } yield succeed
      }

      "be idempotent" in {
        val store = mk(indexedTargetSynchronizer)
        val reassignmentId = unassignmentData.reassignmentId
        for {
          _ <- valueOrFail(store.addUnassignmentData(unassignmentData))("add failed")
          _ <- store.deleteReassignment(reassignmentId)
          _ <- store.deleteReassignment(reassignmentId)
        } yield succeed
      }
    }

    "reassignment stores should be isolated" in {
      val storeTarget = mk(indexedTargetSynchronizer)
      val store1 = mk(IndexedSynchronizer.tryCreate(sourceSynchronizer1.unwrap, 2))
      for {
        _ <- valueOrFail(storeTarget.addUnassignmentData(unassignmentData))("add failed")
        found <- store1.lookup(unassignmentData.reassignmentId).value
      } yield found shouldBe Left(UnknownReassignmentId(unassignmentData.reassignmentId))
    }

    "deleteCompletionsSince" should {
      "remove the completions from the criterion on" in {
        val store = mk(indexedTargetSynchronizer)
        val ts1 = CantonTimestamp.ofEpochSecond(5)
        val ts2 = CantonTimestamp.ofEpochSecond(7)

        val aliceReassignment =
          mkUnassignmentData(
            sourceSynchronizer1,
            Epoch,
            mediator1,
            LfPartyId.assertFromString("alice"),
          )
        val bobReassignment = mkUnassignmentData(
          sourceSynchronizer1,
          ofEpochSecond(1),
          mediator1,
          LfPartyId.assertFromString("bob"),
        )
        val eveReassignment = mkUnassignmentData(
          sourceSynchronizer2,
          ofEpochSecond(1),
          mediator2,
          LfPartyId.assertFromString("eve"),
        )

        for {
          _ <- valueOrFail(store.addUnassignmentData(aliceReassignment))(
            "add alice failed"
          )
          _ <- valueOrFail(store.addUnassignmentData(bobReassignment))("add bob failed")
          _ <- valueOrFail(store.addUnassignmentData(eveReassignment))("add eve failed")
          _ <- valueOrFail(store.completeReassignment(aliceReassignment.reassignmentId, ts))(
            "completion alice failed"
          )
          _ <- valueOrFail(store.completeReassignment(bobReassignment.reassignmentId, ts1))(
            "completion bob failed"
          )
          _ <- valueOrFail(store.completeReassignment(eveReassignment.reassignmentId, ts2))(
            "completion eve failed"
          )
          _ <- store.deleteCompletionsSince(ts1)
          alice <- leftOrFail(store.lookup(aliceReassignment.reassignmentId))(
            "alice must still be completed"
          )
          bob <- valueOrFail(store.lookup(bobReassignment.reassignmentId))(
            "bob must not be completed"
          )
          eve <- valueOrFail(store.lookup(eveReassignment.reassignmentId))(
            "eve must not be completed"
          )
          _ <- valueOrFail(store.completeReassignment(bobReassignment.reassignmentId, ts2))(
            "second completion bob failed"
          )
          _ <- valueOrFail(store.completeReassignment(eveReassignment.reassignmentId, ts1))(
            "second completion eve failed"
          )
        } yield {
          alice shouldBe ReassignmentCompleted(aliceReassignment.reassignmentId, ts)
          bob shouldBe bobReassignment
          eve shouldBe eveReassignment
        }
      }
    }
  }
}

object ReassignmentStoreTest extends EitherValues with NoTracing {
  import BaseTest.*

  val alice = LfPartyId.assertFromString("alice")
  val bob = LfPartyId.assertFromString("bob")

  private def contract(id: LfContractId, signatory: LfPartyId): ContractInstance =
    ExampleTransactionFactory.asContractInstance(
      contractId = id,
      contractInstance = contractInstance(),
      ledgerTime = CreationTime.CreatedAt(LfTimestamp.Epoch),
      metadata = ContractMetadata.tryCreate(Set(signatory), Set(signatory), None),
    )()

  val coidAbs1 = suffixedId(1, 0)
  val coidAbs2 = suffixedId(2, 0)
  val contract = ExampleTransactionFactory.asContractInstance(
    contractId = coidAbs1,
    contractInstance = contractInstance(),
    ledgerTime = CreationTime.CreatedAt(LfTimestamp.Epoch),
  )()

  val synchronizer1 = SynchronizerId(
    UniqueIdentifier.tryCreate("synchronizer1", "SYNCHRONIZER1")
  ).toPhysical
  val sourceSynchronizer1 = Source(
    SynchronizerId(UniqueIdentifier.tryCreate("synchronizer1", "SYNCHRONIZER1")).toPhysical
  )
  val targetSynchronizer1 = Target(
    SynchronizerId(UniqueIdentifier.tryCreate("synchronizer1", "SYNCHRONIZER1"))
  )
  val mediator1 = MediatorGroupRecipient(MediatorGroupIndex.zero)

  val synchronizer2 = SynchronizerId(
    UniqueIdentifier.tryCreate("synchronizer2", "SYNCHRONIZER2")
  ).toPhysical
  val sourceSynchronizer2 = Source(
    SynchronizerId(UniqueIdentifier.tryCreate("synchronizer2", "SYNCHRONIZER2")).toPhysical
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

  val reassignment10 = ReassignmentId.tryCreate("0010")
  val reassignment11 = ReassignmentId.tryCreate("0011")
  val reassignment20 = ReassignmentId.tryCreate("0020")

  val loggerFactoryNotUsed = NamedLoggerFactory.unnamedKey("test", "NotUsed-ReassignmentStoreTest")
  val ec: ExecutionContext = DirectExecutionContext(
    loggerFactoryNotUsed.getLogger(ReassignmentStoreTest.getClass)
  )
  @unused
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

  def mkUnassignmentDataForSynchronizer(
      sourceMediator: MediatorGroupRecipient,
      submittingParty: LfPartyId = LfPartyId.assertFromString("submitter"),
      sourceSynchronizerId: Source[PhysicalSynchronizerId],
      targetSynchronizerId: Target[SynchronizerId],
      contract: ContractInstance = contract,
      unassignmentTs: CantonTimestamp = CantonTimestamp.Epoch,
  ): UnassignmentData = {

    val identityFactory = TestingTopology()
      .withSynchronizers(sourceSynchronizerId.unwrap)
      .build(loggerFactoryNotUsed)

    val helpers = ReassignmentDataHelpers(
      contract,
      sourceSynchronizerId,
      targetSynchronizerId.map(_.toPhysical),
      identityFactory,
    )

    val unassignmentRequest = helpers.unassignmentRequest(
      submittingParty,
      DefaultTestIdentities.participant1,
      sourceMediator,
    )()

    helpers.unassignmentData(unassignmentRequest, unassignmentTs)
  }

  private def mkUnassignmentData(
      sourceSynchronizer: Source[PhysicalSynchronizerId],
      unassignmentTs: CantonTimestamp,
      sourceMediator: MediatorGroupRecipient,
      submitter: LfPartyId = LfPartyId.assertFromString("submitter"),
      contract: ContractInstance = contract,
  ): UnassignmentData =
    mkUnassignmentDataForSynchronizer(
      sourceMediator,
      submitter,
      sourceSynchronizer,
      targetSynchronizerId,
      contract,
      unassignmentTs = unassignmentTs,
    )
}
