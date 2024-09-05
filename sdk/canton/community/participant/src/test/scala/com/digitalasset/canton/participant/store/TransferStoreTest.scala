// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata, ViewType}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.protocol.transfer.ReassignmentData.*
import com.digitalasset.canton.participant.protocol.transfer.{
  IncompleteReassignmentData,
  ReassignmentData,
  UnassignmentRequest,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.*
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  suffixedId,
  transactionId,
}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  ExampleTransactionFactory,
  LfContractId,
  ReassignmentId,
  RequestId,
  SerializableContract,
  SourceDomainId,
  TargetDomainId,
  TransactionId,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, MonadUtil}
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  LedgerApplicationId,
  LedgerCommandId,
  LfPartyId,
  ReassignmentCounter,
  RequestCounter,
  SequencerCounter,
  config,
}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, EitherValues}

import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait TransferStoreTest {
  this: AsyncWordSpec & BaseTest =>

  import TransferStoreTest.*
  private implicit val _ec: ExecutionContext = ec

  private implicit def toGlobalOffset(i: Long): GlobalOffset = GlobalOffset.tryFromLong(i)

  protected def reassignmentStore(mk: TargetDomainId => ReassignmentStore): Unit = {
    val transferData =
      config
        .NonNegativeFiniteDuration(10.seconds)
        .await("make transfer data")(mkTransferData(transfer10, mediator1))

    val transferData2 =
      config
        .NonNegativeFiniteDuration(10.seconds)
        .await("make transfer data")(mkTransferData(transfer11, mediator1))

    val transferData3 =
      config
        .NonNegativeFiniteDuration(10.seconds)
        .await("make transfer data")(mkTransferData(transfer20, mediator1))

    def reassignmentDataFor(
        reassignmentId: ReassignmentId,
        contract: SerializableContract,
        unassignmentGlobalOffset: Option[GlobalOffset] = None,
    ): ReassignmentData =
      config
        .NonNegativeFiniteDuration(10.seconds)
        .await("make reassignment data")(
          mkTransferData(
            reassignmentId,
            mediator1,
            contract = contract,
            unassignmentGlobalOffset = unassignmentGlobalOffset,
          )
        )

    val unassignmentResult = mkUnassignmentResult(transferData)
    val withUnassignmentResult = transferData.copy(unassignmentResult = Some(unassignmentResult))
    val toc = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(3))

    "lookup" should {
      "find previously stored reassignments" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          lookup10 <- valueOrFail(store.lookup(transfer10))(
            "lookup failed to find the stored transfer"
          )
        } yield assert(lookup10 == transferData, "lookup finds the stored data")
      }

      "not invent transfers" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          lookup10 <- store.lookup(transfer11).value
        } yield assert(
          lookup10 == Left(UnknownReassignmentId(transfer11)),
          "lookup finds the stored data",
        )
      }
    }

    "find" should {
      "filter by party" in {
        val store = mk(targetDomain)
        for {
          aliceTransfer <- mkTransferData(
            transfer10,
            mediator1,
            LfPartyId.assertFromString("alice"),
          )
          bobTransfer <- mkTransferData(transfer11, mediator1, LfPartyId.assertFromString("bob"))
          eveTransfer <- mkTransferData(transfer20, mediator2, LfPartyId.assertFromString("eve"))
          _ <- valueOrFail(store.addReassignment(aliceTransfer).failOnShutdown)("add alice failed")
          _ <- valueOrFail(store.addReassignment(bobTransfer).failOnShutdown)("add bob failed")
          _ <- valueOrFail(store.addReassignment(eveTransfer).failOnShutdown)("add eve failed")
          lookup <- store.find(None, None, Some(LfPartyId.assertFromString("bob")), 10)
        } yield {
          assert(lookup.toList == List(bobTransfer))
        }
      }

      "filter by timestamp" in {
        val store = mk(targetDomain)

        for {
          transfer1 <- mkTransferData(
            ReassignmentId(sourceDomain1, CantonTimestamp.ofEpochMilli(100L)),
            mediator1,
          )
          transfer2 <- mkTransferData(
            ReassignmentId(sourceDomain1, CantonTimestamp.ofEpochMilli(200L)),
            mediator1,
          )
          transfer3 <- mkTransferData(
            ReassignmentId(sourceDomain1, CantonTimestamp.ofEpochMilli(300L)),
            mediator1,
          )
          _ <- valueOrFail(store.addReassignment(transfer1).failOnShutdown)("add1 failed")
          _ <- valueOrFail(store.addReassignment(transfer2).failOnShutdown)("add2 failed")
          _ <- valueOrFail(store.addReassignment(transfer3).failOnShutdown)("add3 failed")
          lookup <- store.find(None, Some(CantonTimestamp.Epoch.plusMillis(200L)), None, 10)
        } yield {
          assert(lookup.toList == List(transfer2))
        }
      }
      "filter by domain" in {
        val store = mk(targetDomain)
        for {
          transfer1 <- mkTransferData(
            ReassignmentId(sourceDomain1, CantonTimestamp.ofEpochMilli(100L)),
            mediator1,
          )
          transfer2 <- mkTransferData(
            ReassignmentId(sourceDomain2, CantonTimestamp.ofEpochMilli(200L)),
            mediator2,
          )
          _ <- valueOrFail(store.addReassignment(transfer1).failOnShutdown)("add1 failed")
          _ <- valueOrFail(store.addReassignment(transfer2).failOnShutdown)("add2 failed")
          lookup <- store.find(Some(sourceDomain2), None, None, 10)
        } yield {
          assert(lookup.toList == List(transfer2))
        }
      }
      "limit the number of results" in {
        val store = mk(targetDomain)
        for {
          transferData10 <- mkTransferData(transfer10, mediator1)
          transferData11 <- mkTransferData(transfer11, mediator1)
          transferData20 <- mkTransferData(transfer20, mediator2)
          _ <- valueOrFail(store.addReassignment(transferData10).failOnShutdown)("first add failed")
          _ <- valueOrFail(store.addReassignment(transferData11).failOnShutdown)(
            "second add failed"
          )
          _ <- valueOrFail(store.addReassignment(transferData20).failOnShutdown)("third add failed")
          lookup <- store.find(None, None, None, 2)
        } yield {
          assert(lookup.length == 2)
        }
      }
      "apply filters conjunctively" in {
        val store = mk(targetDomain)

        for {
          // Correct timestamp
          transfer1 <- mkTransferData(
            ReassignmentId(sourceDomain1, CantonTimestamp.Epoch.plusMillis(200L)),
            mediator1,
            LfPartyId.assertFromString("party1"),
          )
          // Correct submitter
          transfer2 <- mkTransferData(
            ReassignmentId(sourceDomain1, CantonTimestamp.Epoch.plusMillis(100L)),
            mediator1,
            LfPartyId.assertFromString("party2"),
          )
          // Correct domain
          transfer3 <- mkTransferData(
            ReassignmentId(sourceDomain2, CantonTimestamp.Epoch.plusMillis(100L)),
            mediator2,
            LfPartyId.assertFromString("party2"),
          )
          // Correct transfer
          transfer4 <- mkTransferData(
            ReassignmentId(sourceDomain2, CantonTimestamp.Epoch.plusMillis(200L)),
            mediator2,
            LfPartyId.assertFromString("party2"),
          )
          _ <- valueOrFail(store.addReassignment(transfer1).failOnShutdown)("first add failed")
          _ <- valueOrFail(store.addReassignment(transfer2).failOnShutdown)("second add failed")
          _ <- valueOrFail(store.addReassignment(transfer3).failOnShutdown)("third add failed")
          _ <- valueOrFail(store.addReassignment(transfer4).failOnShutdown)("fourth add failed")
          lookup <- store.find(
            Some(sourceDomain2),
            Some(CantonTimestamp.Epoch.plusMillis(200L)),
            Some(LfPartyId.assertFromString("party2")),
            10,
          )
        } yield {
          assert(lookup.toList == List(transfer4))
        }

      }
    }

    "findAfter" should {

      def populate(store: ReassignmentStore): Future[List[ReassignmentData]] = for {
        transfer1 <- mkTransferData(
          ReassignmentId(sourceDomain1, CantonTimestamp.Epoch.plusMillis(200L)),
          mediator1,
          LfPartyId.assertFromString("party1"),
        )
        transfer2 <- mkTransferData(
          ReassignmentId(sourceDomain1, CantonTimestamp.Epoch.plusMillis(100L)),
          mediator1,
          LfPartyId.assertFromString("party2"),
        )
        transfer3 <- mkTransferData(
          ReassignmentId(sourceDomain2, CantonTimestamp.Epoch.plusMillis(100L)),
          mediator2,
          LfPartyId.assertFromString("party2"),
        )
        transfer4 <- mkTransferData(
          ReassignmentId(sourceDomain2, CantonTimestamp.Epoch.plusMillis(200L)),
          mediator2,
          LfPartyId.assertFromString("party2"),
        )
        _ <- valueOrFail(store.addReassignment(transfer1).failOnShutdown)("first add failed")
        _ <- valueOrFail(store.addReassignment(transfer2).failOnShutdown)("second add failed")
        _ <- valueOrFail(store.addReassignment(transfer3).failOnShutdown)("third add failed")
        _ <- valueOrFail(store.addReassignment(transfer4).failOnShutdown)("fourth add failed")
      } yield (List(transfer1, transfer2, transfer3, transfer4))

      "order pending transfers" in {
        val store = mk(targetDomain)

        for {
          transfers <- populate(store)
          lookup <- store.findAfter(None, 10)
        } yield {
          val List(transfer1, transfer2, transfer3, transfer4) = transfers: @unchecked
          assert(lookup == Seq(transfer2, transfer3, transfer1, transfer4))
        }

      }
      "give pending transfers after the given timestamp" in {
        val store = mk(targetDomain)

        for {
          transfers <- populate(store)
          List(transfer1, transfer2, transfer3, transfer4) = transfers: @unchecked
          lookup <- store.findAfter(
            requestAfter = Some(transfer2.reassignmentId.unassignmentTs -> transfer2.sourceDomain),
            10,
          )
        } yield {
          assert(lookup == Seq(transfer3, transfer1, transfer4))
        }
      }
      "give no pending transfers when empty" in {
        val store = mk(targetDomain)
        for { lookup <- store.findAfter(None, 10) } yield {
          lookup shouldBe empty
        }
      }
      "limit the results" in {
        val store = mk(targetDomain)

        for {
          transfers <- populate(store)
          lookup <- store.findAfter(None, 2)
        } yield {
          val List(_transfer1, transfer2, transfer3, _transfer4) = transfers: @unchecked
          assert(lookup == Seq(transfer2, transfer3))
        }
      }
      "exclude completed transfers" in {
        val store = mk(targetDomain)

        for {
          transfers <- populate(store)
          List(transfer1, transfer2, transfer3, transfer4) = transfers: @unchecked
          checked <- store
            .completeReasignment(
              transfer2.reassignmentId,
              TimeOfChange(RequestCounter(3), CantonTimestamp.Epoch.plusSeconds(3)),
            )
            .value
          lookup <- store.findAfter(None, 10)
        } yield {
          assert(checked.successful)
          assert(lookup == Seq(transfer3, transfer1, transfer4))
        }

      }
    }

    "add unassignment/in global offsets" should {

      val reassignmentId = transferData.reassignmentId

      val unassignmentOffset = UnassignmentGlobalOffset(10L)
      val assignmentOffset = AssignmentGlobalOffset(15L)

      val transferDataOnlyOut =
        transferData.copy(reassignmentGlobalOffset =
          Some(UnassignmentGlobalOffset(unassignmentOffset.offset))
        )
      val transferDataTransferComplete = transferData.copy(reassignmentGlobalOffset =
        Some(
          ReassignmentGlobalOffsets.create(unassignmentOffset.offset, assignmentOffset.offset).value
        )
      )

      "allow batch updates" in {
        val store = mk(targetDomain)

        val data = (1L until 13).flatMap { i =>
          val tid = reassignmentId.copy(unassignmentTs = CantonTimestamp.ofEpochSecond(i))
          val transferData = reassignmentDataFor(tid, contract)

          val mod = 4

          if (i % mod == 0)
            Seq((UnassignmentGlobalOffset(i * 10), transferData))
          else if (i % mod == 1)
            Seq((AssignmentGlobalOffset(i * 10), transferData))
          else if (i % mod == 2)
            Seq((ReassignmentGlobalOffsets.create(i * 10, i * 10 + 1).value, transferData))
          else
            Seq(
              (UnassignmentGlobalOffset(i * 10), transferData),
              (AssignmentGlobalOffset(i * 10 + 1), transferData),
            )

        }

        for {
          _ <- valueOrFail(MonadUtil.sequentialTraverse_(data) { case (_, transferData) =>
            store.addReassignment(transferData).failOnShutdown
          })("add transfers")

          offsets = data.map { case (offset, transferData) =>
            transferData.reassignmentId -> offset
          }

          _ <- store.addReassignmentsOffsets(offsets).valueOrFailShutdown("adding offsets")

          result <- valueOrFail(offsets.toList.parTraverse { case (reassignmentId, _) =>
            store.lookup(reassignmentId)
          })("query transfers")
        } yield {
          result.lengthCompare(offsets) shouldBe 0

          forEvery(result.zip(offsets)) {
            case (retrievedTransferData, (reassignmentId, expectedOffset)) =>
              withClue(s"got unexpected data for transfer $reassignmentId") {
                expectedOffset match {
                  case UnassignmentGlobalOffset(out) =>
                    retrievedTransferData.unassignmentGlobalOffset shouldBe Some(out)

                  case AssignmentGlobalOffset(in) =>
                    retrievedTransferData.assignmentGlobalOffset shouldBe Some(in)

                  case ReassignmentGlobalOffsets(out, in) =>
                    retrievedTransferData.unassignmentGlobalOffset shouldBe Some(out)
                    retrievedTransferData.assignmentGlobalOffset shouldBe Some(in)
                }
              }
          }
        }
      }

      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFailShutdown(
              "add unassignment offset 1"
            )

          lookupOnlyUnassignment1 <- valueOrFail(store.lookup(reassignmentId))(
            "lookup transfer data"
          )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .valueOrFailShutdown(
              "add unassignment offset 2"
            )

          lookupOnlyUnassignment2 <- valueOrFail(store.lookup(reassignmentId))(
            "lookup transfer data"
          )

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFailShutdown(
              "add assignment offset 1"
            )

          lookup1 <- valueOrFail(store.lookup(reassignmentId))("lookup transfer data")

          _ <- store
            .addReassignmentsOffsets(Map(reassignmentId -> assignmentOffset))
            .valueOrFailShutdown(
              "add assignment offset 2"
            )

          lookup2 <- valueOrFail(store.lookup(reassignmentId))("lookup transfer data")

        } yield {
          lookupOnlyUnassignment1 shouldBe transferDataOnlyOut
          lookupOnlyUnassignment2 shouldBe transferDataOnlyOut

          lookup1 shouldBe transferDataTransferComplete
          lookup2 shouldBe transferDataTransferComplete
        }
      }

      "return an error if assignment offset is the same as the unassignment" in {
        val store = mk(targetDomain)

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add")

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
        val store = mk(targetDomain)

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add")

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
        val store = mk(targetDomain)

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add")

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

          lookup1 <- valueOrFail(store.lookup(reassignmentId))("lookup transfer data")

          successfulAddOutOffset <- store
            .addReassignmentsOffsets(Map(reassignmentId -> unassignmentOffset))
            .value
            .failOnShutdown
          failedAddOutOffset <- store
            .addReassignmentsOffsets(
              Map(
                reassignmentId -> UnassignmentGlobalOffset(
                  GlobalOffset.tryFromLong(unassignmentOffset.offset.toLong - 1)
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
                  GlobalOffset.tryFromLong(assignmentOffset.offset.toLong - 1)
                )
              )
            )
            .value
            .failOnShutdown

          lookup2 <- valueOrFail(store.lookup(reassignmentId))("lookup transfer data")

        } yield {
          successfulAddOutOffset.value shouldBe ()
          failedAddOutOffset.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]

          successfulAddInOffset.value shouldBe ()
          failedAddInOffset.left.value shouldBe a[ReassignmentGlobalOffsetsMerge]

          lookup1 shouldBe transferDataTransferComplete
          lookup2 shouldBe transferDataTransferComplete
        }
      }
    }

    "incomplete" should {
      val limit = NonNegativeInt.tryCreate(10)

      def assertIsIncomplete(
          incompletes: Seq[IncompleteReassignmentData],
          expectedTransferData: ReassignmentData,
      ): Assertion =
        incompletes.map(_.toTransferData) shouldBe Seq(expectedTransferData)

      "list incomplete transfers (unassignment done)" in {
        val store = mk(targetDomain)
        val reassignmentId = transferData.reassignmentId

        val unassignmentOsset = 10L
        val assignmentOffset = 20L

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          lookupNoOffset <- store.findIncomplete(None, Long.MaxValue, None, limit)

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOsset))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )
          lookupBeforeUnassignment <- store.findIncomplete(
            None,
            unassignmentOsset - 1,
            None,
            limit,
          )
          lookupAtUnassignment <- store.findIncomplete(None, unassignmentOsset, None, limit)

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )

          lookupBeforeAssignment <- store.findIncomplete(
            None,
            assignmentOffset - 1,
            None,
            limit,
          )
          lookupAtAssignment <- store.findIncomplete(None, assignmentOffset, None, limit)
          lookupAfterAssignment <- store.findIncomplete(
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
            transferData.copy(reassignmentGlobalOffset =
              Some(UnassignmentGlobalOffset(unassignmentOsset))
            ),
          )

          assertIsIncomplete(
            lookupBeforeAssignment,
            transferData.copy(reassignmentGlobalOffset =
              Some(UnassignmentGlobalOffset(unassignmentOsset))
            ),
          )

          lookupAtAssignment shouldBe empty
          lookupAfterAssignment shouldBe empty
        }
      }

      "list incomplete transfers (assignment done)" in {
        val store = mk(targetDomain)
        val reassignmentId = transferData.reassignmentId

        val assignmentOffset = 10L
        val unassignmentOffset = 20L

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          lookupNoOffset <- store.findIncomplete(None, Long.MaxValue, None, limit)

          _ <-
            store
              .addReassignmentsOffsets(
                Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
              )
              .valueOrFailShutdown(
                "add assignment offset failed"
              )
          lookupBeforeAssignment <- store.findIncomplete(
            None,
            assignmentOffset - 1,
            None,
            limit,
          )
          lookupAtAssignment <- store.findIncomplete(None, assignmentOffset, None, limit)

          _ <-
            store
              .addReassignmentsOffsets(
                Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
              )
              .valueOrFailShutdown(
                "add unassignment offset failed"
              )

          lookupBeforeUnassignment <- store.findIncomplete(
            None,
            unassignmentOffset - 1,
            None,
            limit,
          )
          lookupAtUnassignment <- store.findIncomplete(None, unassignmentOffset, None, limit)
          lookupAfterUnassignment <- store.findIncomplete(
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
            transferData.copy(reassignmentGlobalOffset =
              Some(AssignmentGlobalOffset(assignmentOffset))
            ),
          )

          assertIsIncomplete(
            lookupBeforeUnassignment,
            transferData.copy(reassignmentGlobalOffset =
              Some(AssignmentGlobalOffset(assignmentOffset))
            ),
          )

          lookupAtUnassignment shouldBe empty
          lookupAfterUnassignment shouldBe empty
        }
      }

      "take stakeholders filter into account" in {
        val store = mk(targetDomain)

        val alice = TransferStoreTest.alice
        val bob = TransferStoreTest.bob

        val aliceContract = TransferStoreTest.contract(TransferStoreTest.coidAbs1, alice)
        val bobContract = TransferStoreTest.contract(TransferStoreTest.coidAbs2, bob)

        val unassignmentOffset = 42L

        val contracts = Seq(aliceContract, bobContract, aliceContract, bobContract)
        val transfersData = contracts.zipWithIndex.map { case (contract, idx) =>
          val reassignmentId =
            ReassignmentId(sourceDomain1, CantonTimestamp.Epoch.plusSeconds(idx.toLong))

          reassignmentDataFor(
            reassignmentId,
            contract,
            unassignmentGlobalOffset = Some(unassignmentOffset),
          )
        }
        val stakeholders = contracts.map(_.metadata.stakeholders)

        val addTransfersET = transfersData.parTraverse(store.addReassignment)

        def lift(stakeholder: LfPartyId, others: LfPartyId*): Option[NonEmpty[Set[LfPartyId]]] =
          Option(NonEmpty(Set, stakeholder, others*))

        def stakeholdersOf(
            incompleteTransfers: Seq[IncompleteReassignmentData]
        ): Seq[Set[LfPartyId]] =
          incompleteTransfers.map(_.contract.metadata.stakeholders)

        for {
          _ <- valueOrFail(addTransfersET.failOnShutdown)("add failed")

          lookupNone <- store.findIncomplete(None, unassignmentOffset, None, limit)
          lookupAll <- store.findIncomplete(
            None,
            unassignmentOffset,
            lift(alice, bob),
            limit,
          )

          lookupAlice <- store.findIncomplete(None, unassignmentOffset, lift(alice), limit)
          lookupBob <- store.findIncomplete(None, unassignmentOffset, lift(bob), limit)
        } yield {
          stakeholdersOf(lookupNone) should contain theSameElementsAs stakeholders
          stakeholdersOf(lookupAll) should contain theSameElementsAs stakeholders
          stakeholdersOf(lookupAlice) should contain theSameElementsAs Seq(Set(alice), Set(alice))
          stakeholdersOf(lookupBob) should contain theSameElementsAs Seq(Set(bob), Set(bob))
        }
      }

      "take domain filter into account" in {
        val store = mk(targetDomain)
        val offset = 10L

        val transfer =
          transferData.copy(reassignmentGlobalOffset = Some(AssignmentGlobalOffset(offset)))

        for {
          _ <- valueOrFail(store.addReassignment(transfer).failOnShutdown)("add")

          lookup1a <- store.findIncomplete(Some(sourceDomain2), offset, None, limit) // Wrong domain
          lookup1b <- store.findIncomplete(Some(sourceDomain1), offset, None, limit)
          lookup1c <- store.findIncomplete(None, offset, None, limit)
        } yield {
          lookup1a shouldBe empty
          assertIsIncomplete(lookup1b, transfer)
          assertIsIncomplete(lookup1c, transfer)
        }
      }

      "limit the results" in {
        val store = mk(targetDomain)
        val offset = 42L

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add")
          _ <- store
            .addReassignmentsOffsets(
              Map(transferData.reassignmentId -> UnassignmentGlobalOffset(offset))
            )
            .valueOrFailShutdown("add out offset")

          lookup0 <- store.findIncomplete(None, offset, None, NonNegativeInt.zero)
          lookup1 <- store.findIncomplete(None, offset, None, NonNegativeInt.one)

        } yield {
          lookup0 shouldBe empty
          lookup1 should have size 1
        }
      }
    }

    "find first incomplete" should {

      "find incomplete transfers (unassignment done)" in {
        val store = mk(targetDomain)
        val reassignmentId = transferData.reassignmentId

        val unassignmentOffset = 10L
        val assignmentOffset = 20L
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )
          lookupAfterUnassignment <- store.findEarliestIncomplete()

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )

          lookupAfterAssignment <- store.findEarliestIncomplete()
        } yield {
          inside(lookupAfterUnassignment) { case Some((offset, _, _)) =>
            offset shouldBe GlobalOffset.tryFromLong(unassignmentOffset)
          }
          lookupAfterAssignment shouldBe None
        }
      }

      "find incomplete transfers (assignment done)" in {
        val store = mk(targetDomain)
        val reassignmentId = transferData.reassignmentId

        val unassignmentOffset = 10L
        val assignmentOffset = 20L

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> AssignmentGlobalOffset(assignmentOffset))
            )
            .valueOrFailShutdown(
              "add assignment offset failed"
            )
          lookupAfterAssignment <- store.findEarliestIncomplete()

          _ <- store
            .addReassignmentsOffsets(
              Map(reassignmentId -> UnassignmentGlobalOffset(unassignmentOffset))
            )
            .valueOrFailShutdown(
              "add unassignment offset failed"
            )
          lookupAfterUnassignment <- store.findEarliestIncomplete()

        } yield {
          inside(lookupAfterAssignment) { case Some((offset, _, _)) =>
            offset shouldBe GlobalOffset.tryFromLong(assignmentOffset)
          }
          lookupAfterUnassignment shouldBe None
        }
      }

      "returns None when transfer store is empty or each transfer is either complete or has no offset information" in {
        val store = mk(targetDomain)
        val reassignmentId1 = transferData.reassignmentId
        val reassignmentId3 = transferData3.reassignmentId

        val unassignmentOffset1 = 10L
        val assignmentOffset1 = 20L

        val unassignmentOffset3 = 30L
        val assignmentOffset3 = 35L

        for {
          lookupEmpty <- store.findEarliestIncomplete()
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addReassignment(transferData3).failOnShutdown)("add failed")

          lookupAllInFlight <- store.findEarliestIncomplete()

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

          lookupInFlightOrComplete <- store.findEarliestIncomplete()

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
          lookupAllComplete <- store.findEarliestIncomplete()

        } yield {
          lookupEmpty shouldBe None
          lookupAllInFlight shouldBe None
          lookupInFlightOrComplete shouldBe None
          lookupAllComplete shouldBe None
        }
      }

      "works in complex scenario" in {
        val store = mk(targetDomain)
        val reassignmentId1 = transferData.reassignmentId
        val reassignmentId2 = transferData2.reassignmentId
        val reassignmentId3 = transferData3.reassignmentId

        val unassignmentOffset1 = 10L
        val assignmentOffset1 = 20L

        // transfer 2 is incomplete
        val unassignmentOffset2 = 12L

        val unassignmentOffset3 = 30L
        val assignmentOffset3 = 35L

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addReassignment(transferData2).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addReassignment(transferData3).failOnShutdown)("add failed")

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

          lookupEnd <- store.findEarliestIncomplete()

        } yield {
          inside(lookupEnd) { case Some((offset, _, _)) =>
            offset shouldBe GlobalOffset.tryFromLong(unassignmentOffset2)
          }
        }
      }
    }

    "addTransfer" should {
      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("first add failed")
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("second add failed")
        } yield succeed
      }

      "detect modified transfer data" in {
        val store = mk(targetDomain)
        val modifiedContract =
          asSerializable(
            transferData.contract.contractId,
            contractInstance(),
            contract.metadata,
            CantonTimestamp.ofEpochMilli(1),
          )
        val transferDataModified = transferData.copy(contract = modifiedContract)

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("first add failed")
          add2 <- store.addReassignment(transferDataModified).failOnShutdown.value
        } yield assert(
          add2 == Left(ReassignmentDataAlreadyExists(transferData, transferDataModified)),
          "second add failed",
        )
      }

      "handle unassignment results" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(withUnassignmentResult).failOnShutdown)(
            "first add failed"
          )
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("second add failed")
          lookup2 <- valueOrFail(store.lookup(transfer10))("UnassignmentResult missing")
          _ <- valueOrFail(store.addReassignment(withUnassignmentResult).failOnShutdown)(
            "third add failed"
          )
        } yield assert(lookup2 == withUnassignmentResult, "UnassignmentResult remains")
      }

      "add several transfers" in {
        val store = mk(targetDomain)
        for {
          transferData10 <- mkTransferData(transfer10, mediator1)
          transferData11 <- mkTransferData(transfer11, mediator1)
          transferData20 <- mkTransferData(transfer20, mediator2)
          _ <- valueOrFail(store.addReassignment(transferData10).failOnShutdown)("first add failed")
          _ <- valueOrFail(store.addReassignment(transferData11).failOnShutdown)(
            "second add failed"
          )
          _ <- valueOrFail(store.addReassignment(transferData20).failOnShutdown)("third add failed")
          lookup10 <- valueOrFail(store.lookup(transfer10))("first transfer not found")
          lookup11 <- valueOrFail(store.lookup(transfer11))("second transfer not found")
          lookup20 <- valueOrFail(store.lookup(transfer20))("third transfer not found")
        } yield {
          lookup10 shouldBe transferData10
          lookup11 shouldBe transferData11
          lookup20 shouldBe transferData20
        }
      }

      "complain about transfers for a different domain" in {
        val store = mk(TargetDomainId(sourceDomain1.unwrap))
        loggerFactory.assertInternalError[IllegalArgumentException](
          store.addReassignment(transferData),
          _.getMessage shouldBe "Domain domain1::DOMAIN1: Reassignment store cannot store reassignment for domain target::DOMAIN",
        )
      }
    }

    "addUnassignmentResult" should {

      "report missing transfers" in {
        val store = mk(targetDomain)
        for {
          missing <- store.addUnassignmentResult(unassignmentResult).failOnShutdown.value
        } yield missing shouldBe Left(UnknownReassignmentId(transfer10))
      }

      "add the result" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          lookup <- valueOrFail(store.lookup(transfer10))("transfer not found")
        } yield assert(
          lookup == transferData.copy(unassignmentResult = Some(unassignmentResult)),
          "result is stored",
        )
      }

      "report mismatching results" in {
        val store = mk(targetDomain)
        val modifiedUnassignmentResult = unassignmentResult.copy(
          result = unassignmentResult.result.copy(
            content =
              unassignmentResult.result.content.copy(timestamp = CantonTimestamp.ofEpochSecond(2))
          )
        )
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          modified <- store.addUnassignmentResult(modifiedUnassignmentResult).failOnShutdown.value
          lookup <- valueOrFail(store.lookup(transfer10))("transfer not found")
        } yield {
          assert(
            modified == Left(
              UnassignmentResultAlreadyExists(
                transfer10,
                unassignmentResult,
                modifiedUnassignmentResult,
              )
            ),
            "modified result is flagged",
          )
          assert(
            lookup == transferData.copy(unassignmentResult = Some(unassignmentResult)),
            "result is not overwritten stored",
          )
        }
      }
    }

    "completeTransfer" should {
      "mark the transfer as completed" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReasignment(transfer10, toc))("completion failed")
          lookup <- store.lookup(transfer10).value
        } yield lookup shouldBe Left(ReassignmentCompleted(transfer10, toc))
      }

      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReasignment(transfer10, toc))("first completion failed")
          _ <- valueOrFail(store.completeReasignment(transfer10, toc))("second completion failed")
        } yield succeed
      }

      "be allowed before the result" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.completeReasignment(transfer10, toc))("first completion failed")
          lookup1 <- store.lookup(transfer10).value
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          lookup2 <- store.lookup(transfer10).value
          _ <- valueOrFail(store.completeReasignment(transfer10, toc))("second completion failed")
        } yield {
          lookup1 shouldBe Left(ReassignmentCompleted(transfer10, toc))
          lookup2 shouldBe Left(ReassignmentCompleted(transfer10, toc))
        }
      }

      "detect mismatches" in {
        val store = mk(targetDomain)
        val toc2 = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(4))
        val modifiedTransferData =
          transferData.copy(unassignmentRequestCounter = RequestCounter(100))
        val modifiedUnassignmentResult = unassignmentResult.copy(
          result = unassignmentResult.result.copy(content =
            unassignmentResult.result.content.copy(counter = SequencerCounter(120))
          )
        )

        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReasignment(transfer10, toc))("first completion failed")
          complete2 <- store.completeReasignment(transfer10, toc2).value
          add2 <- store.addReassignment(modifiedTransferData).failOnShutdown.value
          addResult2 <- store.addUnassignmentResult(modifiedUnassignmentResult).failOnShutdown.value
        } yield {
          complete2 shouldBe Checked.continue(ReassignmentAlreadyCompleted(transfer10, toc2))
          add2 shouldBe Left(
            ReassignmentDataAlreadyExists(withUnassignmentResult, modifiedTransferData)
          )
          addResult2 shouldBe Left(
            UnassignmentResultAlreadyExists(
              transfer10,
              unassignmentResult,
              modifiedUnassignmentResult,
            )
          )
        }
      }

      "store the first completion" in {
        val store = mk(targetDomain)
        val toc2 = TimeOfChange(RequestCounter(1), CantonTimestamp.ofEpochSecond(4))
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReasignment(transfer10, toc2))("later completion failed")
          complete2 <- store.completeReasignment(transfer10, toc).value
          lookup <- store.lookup(transfer10).value
        } yield {
          complete2 shouldBe Checked.continue(ReassignmentAlreadyCompleted(transfer10, toc))
          lookup shouldBe Left(ReassignmentCompleted(transfer10, toc2))
        }
      }

    }

    "delete" should {
      "remove the transfer" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          _ <- store.deleteReassignment(transfer10)
          lookup <- store.lookup(transfer10).value
        } yield lookup shouldBe Left(UnknownReassignmentId(transfer10))
      }

      "purge completed transfers" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          _ <- valueOrFail(store.addUnassignmentResult(unassignmentResult).failOnShutdown)(
            "addResult failed"
          )
          _ <- valueOrFail(store.completeReasignment(transfer10, toc))("completion failed")
          _ <- store.deleteReassignment(transfer10)
        } yield succeed
      }

      "ignore unknown reassignment IDs" in {
        val store = mk(targetDomain)
        for {
          () <- store.deleteReassignment(transfer10)
        } yield succeed
      }

      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addReassignment(transferData).failOnShutdown)("add failed")
          () <- store.deleteReassignment(transfer10)
          () <- store.deleteReassignment(transfer10)
        } yield succeed
      }
    }

    "transfer stores should be isolated" in {
      val storeTarget = mk(targetDomain)
      val store1 = mk(TargetDomainId(sourceDomain1.unwrap))
      for {
        _ <- valueOrFail(storeTarget.addReassignment(transferData).failOnShutdown)("add failed")
        found <- store1.lookup(transferData.reassignmentId).value
      } yield found shouldBe Left(UnknownReassignmentId(transferData.reassignmentId))
    }

    "deleteCompletionsSince" should {
      "remove the completions from the criterion on" in {
        val store = mk(targetDomain)
        val toc1 = TimeOfChange(RequestCounter(1), CantonTimestamp.ofEpochSecond(5))
        val toc2 = TimeOfChange(RequestCounter(2), CantonTimestamp.ofEpochSecond(7))

        for {
          aliceTransfer <-
            mkTransferData(transfer10, mediator1, LfPartyId.assertFromString("alice"))
          bobTransfer <- mkTransferData(transfer11, mediator1, LfPartyId.assertFromString("bob"))
          eveTransfer <- mkTransferData(transfer20, mediator2, LfPartyId.assertFromString("eve"))
          _ <- valueOrFail(store.addReassignment(aliceTransfer).failOnShutdown)("add alice failed")
          _ <- valueOrFail(store.addReassignment(bobTransfer).failOnShutdown)("add bob failed")
          _ <- valueOrFail(store.addReassignment(eveTransfer).failOnShutdown)("add eve failed")
          _ <- valueOrFail(store.completeReasignment(transfer10, toc))("completion alice failed")
          _ <- valueOrFail(store.completeReasignment(transfer11, toc1))("completion bob failed")
          _ <- valueOrFail(store.completeReasignment(transfer20, toc2))("completion eve failed")
          _ <- store.deleteCompletionsSince(RequestCounter(1))
          alice <- leftOrFail(store.lookup(transfer10))("alice must still be completed")
          bob <- valueOrFail(store.lookup(transfer11))("bob must not be completed")
          eve <- valueOrFail(store.lookup(transfer20))("eve must not be completed")
          _ <- valueOrFail(store.completeReasignment(transfer11, toc2))(
            "second completion bob failed"
          )
          _ <- valueOrFail(store.completeReasignment(transfer20, toc1))(
            "second completion eve failed"
          )
        } yield {
          alice shouldBe ReassignmentCompleted(transfer10, toc)
          bob shouldBe bobTransfer
          eve shouldBe eveTransfer
        }
      }
    }
  }
}

object TransferStoreTest extends EitherValues with NoTracing {

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
  val transactionId1 = transactionId(1)

  val domain1 = DomainId(UniqueIdentifier.tryCreate("domain1", "DOMAIN1"))
  val sourceDomain1 = SourceDomainId(DomainId(UniqueIdentifier.tryCreate("domain1", "DOMAIN1")))
  val targetDomain1 = TargetDomainId(DomainId(UniqueIdentifier.tryCreate("domain1", "DOMAIN1")))
  val mediator1 = MediatorGroupRecipient(MediatorGroupIndex.zero)

  val domain2 = DomainId(UniqueIdentifier.tryCreate("domain2", "DOMAIN2"))
  val sourceDomain2 = SourceDomainId(DomainId(UniqueIdentifier.tryCreate("domain2", "DOMAIN2")))
  val targetDomain2 = TargetDomainId(DomainId(UniqueIdentifier.tryCreate("domain2", "DOMAIN2")))
  val mediator2 = MediatorGroupRecipient(MediatorGroupIndex.one)

  val targetDomain = TargetDomainId(DomainId(UniqueIdentifier.tryCreate("target", "DOMAIN")))

  val transfer10 = ReassignmentId(sourceDomain1, CantonTimestamp.Epoch)
  val transfer11 = ReassignmentId(sourceDomain1, CantonTimestamp.ofEpochMilli(1))
  val transfer20 = ReassignmentId(sourceDomain2, CantonTimestamp.Epoch)

  val loggerFactoryNotUsed = NamedLoggerFactory.unnamedKey("test", "NotUsed-TransferStoreTest")
  val ec: ExecutionContext = DirectExecutionContext(
    loggerFactoryNotUsed.getLogger(TransferStoreTest.getClass)
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
    crypto.sign(hash, sequencerKey.id)
  }

  private val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis

  val seedGenerator = new SeedGenerator(crypto.pureCrypto)

  private def submitterMetadata(submitter: LfPartyId): TransferSubmitterMetadata = {

    val submittingParticipant: ParticipantId = DefaultTestIdentities.participant1

    val applicationId: LedgerApplicationId =
      LedgerApplicationId.assertFromString("application-tests")

    val commandId: LedgerCommandId = LedgerCommandId.assertFromString("transfer-store-command-id")

    TransferSubmitterMetadata(
      submitter,
      submittingParticipant,
      commandId,
      submissionId = None,
      applicationId,
      workflowId = None,
    )
  }

  def mkTransferDataForDomain(
      reassignmentId: ReassignmentId,
      sourceMediator: MediatorGroupRecipient,
      submittingParty: LfPartyId = LfPartyId.assertFromString("submitter"),
      targetDomainId: TargetDomainId,
      creatingTransactionId: TransactionId = ExampleTransactionFactory.transactionId(0),
      contract: SerializableContract = contract,
      unassignmentGlobalOffset: Option[GlobalOffset] = None,
  ): Future[ReassignmentData] = {

    val unassignmentRequest = UnassignmentRequest(
      submitterMetadata(submittingParty),
      Set(submittingParty),
      Set.empty,
      creatingTransactionId,
      contract,
      reassignmentId.sourceDomain,
      SourceProtocolVersion(BaseTest.testedProtocolVersion),
      sourceMediator,
      targetDomainId,
      TargetProtocolVersion(BaseTest.testedProtocolVersion),
      TimeProofTestUtil.mkTimeProof(
        timestamp = CantonTimestamp.Epoch,
        targetDomain = targetDomainId,
      ),
      initialReassignmentCounter,
    )
    val uuid = new UUID(10L, 0L)
    val seed = seedGenerator.generateSaltSeed()
    val fullUnassignmentViewTree = unassignmentRequest
      .toFullUnassignmentTree(
        crypto.pureCrypto,
        crypto.pureCrypto,
        seed,
        uuid,
      )
    Future.successful(
      ReassignmentData(
        sourceProtocolVersion = SourceProtocolVersion(BaseTest.testedProtocolVersion),
        unassignmentTs = reassignmentId.unassignmentTs,
        unassignmentRequestCounter = RequestCounter(0),
        unassignmentRequest = fullUnassignmentViewTree,
        unassignmentDecisionTime = CantonTimestamp.ofEpochSecond(10),
        contract = contract,
        creatingTransactionId = transactionId1,
        unassignmentResult = None,
        reassignmentGlobalOffset = unassignmentGlobalOffset.map(UnassignmentGlobalOffset),
      )
    )
  }

  private def mkTransferData(
      reassignmentId: ReassignmentId,
      sourceMediator: MediatorGroupRecipient,
      submitter: LfPartyId = LfPartyId.assertFromString("submitter"),
      creatingTransactionId: TransactionId = transactionId1,
      contract: SerializableContract = contract,
      unassignmentGlobalOffset: Option[GlobalOffset] = None,
  ): Future[ReassignmentData] =
    mkTransferDataForDomain(
      reassignmentId,
      sourceMediator,
      submitter,
      targetDomain,
      creatingTransactionId,
      contract,
      unassignmentGlobalOffset,
    )

  def mkUnassignmentResult(reassignmentData: ReassignmentData): DeliveredUnassignmentResult =
    DeliveredUnassignmentResult {
      val requestId = RequestId(reassignmentData.unassignmentTs)

      val mediatorMessage =
        reassignmentData.unassignmentRequest.tree.mediatorMessage(Signature.noSignature)
      val result = ConfirmationResultMessage.create(
        mediatorMessage.domainId,
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
        reassignmentData.sourceDomain.unwrap,
        Some(MessageId.tryCreate("1")),
        batch,
        Some(reassignmentData.unassignmentTs),
        BaseTest.testedProtocolVersion,
        Option.empty[TrafficReceipt],
      )
      SignedContent(
        deliver,
        sign("UnassignmentResult-sequencer"),
        None,
        BaseTest.testedProtocolVersion,
      )
    }
}
