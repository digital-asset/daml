// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.{
  CantonTimestamp,
  TransferInView,
  TransferOutView,
  TransferSubmitterMetadata,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.protocol.transfer.TransferData.*
import com.digitalasset.canton.participant.protocol.transfer.{
  IncompleteTransferData,
  TransferData,
  TransferOutRequest,
}
import com.digitalasset.canton.participant.store.TransferStore.*
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
  LfTemplateId,
  RequestId,
  SerializableContract,
  SourceDomainId,
  TargetDomainId,
  TransactionId,
  TransferId,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, FutureUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  LedgerApplicationId,
  LedgerCommandId,
  LedgerParticipantId,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, EitherValues}

import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.implicitConversions

trait TransferStoreTest {
  this: AsyncWordSpec with BaseTest =>

  import TransferStoreTest.*
  private implicit val _ec: ExecutionContext = ec

  private implicit def toGlobalOffset(i: Long): GlobalOffset = GlobalOffset.tryFromLong(i)

  protected def transferStore(mk: TargetDomainId => TransferStore): Unit = {
    val transferData = FutureUtil.noisyAwaitResult(
      mkTransferData(transfer10, mediator1),
      "make transfer data",
      10.seconds,
    )

    def transferDataFor(
        transferId: TransferId,
        contract: SerializableContract,
        transferOutGlobalOffset: Option[GlobalOffset] = None,
    ) =
      FutureUtil.noisyAwaitResult(
        mkTransferData(
          transferId,
          mediator1,
          contract = contract,
          transferOutGlobalOffset = transferOutGlobalOffset,
        ),
        "make transfer data",
        10.seconds,
      )

    val transferOutResult = mkTransferOutResult(transferData)
    val withTransferOutResult = transferData.copy(transferOutResult = Some(transferOutResult))
    val toc = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(3))

    "lookup" should {
      "find previously stored transfers" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          lookup10 <- valueOrFail(store.lookup(transfer10))(
            "lookup failed to find the stored transfer"
          )
        } yield assert(lookup10 == transferData, "lookup finds the stored data")
      }

      "not invent transfers" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          lookup10 <- store.lookup(transfer11).value
        } yield assert(
          lookup10 == Left(UnknownTransferId(transfer11)),
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
          _ <- valueOrFail(store.addTransfer(aliceTransfer))("add alice failed")
          _ <- valueOrFail(store.addTransfer(bobTransfer))("add bob failed")
          _ <- valueOrFail(store.addTransfer(eveTransfer))("add eve failed")
          lookup <- store.find(None, None, Some(LfPartyId.assertFromString("bob")), 10)
        } yield {
          assert(lookup.toList == List(bobTransfer))
        }
      }

      "filter by timestamp" in {
        val store = mk(targetDomain)

        for {
          transfer1 <- mkTransferData(
            TransferId(sourceDomain1, CantonTimestamp.ofEpochMilli(100L)),
            mediator1,
          )
          transfer2 <- mkTransferData(
            TransferId(sourceDomain1, CantonTimestamp.ofEpochMilli(200L)),
            mediator1,
          )
          transfer3 <- mkTransferData(
            TransferId(sourceDomain1, CantonTimestamp.ofEpochMilli(300L)),
            mediator1,
          )
          _ <- valueOrFail(store.addTransfer(transfer1))("add1 failed")
          _ <- valueOrFail(store.addTransfer(transfer2))("add2 failed")
          _ <- valueOrFail(store.addTransfer(transfer3))("add3 failed")
          lookup <- store.find(None, Some(CantonTimestamp.Epoch.plusMillis(200L)), None, 10)
        } yield {
          assert(lookup.toList == List(transfer2))
        }
      }
      "filter by domain" in {
        val store = mk(targetDomain)
        for {
          transfer1 <- mkTransferData(
            TransferId(sourceDomain1, CantonTimestamp.ofEpochMilli(100L)),
            mediator1,
          )
          transfer2 <- mkTransferData(
            TransferId(sourceDomain2, CantonTimestamp.ofEpochMilli(200L)),
            mediator2,
          )
          _ <- valueOrFail(store.addTransfer(transfer1))("add1 failed")
          _ <- valueOrFail(store.addTransfer(transfer2))("add2 failed")
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
          _ <- valueOrFail(store.addTransfer(transferData10))("first add failed")
          _ <- valueOrFail(store.addTransfer(transferData11))("second add failed")
          _ <- valueOrFail(store.addTransfer(transferData20))("third add failed")
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
            TransferId(sourceDomain1, CantonTimestamp.Epoch.plusMillis(200L)),
            mediator1,
            LfPartyId.assertFromString("party1"),
          )
          // Correct submitter
          transfer2 <- mkTransferData(
            TransferId(sourceDomain1, CantonTimestamp.Epoch.plusMillis(100L)),
            mediator1,
            LfPartyId.assertFromString("party2"),
          )
          // Correct domain
          transfer3 <- mkTransferData(
            TransferId(sourceDomain2, CantonTimestamp.Epoch.plusMillis(100L)),
            mediator2,
            LfPartyId.assertFromString("party2"),
          )
          // Correct transfer
          transfer4 <- mkTransferData(
            TransferId(sourceDomain2, CantonTimestamp.Epoch.plusMillis(200L)),
            mediator2,
            LfPartyId.assertFromString("party2"),
          )
          _ <- valueOrFail(store.addTransfer(transfer1))("first add failed")
          _ <- valueOrFail(store.addTransfer(transfer2))("second add failed")
          _ <- valueOrFail(store.addTransfer(transfer3))("third add failed")
          _ <- valueOrFail(store.addTransfer(transfer4))("fourth add failed")
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

      def populate(store: TransferStore): Future[List[TransferData]] = for {
        transfer1 <- mkTransferData(
          TransferId(sourceDomain1, CantonTimestamp.Epoch.plusMillis(200L)),
          mediator1,
          LfPartyId.assertFromString("party1"),
        )
        transfer2 <- mkTransferData(
          TransferId(sourceDomain1, CantonTimestamp.Epoch.plusMillis(100L)),
          mediator1,
          LfPartyId.assertFromString("party2"),
        )
        transfer3 <- mkTransferData(
          TransferId(sourceDomain2, CantonTimestamp.Epoch.plusMillis(100L)),
          mediator2,
          LfPartyId.assertFromString("party2"),
        )
        transfer4 <- mkTransferData(
          TransferId(sourceDomain2, CantonTimestamp.Epoch.plusMillis(200L)),
          mediator2,
          LfPartyId.assertFromString("party2"),
        )
        _ <- valueOrFail(store.addTransfer(transfer1))("first add failed")
        _ <- valueOrFail(store.addTransfer(transfer2))("second add failed")
        _ <- valueOrFail(store.addTransfer(transfer3))("third add failed")
        _ <- valueOrFail(store.addTransfer(transfer4))("fourth add failed")
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
            requestAfter =
              Some(transfer2.transferId.transferOutTimestamp -> transfer2.sourceDomain),
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
            .completeTransfer(
              transfer2.transferId,
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

    "add transfer-out/in global offsets" should {

      val transferId = transferData.transferId

      val transferOutOffset = TransferOutGlobalOffset(10L)
      val transferInOffset = TransferInGlobalOffset(15L)

      val transferDataOnlyOut =
        transferData.copy(transferGlobalOffset =
          Some(TransferOutGlobalOffset(transferOutOffset.offset))
        )
      val transferDataTransferComplete = transferData.copy(transferGlobalOffset =
        Some(TransferGlobalOffsets.create(transferOutOffset.offset, transferInOffset.offset).value)
      )

      "allow batch updates" in {
        val store = mk(targetDomain)

        val data = (1L until 13).flatMap { i =>
          val tid = transferId.copy(transferOutTimestamp = CantonTimestamp.ofEpochSecond(i))
          val transferData = transferDataFor(tid, contract)

          val mod = 4

          if (i % mod == 0)
            Seq((TransferOutGlobalOffset(i * 10), transferData))
          else if (i % mod == 1)
            Seq((TransferInGlobalOffset(i * 10), transferData))
          else if (i % mod == 2)
            Seq((TransferGlobalOffsets.create(i * 10, i * 10 + 1).value, transferData))
          else
            Seq(
              (TransferOutGlobalOffset(i * 10), transferData),
              (TransferInGlobalOffset(i * 10 + 1), transferData),
            )

        }

        for {
          _ <- valueOrFail(MonadUtil.sequentialTraverse_(data) { case (_, transferData) =>
            store.addTransfer(transferData)
          })("add transfers")

          offsets = data.map { case (offset, transferData) =>
            transferData.transferId -> offset
          }

          _ <- store.addTransfersOffsets(offsets).valueOrFailShutdown("adding offsets")

          result <- valueOrFail(offsets.toList.parTraverse { case (transferId, _) =>
            store.lookup(transferId)
          })("query transfers")
        } yield {
          result.lengthCompare(offsets) shouldBe 0

          forEvery(result.zip(offsets)) {
            case (retrievedTransferData, (transferId, expectedOffset)) =>
              withClue(s"got unexpected data for transfer $transferId") {
                expectedOffset match {
                  case TransferOutGlobalOffset(out) =>
                    retrievedTransferData.transferOutGlobalOffset shouldBe Some(out)

                  case TransferInGlobalOffset(in) =>
                    retrievedTransferData.transferInGlobalOffset shouldBe Some(in)

                  case TransferGlobalOffsets(out, in) =>
                    retrievedTransferData.transferOutGlobalOffset shouldBe Some(out)
                    retrievedTransferData.transferInGlobalOffset shouldBe Some(in)
                }
              }
          }
        }
      }

      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add")

          _ <- store
            .addTransfersOffsets(Map(transferId -> transferOutOffset))
            .valueOrFailShutdown(
              "add transfer-out offset 1"
            )

          lookupOnlyTransferOut1 <- valueOrFail(store.lookup(transferId))("lookup transfer data")

          _ <- store
            .addTransfersOffsets(Map(transferId -> transferOutOffset))
            .valueOrFailShutdown(
              "add transfer-out offset 2"
            )

          lookupOnlyTransferOut2 <- valueOrFail(store.lookup(transferId))("lookup transfer data")

          _ <- store
            .addTransfersOffsets(Map(transferId -> transferInOffset))
            .valueOrFailShutdown(
              "add transfer-in offset 1"
            )

          lookup1 <- valueOrFail(store.lookup(transferId))("lookup transfer data")

          _ <- store
            .addTransfersOffsets(Map(transferId -> transferInOffset))
            .valueOrFailShutdown(
              "add transfer-in offset 2"
            )

          lookup2 <- valueOrFail(store.lookup(transferId))("lookup transfer data")

        } yield {
          lookupOnlyTransferOut1 shouldBe transferDataOnlyOut
          lookupOnlyTransferOut2 shouldBe transferDataOnlyOut

          lookup1 shouldBe transferDataTransferComplete
          lookup2 shouldBe transferDataTransferComplete
        }
      }

      "return an error if transfer-in offset is the same as the transfer-out" in {
        val store = mk(targetDomain)

        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add")

          _ <- store
            .addTransfersOffsets(Map(transferId -> transferOutOffset))
            .valueOrFailShutdown(
              "add transfer-out offset"
            )

          failedAdd <- store
            .addTransfersOffsets(
              Map(transferId -> TransferInGlobalOffset(transferOutOffset.offset))
            )
            .value
            .failOnShutdown
        } yield failedAdd.left.value shouldBe a[TransferGlobalOffsetsMerge]
      }

      "return an error if transfer-out offset is the same as the transfer-in" in {
        val store = mk(targetDomain)

        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add")

          _ <- store
            .addTransfersOffsets(Map(transferId -> transferInOffset))
            .valueOrFailShutdown(
              "add transfer-in offset"
            )

          failedAdd <- store
            .addTransfersOffsets(
              Map(transferId -> TransferOutGlobalOffset(transferInOffset.offset))
            )
            .value
            .failOnShutdown
        } yield failedAdd.left.value shouldBe a[TransferGlobalOffsetsMerge]
      }

      "return an error if the new value differs from the old one" in {
        val store = mk(targetDomain)

        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add")

          _ <- store
            .addTransfersOffsets(Map(transferId -> transferOutOffset))
            .valueOrFailShutdown(
              "add transfer-out offset 1"
            )

          _ <- store
            .addTransfersOffsets(Map(transferId -> transferInOffset))
            .valueOrFailShutdown(
              "add transfer-out offset 2"
            )

          lookup1 <- valueOrFail(store.lookup(transferId))("lookup transfer data")

          successfulAddOutOffset <- store
            .addTransfersOffsets(Map(transferId -> transferOutOffset))
            .value
            .failOnShutdown
          failedAddOutOffset <- store
            .addTransfersOffsets(
              Map(
                transferId -> TransferOutGlobalOffset(
                  GlobalOffset.tryFromLong(transferOutOffset.offset.toLong - 1)
                )
              )
            )
            .value
            .failOnShutdown

          successfulAddInOffset <- store
            .addTransfersOffsets(Map(transferId -> transferInOffset))
            .value
            .failOnShutdown
          failedAddInOffset <- store
            .addTransfersOffsets(
              Map(
                transferId -> TransferInGlobalOffset(
                  GlobalOffset.tryFromLong(transferInOffset.offset.toLong - 1)
                )
              )
            )
            .value
            .failOnShutdown

          lookup2 <- valueOrFail(store.lookup(transferId))("lookup transfer data")

        } yield {
          successfulAddOutOffset.value shouldBe ()
          failedAddOutOffset.left.value shouldBe a[TransferGlobalOffsetsMerge]

          successfulAddInOffset.value shouldBe ()
          failedAddInOffset.left.value shouldBe a[TransferGlobalOffsetsMerge]

          lookup1 shouldBe transferDataTransferComplete
          lookup2 shouldBe transferDataTransferComplete
        }
      }
    }

    "incomplete" should {
      val limit = NonNegativeInt.tryCreate(10)

      def assertIsIncomplete(
          incompletes: Seq[IncompleteTransferData],
          expectedTransferData: TransferData,
      ): Assertion =
        incompletes.map(_.toTransferData) shouldBe Seq(expectedTransferData)

      "list incomplete transfers (transfer-out done)" in {
        val store = mk(targetDomain)
        val transferId = transferData.transferId

        val transferOutOffset = 10L
        val transferInOffset = 20L

        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          lookupNoOffset <- store.findIncomplete(None, Long.MaxValue, None, limit)

          _ <- store
            .addTransfersOffsets(Map(transferId -> TransferOutGlobalOffset(transferOutOffset)))
            .valueOrFailShutdown(
              "add transfer-out offset failed"
            )
          lookupBeforeTransferOut <- store.findIncomplete(
            None,
            transferOutOffset - 1,
            None,
            limit,
          )
          lookupAtTransferOut <- store.findIncomplete(None, transferOutOffset, None, limit)

          _ <- store
            .addTransfersOffsets(Map(transferId -> TransferInGlobalOffset(transferInOffset)))
            .valueOrFailShutdown(
              "add transfer-in offset failed"
            )

          lookupBeforeTransferIn <- store.findIncomplete(
            None,
            transferInOffset - 1,
            None,
            limit,
          )
          lookupAtTransferIn <- store.findIncomplete(None, transferInOffset, None, limit)
          lookupAfterTransferIn <- store.findIncomplete(
            None,
            transferInOffset,
            None,
            limit,
          )
        } yield {
          lookupNoOffset shouldBe empty

          lookupBeforeTransferOut shouldBe empty

          assertIsIncomplete(
            lookupAtTransferOut,
            transferData.copy(transferGlobalOffset =
              Some(TransferOutGlobalOffset(transferOutOffset))
            ),
          )

          assertIsIncomplete(
            lookupBeforeTransferIn,
            transferData.copy(transferGlobalOffset =
              Some(TransferOutGlobalOffset(transferOutOffset))
            ),
          )

          lookupAtTransferIn shouldBe empty
          lookupAfterTransferIn shouldBe empty
        }
      }

      "list incomplete transfers (transfer-in done)" in {
        val store = mk(targetDomain)
        val transferId = transferData.transferId

        val transferInOffset = 10L
        val transferOutOffset = 20L

        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          lookupNoOffset <- store.findIncomplete(None, Long.MaxValue, None, limit)

          _ <-
            store
              .addTransfersOffsets(Map(transferId -> TransferInGlobalOffset(transferInOffset)))
              .valueOrFailShutdown(
                "add transfer-in offset failed"
              )
          lookupBeforeTransferIn <- store.findIncomplete(
            None,
            transferInOffset - 1,
            None,
            limit,
          )
          lookupAtTransferIn <- store.findIncomplete(None, transferInOffset, None, limit)

          _ <-
            store
              .addTransfersOffsets(Map(transferId -> TransferOutGlobalOffset(transferOutOffset)))
              .valueOrFailShutdown(
                "add transfer-out offset failed"
              )

          lookupBeforeTransferOut <- store.findIncomplete(
            None,
            transferOutOffset - 1,
            None,
            limit,
          )
          lookupAtTransferOut <- store.findIncomplete(None, transferOutOffset, None, limit)
          lookupAfterTransferOut <- store.findIncomplete(
            None,
            transferOutOffset,
            None,
            limit,
          )
        } yield {
          lookupNoOffset shouldBe empty

          lookupBeforeTransferIn shouldBe empty

          assertIsIncomplete(
            lookupAtTransferIn,
            transferData.copy(transferGlobalOffset = Some(TransferInGlobalOffset(transferInOffset))),
          )

          assertIsIncomplete(
            lookupBeforeTransferOut,
            transferData.copy(transferGlobalOffset = Some(TransferInGlobalOffset(transferInOffset))),
          )

          lookupAtTransferOut shouldBe empty
          lookupAfterTransferOut shouldBe empty
        }
      }

      "take stakeholders filter into account" in {
        val store = mk(targetDomain)

        val alice = TransferStoreTest.alice
        val bob = TransferStoreTest.bob

        val aliceContract = TransferStoreTest.contract(TransferStoreTest.coidAbs1, alice)
        val bobContract = TransferStoreTest.contract(TransferStoreTest.coidAbs2, bob)

        val transferOutOffset = 42L

        val contracts = Seq(aliceContract, bobContract, aliceContract, bobContract)
        val transfersData = contracts.zipWithIndex.map { case (contract, idx) =>
          val transferId =
            TransferId(sourceDomain1, CantonTimestamp.Epoch.plusSeconds(idx.toLong))

          transferDataFor(
            transferId,
            contract,
            transferOutGlobalOffset = Some(transferOutOffset),
          )
        }
        val stakeholders = contracts.map(_.metadata.stakeholders)

        val addTransfersET = transfersData.parTraverse(store.addTransfer)

        def lift(stakeholder: LfPartyId, others: LfPartyId*): Option[NonEmpty[Set[LfPartyId]]] =
          Option(NonEmpty(Set, stakeholder, others: _*))

        def stakeholdersOf(incompleteTransfers: Seq[IncompleteTransferData]): Seq[Set[LfPartyId]] =
          incompleteTransfers.map(_.contract.metadata.stakeholders)

        for {
          _ <- valueOrFail(addTransfersET)("add failed")

          lookupNone <- store.findIncomplete(None, transferOutOffset, None, limit)
          lookupAll <- store.findIncomplete(
            None,
            transferOutOffset,
            lift(alice, bob),
            limit,
          )

          lookupAlice <- store.findIncomplete(None, transferOutOffset, lift(alice), limit)
          lookupBob <- store.findIncomplete(None, transferOutOffset, lift(bob), limit)
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
          transferData.copy(transferGlobalOffset = Some(TransferInGlobalOffset(offset)))

        for {
          _ <- valueOrFail(store.addTransfer(transfer))("add")

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
          _ <- valueOrFail(store.addTransfer(transferData))("add")
          _ <- store
            .addTransfersOffsets(
              Map(transferData.transferId -> TransferOutGlobalOffset(offset))
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

    "addTransfer" should {
      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("first add failed")
          _ <- valueOrFail(store.addTransfer(transferData))("second add failed")
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
          _ <- valueOrFail(store.addTransfer(transferData))("first add failed")
          add2 <- store.addTransfer(transferDataModified).value
        } yield assert(
          add2 == Left(TransferDataAlreadyExists(transferData, transferDataModified)),
          "second add failed",
        )
      }

      "handle transfer-out results" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(withTransferOutResult))("first add failed")
          _ <- valueOrFail(store.addTransfer(transferData))("second add failed")
          lookup2 <- valueOrFail(store.lookup(transfer10))("TransferOutResult missing")
          _ <- valueOrFail(store.addTransfer(withTransferOutResult))("third add failed")
        } yield assert(lookup2 == withTransferOutResult, "TransferOutResult remains")
      }

      "add several transfers" in {
        val store = mk(targetDomain)
        for {
          transferData10 <- mkTransferData(transfer10, mediator1)
          transferData11 <- mkTransferData(transfer11, mediator1)
          transferData20 <- mkTransferData(transfer20, mediator2)
          _ <- valueOrFail(store.addTransfer(transferData10))("first add failed")
          _ <- valueOrFail(store.addTransfer(transferData11))("second add failed")
          _ <- valueOrFail(store.addTransfer(transferData20))("third add failed")
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
          store.addTransfer(transferData),
          _.getMessage shouldBe "Domain domain1::DOMAIN1: Transfer store cannot store transfer for domain target::DOMAIN",
        )
      }
    }

    "addTransferOutResult" should {

      "report missing transfers" in {
        val store = mk(targetDomain)
        for {
          missing <- store.addTransferOutResult(transferOutResult).value
        } yield missing shouldBe Left(UnknownTransferId(transfer10))
      }

      "add the result" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          lookup <- valueOrFail(store.lookup(transfer10))("transfer not found")
        } yield assert(
          lookup == transferData.copy(transferOutResult = Some(transferOutResult)),
          "result is stored",
        )
      }

      "report mismatching results" in {
        val store = mk(targetDomain)
        val modifiedTransferOutResult = transferOutResult.copy(
          result = transferOutResult.result.copy(
            content =
              transferOutResult.result.content.copy(timestamp = CantonTimestamp.ofEpochSecond(2))
          )
        )
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          modified <- store.addTransferOutResult(modifiedTransferOutResult).value
          lookup <- valueOrFail(store.lookup(transfer10))("transfer not found")
        } yield {
          assert(
            modified == Left(
              TransferOutResultAlreadyExists(
                transfer10,
                transferOutResult,
                modifiedTransferOutResult,
              )
            ),
            "modified result is flagged",
          )
          assert(
            lookup == transferData.copy(transferOutResult = Some(transferOutResult)),
            "result is not overwritten stored",
          )
        }
      }
    }

    "completeTransfer" should {
      "mark the transfer as completed" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("completion failed")
          lookup <- store.lookup(transfer10).value
        } yield lookup shouldBe Left(TransferCompleted(transfer10, toc))
      }

      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("first completion failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("second completion failed")
        } yield succeed
      }

      "be allowed before the result" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("first completion failed")
          lookup1 <- store.lookup(transfer10).value
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          lookup2 <- store.lookup(transfer10).value
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("second completion failed")
        } yield {
          lookup1 shouldBe Left(TransferCompleted(transfer10, toc))
          lookup2 shouldBe Left(TransferCompleted(transfer10, toc))
        }
      }

      "detect mismatches" in {
        val store = mk(targetDomain)
        val toc2 = TimeOfChange(RequestCounter(0), CantonTimestamp.ofEpochSecond(4))
        val modifiedTransferData =
          transferData.copy(transferOutRequestCounter = RequestCounter(100))
        val modifiedTransferOutResult = transferOutResult.copy(
          result = transferOutResult.result.copy(content =
            transferOutResult.result.content.copy(counter = SequencerCounter(120))
          )
        )

        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("first completion failed")
          complete2 <- store.completeTransfer(transfer10, toc2).value
          add2 <- store.addTransfer(modifiedTransferData).value
          addResult2 <- store.addTransferOutResult(modifiedTransferOutResult).value
        } yield {
          complete2 shouldBe Checked.continue(TransferAlreadyCompleted(transfer10, toc2))
          add2 shouldBe Left(TransferDataAlreadyExists(withTransferOutResult, modifiedTransferData))
          addResult2 shouldBe Left(
            TransferOutResultAlreadyExists(transfer10, transferOutResult, modifiedTransferOutResult)
          )
        }
      }

      "store the first completion" in {
        val store = mk(targetDomain)
        val toc2 = TimeOfChange(RequestCounter(1), CantonTimestamp.ofEpochSecond(4))
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc2))("later completion failed")
          complete2 <- store.completeTransfer(transfer10, toc).value
          lookup <- store.lookup(transfer10).value
        } yield {
          complete2 shouldBe Checked.continue(TransferAlreadyCompleted(transfer10, toc))
          lookup shouldBe Left(TransferCompleted(transfer10, toc2))
        }
      }

    }

    "delete" should {
      "remove the transfer" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- store.deleteTransfer(transfer10)
          lookup <- store.lookup(transfer10).value
        } yield lookup shouldBe Left(UnknownTransferId(transfer10))
      }

      "purge completed transfers" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          _ <- valueOrFail(store.addTransferOutResult(transferOutResult))("addResult failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("completion failed")
          _ <- store.deleteTransfer(transfer10)
        } yield succeed
      }

      "ignore unknown transfer IDs" in {
        val store = mk(targetDomain)
        for {
          () <- store.deleteTransfer(transfer10)
        } yield succeed
      }

      "be idempotent" in {
        val store = mk(targetDomain)
        for {
          _ <- valueOrFail(store.addTransfer(transferData))("add failed")
          () <- store.deleteTransfer(transfer10)
          () <- store.deleteTransfer(transfer10)
        } yield succeed
      }
    }

    "transfer stores should be isolated" in {
      val storeTarget = mk(targetDomain)
      val store1 = mk(TargetDomainId(sourceDomain1.unwrap))
      for {
        _ <- valueOrFail(storeTarget.addTransfer(transferData))("add failed")
        found <- store1.lookup(transferData.transferId).value
      } yield found shouldBe Left(UnknownTransferId(transferData.transferId))
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
          _ <- valueOrFail(store.addTransfer(aliceTransfer))("add alice failed")
          _ <- valueOrFail(store.addTransfer(bobTransfer))("add bob failed")
          _ <- valueOrFail(store.addTransfer(eveTransfer))("add eve failed")
          _ <- valueOrFail(store.completeTransfer(transfer10, toc))("completion alice failed")
          _ <- valueOrFail(store.completeTransfer(transfer11, toc1))("completion bob failed")
          _ <- valueOrFail(store.completeTransfer(transfer20, toc2))("completion eve failed")
          _ <- store.deleteCompletionsSince(RequestCounter(1))
          alice <- leftOrFail(store.lookup(transfer10))("alice must still be completed")
          bob <- valueOrFail(store.lookup(transfer11))("bob must not be completed")
          eve <- valueOrFail(store.lookup(transfer20))("eve must not be completed")
          _ <- valueOrFail(store.completeTransfer(transfer11, toc2))("second completion bob failed")
          _ <- valueOrFail(store.completeTransfer(transfer20, toc1))("second completion eve failed")
        } yield {
          alice shouldBe TransferCompleted(transfer10, toc)
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
  val mediator1 = MediatorId(UniqueIdentifier.tryCreate("mediator1", "DOMAIN1"))

  val domain2 = DomainId(UniqueIdentifier.tryCreate("domain2", "DOMAIN2"))
  val sourceDomain2 = SourceDomainId(DomainId(UniqueIdentifier.tryCreate("domain2", "DOMAIN2")))
  val targetDomain2 = TargetDomainId(DomainId(UniqueIdentifier.tryCreate("domain2", "DOMAIN2")))
  val mediator2 = MediatorId(UniqueIdentifier.tryCreate("mediator2", "DOMAIN2"))

  val targetDomain = TargetDomainId(DomainId(UniqueIdentifier.tryCreate("target", "DOMAIN")))

  val transfer10 = TransferId(sourceDomain1, CantonTimestamp.Epoch)
  val transfer11 = TransferId(sourceDomain1, CantonTimestamp.ofEpochMilli(1))
  val transfer20 = TransferId(sourceDomain2, CantonTimestamp.Epoch)

  val loggerFactoryNotUsed = NamedLoggerFactory.unnamedKey("test", "NotUsed-TransferStoreTest")
  val ec: ExecutionContext = DirectExecutionContext(
    loggerFactoryNotUsed.getLogger(TransferStoreTest.getClass)
  )
  private implicit val _ec: ExecutionContext = ec
  val cryptoFactory =
    TestingIdentityFactory(loggerFactoryNotUsed).forOwnerAndDomain(
      DefaultTestIdentities.sequencerId
    )
  val sequencerKey =
    TestingIdentityFactory(loggerFactoryNotUsed)
      .newSigningPublicKey(DefaultTestIdentities.sequencerId)
      .fingerprint
  val privateCrypto = cryptoFactory.crypto.privateCrypto
  val pureCryptoApi: CryptoPureApi = cryptoFactory.pureCrypto

  def sign(str: String): Signature = {
    val hash =
      pureCryptoApi.build(HashPurpose.TransferResultSignature).addWithoutLengthPrefix(str).finish()
    Await.result(
      privateCrypto
        .sign(hash, sequencerKey)
        .valueOr(err => throw new RuntimeException(err.toString)),
      10.seconds,
    )
  }

  val seedGenerator = new SeedGenerator(pureCryptoApi)

  private def submitterMetadata(submitter: LfPartyId): TransferSubmitterMetadata = {

    val submittingParticipant: LedgerParticipantId =
      TransferInView.submittingParticipantDefaultValue

    val applicationId: LedgerApplicationId = TransferInView.applicationIdDefaultValue

    val commandId: LedgerCommandId = TransferInView.commandIdDefaultValue

    TransferSubmitterMetadata(
      submitter,
      applicationId,
      submittingParticipant,
      commandId,
      submissionId = None,
      workflowId = None,
    )
  }

  private[participant] val templateId: LfTemplateId = TransferOutView.templateIdDefaultValue

  def mkTransferDataForDomain(
      transferId: TransferId,
      sourceMediator: MediatorRef,
      submittingParty: LfPartyId = LfPartyId.assertFromString("submitter"),
      targetDomainId: TargetDomainId,
      creatingTransactionId: TransactionId = ExampleTransactionFactory.transactionId(0),
      contract: SerializableContract = contract,
      transferOutGlobalOffset: Option[GlobalOffset] = None,
  ): Future[TransferData] = {

    /*
      Method TransferOutView.fromProtoV0 set protocol version to v3 (not present in Protobuf v0).
     */
    val targetProtocolVersion =
      if (BaseTest.testedProtocolVersion <= ProtocolVersion.v3)
        TargetProtocolVersion(ProtocolVersion.v3)
      else
        TargetProtocolVersion(BaseTest.testedProtocolVersion)

    val transferOutRequest = TransferOutRequest(
      submitterMetadata(submittingParty),
      Set(submittingParty),
      Set.empty,
      creatingTransactionId,
      contract,
      transferId.sourceDomain,
      SourceProtocolVersion(BaseTest.testedProtocolVersion),
      sourceMediator,
      targetDomainId,
      targetProtocolVersion,
      TimeProofTestUtil.mkTimeProof(
        timestamp = CantonTimestamp.Epoch,
        targetDomain = targetDomainId,
      ),
    )
    val uuid = new UUID(10L, 0L)
    val seed = seedGenerator.generateSaltSeed()
    val fullTransferOutViewTree = transferOutRequest
      .toFullTransferOutTree(
        pureCryptoApi,
        pureCryptoApi,
        seed,
        uuid,
      )
    Future.successful(
      TransferData(
        sourceProtocolVersion = SourceProtocolVersion(BaseTest.testedProtocolVersion),
        transferOutTimestamp = transferId.transferOutTimestamp,
        transferOutRequestCounter = RequestCounter(0),
        transferOutRequest = fullTransferOutViewTree,
        transferOutDecisionTime = CantonTimestamp.ofEpochSecond(10),
        contract = contract,
        creatingTransactionId = transactionId1,
        transferOutResult = None,
        transferGlobalOffset = transferOutGlobalOffset.map(TransferOutGlobalOffset),
      )
    )
  }

  private def mkTransferData(
      transferId: TransferId,
      sourceMediator: MediatorId,
      submitter: LfPartyId = LfPartyId.assertFromString("submitter"),
      creatingTransactionId: TransactionId = transactionId1,
      contract: SerializableContract = contract,
      transferOutGlobalOffset: Option[GlobalOffset] = None,
  ) =
    mkTransferDataForDomain(
      transferId,
      MediatorRef(sourceMediator),
      submitter,
      targetDomain,
      creatingTransactionId,
      contract,
      transferOutGlobalOffset,
    )

  def mkTransferOutResult(transferData: TransferData): DeliveredTransferOutResult =
    DeliveredTransferOutResult {
      val requestId = RequestId(transferData.transferOutTimestamp)

      val mediatorMessage = transferData.transferOutRequest.tree.mediatorMessage
      val result = mediatorMessage.createMediatorResult(
        requestId,
        Verdict.Approve(BaseTest.testedProtocolVersion),
        mediatorMessage.allInformees,
      )
      val signedResult =
        SignedProtocolMessage.from(
          result,
          BaseTest.testedProtocolVersion,
          sign("TransferOutResult-mediator"),
        )
      val batch =
        Batch.of(BaseTest.testedProtocolVersion, signedResult -> RecipientsTest.testInstance)
      val deliver =
        Deliver.create(
          SequencerCounter(1),
          CantonTimestamp.ofEpochMilli(10),
          transferData.sourceDomain.unwrap,
          Some(MessageId.tryCreate("1")),
          batch,
          BaseTest.testedProtocolVersion,
        )
      SignedContent(
        deliver,
        sign("TransferOutResult-sequencer"),
        Some(transferData.transferOutTimestamp),
        BaseTest.testedProtocolVersion,
      )
    }
}
