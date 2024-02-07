// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.Chain
import cats.syntax.parallel.*
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.QualifiedName
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.*
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  transactionId,
}
import com.digitalasset.canton.protocol.{
  ExampleTransactionFactory,
  LfContractId,
  SourceDomainId,
  TargetDomainId,
}
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.store.PrunableByTimeTest
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.digitalasset.canton.{BaseTest, LfPackageId, RequestCounter}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpecLike

import java.time.Instant
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

@nowarn("msg=match may not be exhaustive")
trait ActiveContractStoreTest extends PrunableByTimeTest {
  this: AsyncWordSpecLike & BaseTest =>

  protected implicit def closeContext: CloseContext

  lazy val acsDomainStr: String300 = String300.tryCreate("active-contract-store::default")
  lazy val acsDomainId: DomainId = DomainId.tryFromString(acsDomainStr.unwrap)

  lazy val active = Active

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  protected def activeContractStore(
      mkAcs: ExecutionContext => ActiveContractStore,
      mkContractStore: ExecutionContext => ContractStore,
  ): Unit = {
    def mk(): ActiveContractStore = mkAcs(executionContext)

    def mkCS(): ContractStore = mkContractStore(executionContext)

    val coid00 = ExampleTransactionFactory.suffixedId(0, 0)
    val coid01 = ExampleTransactionFactory.suffixedId(0, 1)
    val coid10 = ExampleTransactionFactory.suffixedId(1, 0)
    val coid11 = ExampleTransactionFactory.suffixedId(1, 1)

    val thousandOneContracts = (1 to 1001).map(ExampleTransactionFactory.suffixedId(0, _)).toSeq

    val rc = RequestCounter(0)
    val ts = CantonTimestamp.assertFromInstant(Instant.parse("2019-04-04T10:00:00.00Z"))

    behave like prunableByTime(mkAcs)

    /*
      Query the ACS for a snapshot at `ts` and `rc` and assert that the snapshot
      contains exactly `expectedContract`
     */
    def assertSnapshots(acs: ActiveContractStore, ts: CantonTimestamp, rc: RequestCounter)(
        expectedContract: Option[LfContractId]
    ): Future[Assertion] =
      for {
        snapshotTs <- acs.snapshot(ts)
        snapshotRc <- acs.snapshot(rc)
      } yield {
        val expectedSnapshotTs = expectedContract.toList.map { cid =>
          cid -> ts
        }.toMap
        val expectedSnapshotRc = expectedContract.toList.map { cid =>
          cid -> rc
        }.toMap

        snapshotTs shouldBe expectedSnapshotTs
        snapshotRc shouldBe expectedSnapshotRc
      }

    "yielding an empty snapshot from an empty ACS" in {
      val acs = mk()

      assertSnapshots(acs, ts, rc)(None)
    }

    "not finding any contract in an empty ACS" in {
      val acs = mk()
      for {
        result <- acs.fetchState(coid00)
      } yield assert(result.isEmpty)
    }

    "querying for a large number of contracts should not error" in {
      val acs = mk()
      for {
        fetch <- acs.fetchStates(thousandOneContracts)
      } yield assert(fetch.isEmpty)
    }

    "creating a contract in an empty ACS" in {
      val acs = mk()

      for {
        created <- acs
          .markContractActive(coid00, TimeOfChange(rc, ts))
          .value
        fetch <- acs.fetchStates(Seq(coid00, coid01))

        // At creation, snapshot should contain exactly the contract
        assertion <- assertSnapshots(acs, ts, rc)(Some(coid00))

        // Before creation, snapshot should be empty
        assertion2 <- assertSnapshots(acs, ts.addMicros(-1), rc - 1)(None)
      } yield {
        created shouldBe Symbol("successful")
        fetch shouldBe Map(coid00 -> ContractState(active, rc, ts))

        assertion shouldBe succeed
        assertion2 shouldBe succeed
      }
    }

    val rc2 = RequestCounter(1)
    val ts2 = ts.addMicros(1)

    "creating and archiving a contract" in {
      val acs = mk()

      for {
        created <- acs
          .markContractActive(coid00, TimeOfChange(rc, ts))
          .value
        archived <- acs
          .archiveContract(coid00, TimeOfChange(rc2, ts2))
          .value

        snapshotTs1 <- acs.snapshot(ts2.addMicros(-1))
        snapshotTs2 <- acs.snapshot(ts2)

        snapshotRc1 <- acs.snapshot(rc2 - 1)
        snapshotRc2 <- acs.snapshot(rc2)

        fetch <- acs.fetchState(coid00)
      } yield {
        assert(created == Checked.unit && archived == Checked.unit, "succeed")

        assert(
          snapshotTs1 == Map(coid00 -> ts),
          "include it in intermediate snapshot",
        )
        assert(
          snapshotRc1 == Map(coid00 -> rc),
          "include it in intermediate snapshot",
        )

        assert(snapshotTs2 == Map.empty, "omit it in snapshots after archival")
        assert(snapshotRc2 == Map.empty, "omit it in snapshots after archival")

        assert(
          fetch.contains(ContractState(Archived, rc2, ts2)),
          "mark it as archived",
        )
      }
    }

    "creating and archiving can have the same timestamp" in {
      val acs = mk()

      for {
        created <- acs
          .markContractActive(coid00, TimeOfChange(rc, ts))
          .value
        archived <- acs.archiveContract(coid00, TimeOfChange(rc2, ts)).value
        fetch <- acs.fetchState(coid00)
        snapshot1 <- acs.snapshot(ts.addMicros(-1))
        snapshot2 <- acs.snapshot(ts)
      } yield {
        assert(created.successful && archived.successful, "succeed")
        assert(fetch.contains(ContractState(Archived, rc2, ts)))
        assert(snapshot1 == Map.empty)
        assert(snapshot2 == Map.empty)
      }
    }

    "creating and archiving can have the same request counter" in {
      val acs = mk()

      for {
        created <- acs
          .markContractActive(coid00, TimeOfChange(rc, ts))
          .value
        archived <- acs.archiveContract(coid00, TimeOfChange(rc, ts2)).value
        fetch <- acs.fetchState(coid00)
        snapshot1 <- acs.snapshot(rc - 1)
        snapshot2 <- acs.snapshot(rc)
      } yield {
        assert(created.successful && archived.successful, "succeed")
        assert(fetch.contains(ContractState(Archived, rc, ts2)))
        assert(snapshot1 == Map.empty)
        assert(snapshot2 == Map.empty)
      }
    }

    "inserting is idempotent" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      for {
        created1 <- acs.markContractActive(coid00, toc).value
        created2 <- acs.markContractActive(coid00, toc).value
        fetch <- acs.fetchState(coid00)
      } yield {
        created1 shouldBe Symbol("successful")

        created2 shouldBe Symbol("successful")

        assert(fetch.contains(ContractState(active, rc, ts)))
      }
    }

    "inserting is idempotent even if the contract is archived in between" must {
      "with a later TimeOfChange" in {
        val acs = mk()
        val toc = TimeOfChange(rc, ts)
        for {
          created1 <- acs.markContractActive(coid00, toc).value
          archived <- acs
            .archiveContract(coid00, TimeOfChange(rc2, ts2))
            .value
          created2 <- acs.markContractActive(coid00, toc).value
          fetch <- acs.fetchState(coid00)
          snapshot <- acs.snapshot(ts2)
        } yield {
          created1 shouldBe Symbol("successful")
          archived shouldBe Symbol("successful")
          created2 shouldBe Symbol("successful")

          assert(fetch.contains(ContractState(Archived, rc2, ts2)))
          snapshot shouldBe Map.empty
        }
      }

      "with the same TimeOfChange" in {
        val acs = mk()
        val toc = TimeOfChange(rc, ts)
        for {
          created1 <- acs.markContractActive(coid00, toc).value
          archived <- acs.archiveContract(coid00, toc).value
          created2 <- acs.markContractActive(coid00, toc).value
          fetch <- acs.fetchState(coid00)
          snapshot <- acs.snapshot(ts2.addMicros(-1))
        } yield {
          created1 shouldBe Symbol("successful")
          archived shouldBe Symbol("successful")
          created2 shouldBe Symbol("successful")

          assert(fetch.contains(ContractState(Archived, toc)))
          snapshot shouldBe Map.empty
        }
      }
    }

    "archival must not be timestamped before creation" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc, ts2)
      for {
        created <- acs.markContractActive(coid00, toc2).value
        archived <- acs.archiveContract(coid00, toc).value
        fetch <- acs.fetchState(coid00)
        snapshot <- acs.snapshot(ts2)
      } yield {
        assert(created.successful, "creation succeeds")
        assert(
          archived.isResult && archived.nonaborts.toList.toSet == Set(
            ChangeBeforeCreation(coid00, toc2, toc),
            ChangeAfterArchival(coid00, toc, toc2),
          ),
          "archival fails",
        )
        assert(fetch.contains(ContractState(active, rc, ts2)), "contract remains active")
        assert(
          snapshot == Map(coid00 -> ts2),
          "contract remains in snapshot",
        )
      }
    }

    "archival may be signalled before creation" in {
      val acs = mk()
      for {
        archived <- acs
          .archiveContract(coid00, TimeOfChange(rc2, ts2))
          .value
        fetch1 <- acs.fetchState(coid00)
        created <- acs
          .markContractActive(coid00, TimeOfChange(rc, ts))
          .value
        fetch2 <- acs.fetchState(coid00)
        snapshot1 <- acs.snapshot(ts2.addMicros(-1))
        snapshot2 <- acs.snapshot(ts2)
      } yield {
        assert(archived.successful && created.successful, "succeed")
        assert(
          fetch1.contains(ContractState(Archived, rc2, ts2)),
          "mark it as Archived even if it was never created",
        )
        assert(
          fetch2.contains(ContractState(Archived, rc2, ts2)),
          "mark it as Archived even if the creation was signalled later",
        )
        assert(
          snapshot1 == Map(coid00 -> ts),
          "include it in the snapshot before the archival",
        )
        assert(snapshot2 == Map.empty, "omit it from the snapshot after the archival")
      }
    }

    "archival is idempotent" in {
      val acs = mk()
      val toc2 = TimeOfChange(rc2, ts2)
      for {
        created <- acs
          .markContractActive(coid00, TimeOfChange(rc, ts))
          .value
        archived1 <- acs.archiveContract(coid00, toc2).value
        archived2 <- acs.archiveContract(coid00, toc2).value
        fetch <- acs.fetchState(coid00)
        snapshotBeforeArchival <- acs.snapshot(ts2.addMicros(-1))
      } yield {
        created shouldBe Symbol("successful")
        archived1 shouldBe Symbol("successful")
        archived2 shouldBe Symbol("successful")

        assert(
          fetch.contains(ContractState(Archived, rc2, ts2)),
          "mark it as Archived",
        )
        snapshotBeforeArchival shouldBe Map(coid00 -> ts)
      }
    }

    "earlier archival wins in irregularity reporting" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc2, ts2)
      val toc3 = TimeOfChange(rc2 + 1, ts2.plusMillis(1))
      for {
        created <- acs
          .markContractActive(coid00, TimeOfChange(rc, ts))
          .value
        archived1 <- acs.archiveContract(coid00, toc2).value
        archived2 <- acs.archiveContract(coid00, toc).value
        archived3 <- acs.archiveContract(coid00, toc3).value
        fetch <- acs.fetchState(coid00)
      } yield {
        assert(created.successful && archived1.successful, "succeed")
        assert(
          archived2.isResult && archived2.nonaborts == Chain(
            DoubleContractArchival(coid00, toc2, toc)
          ),
          "second archival reports error",
        )
        assert(
          archived3.isResult && archived3.nonaborts == Chain(
            DoubleContractArchival(coid00, toc, toc3)
          ),
          "third archival reports error with updated timestamp",
        )
        assert(
          fetch.contains(ContractState(Archived, toc3.rc, toc3.timestamp)),
          "third archival is the latest",
        )
      }
    }

    val rc3 = RequestCounter(2L)
    val ts3 = ts2.plusMillis(1)

    "several contracts can be inserted" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      val toc3 = TimeOfChange(rc2, ts3)
      val toc2 = TimeOfChange(rc2, ts2)
      for {
        created2 <- acs.markContractActive(coid01, toc3).value
        created1 <- acs.markContractActive(coid00, toc).value
        created3 <- acs.markContractActive(coid10, toc2).value
        archived3 <- acs.archiveContract(coid10, toc3).value
        fetch <- acs.fetchStates(Seq(coid00, coid01, coid10))
        snapshot1 <- acs.snapshot(ts)
        snapshot2 <- acs.snapshot(ts2)
        snapshot3 <- acs.snapshot(ts3)
      } yield {
        created1 shouldBe Symbol("successful")
        created2 shouldBe Symbol("successful")
        created3 shouldBe Symbol("successful")
        archived3 shouldBe Symbol("successful")
        fetch shouldBe Map(
          coid00 -> ContractState(active, toc),
          coid01 -> ContractState(active, toc3),
          coid10 -> ContractState(Archived, toc3),
        )
        snapshot1 shouldBe Map(coid00 -> ts)
        snapshot2 shouldBe Map(
          coid00 -> ts,
          coid10 -> ts2,
        )
        snapshot3 shouldBe Map(
          coid00 -> ts,
          coid01 -> ts3,
        )
      }
    }

    "double creation fails if the timestamp or request counter differs" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      val tocTs = TimeOfChange(rc, ts.plusMillis(1))
      val tocRc = TimeOfChange(rc + 1, ts)
      for {
        created1 <- acs.markContractActive(coid00, toc).value
        created2 <- acs.markContractActive(coid00, tocTs).value
        fetch2 <- acs.fetchState(coid00)
        snapshot <- acs.snapshot(ts.plusMillis(2))
        created3 <- acs.markContractActive(coid00, tocRc).value
        fetch3 <- acs.fetchState(coid00)
      } yield {
        assert(created1.successful, "succeed")
        assert(
          created2 == Checked.continue(DoubleContractCreation(coid00, toc, tocTs)),
          "fail if timestamp differs",
        )

        withClue("fail if request counter differs") {
          created3 shouldBe Symbol("result")
          created3.nonaborts.toList.toSet shouldBe Set(DoubleContractCreation(coid00, tocTs, tocRc))
        }

        assert(
          fetch2.contains(ContractState(active, tocTs.rc, tocTs.timestamp)),
          "fetch tracks latest create",
        )
        assert(
          fetch3.contains(ContractState(active, tocTs.rc, tocTs.timestamp)),
          "fetch tracks latest create",
        )
        assert(
          snapshot == Map(coid00 -> tocTs.timestamp),
          "snapshot contains the latest create",
        )
      }
    }

    "double archival fails if the timestamp or request counters differs" in {
      val acs = mk()
      val toc = TimeOfChange(rc2, ts3)
      val tocTs = TimeOfChange(rc2, ts3.addMicros(-2))
      val tocRc = TimeOfChange(rc2 + 1, ts3)

      for {
        archived1 <- acs.archiveContract(coid00, toc).value
        archived2 <- acs.archiveContract(coid00, tocTs).value
        archived3 <- acs.archiveContract(coid00, tocRc).value
        created <- acs
          .markContractActive(coid00, TimeOfChange(rc, ts))
          .value
        fetch <- acs.fetchState(coid00)
        snapshot1 <- acs.snapshot(ts3.addMicros(-2))
        snapshot2 <- acs.snapshot(ts3.addMicros(-3))
      } yield {
        assert(archived1.successful && created.successful, "succeed")
        assert(
          archived2.isResult && archived2.nonaborts == Chain(
            DoubleContractArchival(coid00, toc, tocTs)
          ),
          "fail if timestamp differs",
        )
        assert(
          archived3.isResult && archived3.nonaborts.toList.toSet == Set(
            DoubleContractArchival(coid00, tocTs, tocRc)
          ),
          "fail if request counter differs",
        )
        assert(
          fetch.contains(
            ContractState(Archived, tocRc.rc, tocRc.timestamp)
          ),
          "timestamp and request counter are as expected",
        )
        assert(
          snapshot1 == Map.empty,
          "snapshot after updated archival does not contain the contract",
        )
        assert(
          snapshot2 == Map(coid00 -> ts),
          "snapshot before archival contains the contract",
        )
      }
    }

    "bulk creates create all contracts" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      for {
        created <- acs
          .markContractsActive(
            Seq(
              coid00,
              coid01,
              coid10,
            ),
            toc,
          )
          .value
        snapshot <- acs.snapshot(ts)
        fetch <- acs.fetchStates(Seq(coid00, coid01, coid10))
      } yield {
        created shouldBe Symbol("successful")
        snapshot shouldBe Map(
          coid00 -> ts,
          coid01 -> ts,
          coid10 -> ts,
        )
        fetch shouldBe Map(
          coid00 -> ContractState(active, toc),
          coid01 -> ContractState(active, toc),
          coid10 -> ContractState(active, toc),
        )
      }
    }

    "bulk create with empty set" in {
      val acs = mk()
      for {
        created <- acs
          .markContractsActive(Seq.empty[LfContractId], TimeOfChange(rc, ts))
          .value
      } yield assert(created.successful, "succeed")
    }

    "bulk creates report all errors" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc2, ts2)
      for {
        created1 <- acs
          .markContractsActive(
            Seq(coid00, coid01),
            toc1,
          )
          .value
        created2 <- acs
          .markContractsActive(
            Seq(
              coid00,
              coid01,
              coid10,
            ),
            toc2,
          )
          .value
        fetch <- acs.fetchStates(Seq(coid00, coid01, coid10))
      } yield {
        created1 shouldBe Symbol("successful")
        created2 shouldBe Symbol("isResult")
        created2.nonaborts should (equal(
          Chain(
            DoubleContractCreation(coid00, toc1, toc2),
            DoubleContractCreation(coid01, toc1, toc2),
          )
        ) or equal(
          created2.nonaborts == Chain(
            DoubleContractCreation(coid01, toc1, toc2),
            DoubleContractCreation(coid00, toc1, toc2),
          )
        ))
        fetch shouldBe Map(
          coid00 -> ContractState(active, toc2),
          coid01 -> ContractState(active, toc2),
          coid10 -> ContractState(active, toc2),
        )
      }
    }

    "bulk archivals archive all contracts" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc2, ts2)
      for {
        created <- acs
          .markContractsActive(
            Seq(
              coid00,
              coid01,
              coid10,
            ),
            toc,
          )
          .value
        archived <- acs
          .archiveContracts(Seq(coid00, coid01, coid10), toc2)
          .value
        snapshot <- acs.snapshot(ts2)
        fetch <- acs.fetchStates(Seq(coid00, coid01, coid10))
      } yield {
        created shouldBe Symbol("successful")
        archived shouldBe Symbol("successful")
        snapshot shouldBe Map.empty
        fetch shouldBe Map(
          coid00 -> ContractState(Archived, toc2),
          coid01 -> ContractState(Archived, toc2),
          coid10 -> ContractState(Archived, toc2),
        )
      }
    }

    "bulk archivals report all errors" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc2, ts2)
      for {
        archived1 <- acs
          .archiveContracts(Seq(coid00, coid01), toc)
          .value
        archived2 <- acs
          .archiveContracts(Seq(coid00, coid01, coid10), toc2)
          .value
        fetch <- acs.fetchStates(Seq(coid00, coid01, coid10))
      } yield {
        archived1 shouldBe Symbol("successful")
        archived2 shouldBe Symbol("isResult")
        archived2.nonaborts should (equal(
          Chain(
            DoubleContractArchival(coid00, toc, toc2),
            DoubleContractArchival(coid01, toc, toc2),
          )
        ) or equal(
          Chain(
            DoubleContractArchival(coid01, toc, toc2),
            DoubleContractArchival(coid00, toc, toc2),
          )
        ))
        fetch shouldBe Map(
          coid00 -> ContractState(
            Archived,
            toc2,
          ), // return the latest archival
          coid01 -> ContractState(
            Archived,
            toc2,
          ), // keep the second archival
          coid10 -> ContractState(
            Archived,
            toc2,
          ), // archive the contract even if some archivals fail
        )
      }
    }

    "bulk archival with empty set" in {
      val acs = mk()
      for {
        archived <- acs
          .archiveContracts(Seq.empty[LfContractId], TimeOfChange(rc, ts))
          .value
      } yield assert(archived.successful, "succeed")
    }

    val sourceDomain1 = SourceDomainId(DomainId(UniqueIdentifier.tryCreate("domain1", "DOMAIN1")))
    val targetDomain1 = TargetDomainId(DomainId(UniqueIdentifier.tryCreate("domain1", "DOMAIN1")))

    val sourceDomain2 = SourceDomainId(DomainId(UniqueIdentifier.tryCreate("domain2", "DOMAIN2")))
    val targetDomain2 = TargetDomainId(DomainId(UniqueIdentifier.tryCreate("domain2", "DOMAIN2")))

    "transfer-out makes a contract inactive" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc + 1, ts.plusSeconds(1))
      for {
        created <- acs
          .markContractsActive(
            Seq(coid00, coid01),
            toc,
          )
          .value
        transferOut <- acs
          .transferOutContract(coid00, toc2, targetDomain1)
          .value
        fetch00 <- acs.fetchState(coid00)
        snapshot1 <- acs.snapshot(ts.plusMillis(1))
        snapshot2 <- acs.snapshot(toc2.timestamp)
      } yield {
        assert(created.successful, "creations succeed")
        assert(transferOut.successful, "transfer-out succeeds")
        assert(
          fetch00.contains(
            ContractState(
              TransferredAway(targetDomain1),
              toc2.rc,
              toc2.timestamp,
            )
          ),
          s"Contract $coid00 has been transferred away",
        )
        assert(
          snapshot1 == Map(
            coid00 -> toc.timestamp,
            coid01 -> toc.timestamp,
          ),
          "All contracts are active",
        )
        assert(
          snapshot2 == Map(coid01 -> toc.timestamp),
          s"Transferred contract is inactive",
        )
      }
    }

    "transfer-in makes a contract active" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      for {
        transferIn <- acs
          .transferInContract(coid00, toc, sourceDomain1)
          .value
        fetch <- acs.fetchState(coid00)
        snapshot1 <- acs.snapshot(ts.minusSeconds(1))
        snapshot2 <- acs.snapshot(ts)
      } yield {
        assert(transferIn.successful, "transfer-in succeeds")
        assert(
          fetch.contains(ContractState(active, rc, ts)),
          s"transferred-in contract $coid00 is active",
        )
        assert(
          snapshot1 == Map.empty,
          s"Transferred contract is not active before the transfer",
        )
        assert(
          snapshot2 == Map(coid00 -> ts),
          s"Transferred contract becomes active with the transfer-in",
        )
      }
    }

    "contracts can be transferred multiple times" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc + 2, ts.plusSeconds(1))
      val toc3 = TimeOfChange(rc + 4, ts.plusSeconds(3))
      val toc4 = TimeOfChange(rc + 5, ts.plusSeconds(6))
      val toc5 = TimeOfChange(rc + 6, ts.plusSeconds(7))
      val toc6 = TimeOfChange(rc + 7, ts.plusSeconds(70))
      for {
        create <- acs.markContractActive(coid00, toc1).value
        fetch0 <- acs.fetchState(coid00)
        out1 <- acs.transferOutContract(coid00, toc2, targetDomain2).value
        fetch1 <- acs.fetchState(coid00)
        in1 <- acs.transferInContract(coid00, toc3, sourceDomain1).value
        fetch2 <- acs.fetchState(coid00)
        out2 <- acs
          .transferOutContract(coid00, toc4, targetDomain1)
          .value
        fetch3 <- acs.fetchState(coid00)
        in2 <- acs.transferInContract(coid00, toc5, sourceDomain2).value
        fetch4 <- acs.fetchState(coid00)
        archived <- acs.archiveContract(coid00, toc6).value
        snapshot1 <- acs.snapshot(toc1.timestamp)
        snapshot2 <- acs.snapshot(toc2.timestamp)
        snapshot3 <- acs.snapshot(toc3.timestamp)
        snapshot4 <- acs.snapshot(toc4.timestamp)
        snapshot5 <- acs.snapshot(toc5.timestamp)
        snapshot6 <- acs.snapshot(toc6.timestamp)
      } yield {
        assert(create.successful, "creation succeeds")
        assert(
          fetch0.contains(ContractState(active, toc1.rc, toc1.timestamp)),
          s"Contract $coid00 is active after the creation",
        )
        assert(out1.successful, "first transfer-out succeeds")
        assert(
          fetch1.contains(
            ContractState(
              TransferredAway(targetDomain2),
              toc2.rc,
              toc2.timestamp,
            )
          ),
          s"Contract $coid00 is transferred away",
        )
        assert(in1.successful, "first transfer-in succeeds")
        assert(
          fetch2.contains(ContractState(Active, toc3.rc, toc3.timestamp)),
          s"Contract $coid00 is active after the first transfer-in",
        )
        assert(out2.successful, "second transfer-out succeeds")
        assert(
          fetch3.contains(
            ContractState(
              TransferredAway(targetDomain1),
              toc4.rc,
              toc4.timestamp,
            )
          ),
          s"Contract $coid00 is again transferred away",
        )
        assert(in2.successful, "second transfer-in succeeds")
        assert(
          fetch4.contains(ContractState(Active, toc5.rc, toc5.timestamp)),
          s"Second transfer-in reactivates contract $coid00",
        )
        assert(archived.successful, "archival succeeds")
        assert(
          snapshot1 == Map(coid00 -> (toc1.timestamp)),
          "contract is created",
        )
        assert(
          snapshot2 == Map.empty,
          "first transfer-out removes contract from the snapshot",
        )
        assert(
          snapshot3 == Map(coid00 -> toc3.timestamp),
          "first transfer-in reactivates the contract",
        )
        assert(snapshot4 == Map.empty, "second transfer-out removes the contract again")
        assert(
          snapshot5 == Map(coid00 -> toc5.timestamp),
          "second transfer-in reactivates the contract",
        )
        assert(snapshot6 == Map.empty, "archival archives the contract")
      }
    }

    "transfers can be stored out of order" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc + 2, ts.plusSeconds(1))
      val toc3 = TimeOfChange(rc + 4, ts.plusSeconds(3))
      val toc4 = TimeOfChange(rc + 5, ts.plusSeconds(6))
      val toc5 = TimeOfChange(rc + 6, ts.plusSeconds(7))
      val toc6 = TimeOfChange(rc + 7, ts.plusSeconds(70))
      for {
        out1 <- acs.transferOutContract(coid00, toc2, targetDomain1).value
        archived <- acs.archiveContract(coid00, toc6).value
        out2 <- acs.transferOutContract(coid00, toc4, targetDomain2).value
        in2 <- acs.transferInContract(coid00, toc5, sourceDomain2).value
        in1 <- acs.transferInContract(coid00, toc3, sourceDomain1).value
        create <- acs.markContractActive(coid00, toc1).value
        snapshot1 <- acs.snapshot(toc1.timestamp)
        snapshot2 <- acs.snapshot(toc2.timestamp)
        snapshot3 <- acs.snapshot(toc3.timestamp)
        snapshot4 <- acs.snapshot(toc4.timestamp)
        snapshot5 <- acs.snapshot(toc5.timestamp)
        snapshot6 <- acs.snapshot(toc6.timestamp)
      } yield {
        assert(create.successful, "creation succeeds")
        assert(out1.successful, "first transfer-out succeeds")
        assert(in1.successful, "first transfer-in succeeds")
        assert(out2.successful, "second transfer-out succeeds")
        assert(in2.successful, "second transfer-in succeeds")
        assert(archived.successful, "archival succeeds")
        assert(
          snapshot1 == Map(coid00 -> toc1.timestamp),
          "contract is created",
        )
        assert(
          snapshot2 == Map.empty,
          "first transfer-out removes contract from the snapshot",
        )
        assert(
          snapshot3 == Map(coid00 -> toc3.timestamp),
          "first transfer-in reactivates the contract",
        )
        assert(snapshot4 == Map.empty, "second transfer-out removes the contract again")
        assert(
          snapshot5 == Map(coid00 -> (toc5.timestamp)),
          "second transfer-in reactivates the contract",
        )
        assert(snapshot6 == Map.empty, "archival archives the contract")
      }
    }

    "transfer-out is idempotent" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      for {
        out1 <- acs.transferOutContract(coid00, toc, targetDomain1).value
        out2 <- acs.transferOutContract(coid00, toc, targetDomain1).value
        fetch <- acs.fetchState(coid00)
      } yield {
        out1 shouldBe Symbol("successful")
        out2 shouldBe Symbol("successful")

        assert(
          fetch.contains(
            ContractState(TransferredAway(targetDomain1), rc, ts)
          ),
          "contract is transferred away",
        )
      }
    }

    "transfer-in is idempotent" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      for {
        in1 <- acs.transferInContract(coid00, toc, sourceDomain1).value
        in2 <- acs.transferInContract(coid00, toc, sourceDomain1).value
        fetch <- acs.fetchState(coid00)
      } yield {
        in1 shouldBe Symbol("successful")
        in2 shouldBe Symbol("successful")

        assert(fetch.contains(ContractState(active, rc, ts)), "contract is transferred in")
      }
    }

    "simultaneous transfer-in and out" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      for {
        out <- acs.transferOutContract(coid00, toc, targetDomain2).value
        in <- acs.transferInContract(coid00, toc, sourceDomain1).value
        fetch <- acs.fetchState(coid00)
        snapshot <- acs.snapshot(ts)
      } yield {
        assert(out.successful, "transfer-out succeeds")
        assert(in.successful, "transfer-in succeeds")
        assert(
          fetch.contains(
            ContractState(TransferredAway(targetDomain2), rc, ts)
          ),
          "contract is transferred away",
        )
        assert(snapshot == Map.empty, "contract is not in snapshot")
      }
    }

    "complain about simultaneous transfer-ins" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      for {
        in1 <- acs.transferInContract(coid00, toc1, sourceDomain1).value
        in2 <- acs.transferInContract(coid00, toc1, sourceDomain2).value
        in3 <- acs.transferInContract(coid00, toc1, sourceDomain1).value
        fetch <- acs.fetchState(coid00)
        snapshot <- acs.snapshot(ts)
      } yield {
        assert(in1.successful, "first transfer-in succeeds")
        assert(
          in2.isResult && in2.nonaborts == Chain(
            SimultaneousActivation(
              coid00,
              toc1,
              TransferDetails(sourceDomain1),
              TransferDetails(sourceDomain2),
            )
          ),
          "second transfer-in is flagged",
        )
        // third transfer-in is idempotent
        in3 shouldBe Symbol("successful")

        assert(
          fetch.contains(ContractState(active, rc, ts)),
          s"earlier insertion wins",
        )
        assert(snapshot == Map(coid00 -> ts))
      }
    }

    "complain about simultaneous transfer-outs" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      for {
        out1 <- acs.transferOutContract(coid00, toc1, targetDomain1).value
        out2 <- acs.transferOutContract(coid00, toc1, targetDomain2).value
        out3 <- acs.transferOutContract(coid00, toc1, targetDomain1).value
        fetch <- acs.fetchState(coid00)
      } yield {
        assert(out1.successful, "first transfer-out succeeds")
        assert(
          out2.isResult && out2.nonaborts == Chain(
            SimultaneousDeactivation(
              coid00,
              toc1,
              TransferDetails(targetDomain1),
              TransferDetails(targetDomain2),
            )
          ),
          "second transfer-out is flagged",
        )
        // third transfer-out is idempotent
        out3 shouldBe Symbol("successful")
        assert(
          fetch.contains(
            ContractState(TransferredAway(targetDomain1), rc, ts)
          ),
          s"earlier insertion wins",
        )
      }
    }
    "complain about simultaneous archivals and transfer-outs" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      for {
        out <- acs.transferOutContract(coid00, toc, targetDomain1).value
        arch <- acs.archiveContract(coid00, toc).value
        fetch <- acs.fetchState(coid00)
      } yield {
        assert(out.successful, "transfer-out succeeds")
        assert(
          arch.isResult && arch.nonaborts == Chain(
            SimultaneousDeactivation(
              coid00,
              toc,
              TransferDetails(targetDomain1),
              CreationArchivalDetail,
            )
          ),
          "archival is flagged",
        )
        assert(
          fetch.contains(
            ContractState(TransferredAway(targetDomain1), rc, ts)
          ),
          s"earlier insertion wins",
        )
      }
    }

    "complain about simultaneous creations and transfer-ins" in {
      val acs = mk()
      val toc = TimeOfChange(rc, ts)
      for {
        create <- acs.markContractActive(coid00, toc).value
        in <- acs.transferInContract(coid00, toc, sourceDomain1).value
        fetch <- acs.fetchState(coid00)
      } yield {
        assert(create.successful, "create succeeds")
        assert(
          in.isResult && in.nonaborts == Chain(
            SimultaneousActivation(
              coid00,
              toc,
              CreationArchivalDetail,
              TransferDetails(sourceDomain1),
            )
          ),
          "transfer-in is flagged",
        )
        assert(fetch.contains(ContractState(active, rc, ts)))
      }
    }

    "complain about changes after archival" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc + 1, ts.plusSeconds(1))
      val toc3 = TimeOfChange(rc + 3, ts.plusSeconds(2))
      val toc4 = TimeOfChange(rc + 2, ts.plusSeconds(3))
      for {
        archive <- acs.archiveContract(coid00, toc2).value
        out4 <- acs.transferOutContract(coid00, toc4, targetDomain1).value
        fetch4 <- acs.fetchState(coid00)
        out1 <- acs.transferOutContract(coid00, toc1, targetDomain2).value
        in3 <- acs.transferInContract(coid00, toc3, sourceDomain1).value
        snapshot1 <- acs.snapshot(toc1.timestamp)
        snapshot3 <- acs.snapshot(toc3.timestamp)
        snapshot4 <- acs.snapshot(toc4.timestamp)
        in4 <- acs.transferInContract(coid01, toc4, sourceDomain2).value
        archive2 <- acs.archiveContract(coid01, toc2).value
      } yield {
        assert(archive.successful, "archival succeeds")
        assert(
          out4.isResult && out4.nonaborts == Chain(ChangeAfterArchival(coid00, toc2, toc4)),
          s"transfer-out after archival fails",
        )
        assert(
          fetch4.contains(
            ContractState(
              TransferredAway(targetDomain1),
              toc4.rc,
              toc4.timestamp,
            )
          )
        )
        assert(out1.successful, "transfer-out before archival succeeds")
        assert(in3.isResult && in3.nonaborts == Chain(ChangeAfterArchival(coid00, toc2, toc3)))
        assert(snapshot1 == Map.empty, "contract is inactive after the first transfer-out")
        assert(
          snapshot3 == Map(coid00 -> toc3.timestamp),
          "archival deactivates transferred-in contract",
        )
        assert(snapshot4 == Map.empty, "second transfer-out deactivates the contract again")
        assert(in4.successful, s"transfer-in of $coid01 succeeds")
        assert(
          archive2.isResult && archive2.nonaborts == Chain(ChangeAfterArchival(coid01, toc2, toc4)),
          s"archival of $coid01 reports later transfer-in",
        )
      }
    }

    "complain about changes before creation" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc + 1, ts.plusSeconds(1))
      val toc3 = TimeOfChange(rc + 3, ts.plusSeconds(2))
      val toc4 = TimeOfChange(rc + 2, ts.plusSeconds(3))
      for {
        create <- acs.markContractActive(coid00, toc3).value
        in1 <- acs.transferInContract(coid00, toc1, sourceDomain1).value
        fetch3 <- acs.fetchState(coid00)
        in4 <- acs.transferInContract(coid00, toc4, sourceDomain2).value
        out2 <- acs.transferOutContract(coid00, toc2, targetDomain1).value
        snapshot1 <- acs.snapshot(toc1.timestamp)
        snapshot2 <- acs.snapshot(toc2.timestamp)
        snapshot3 <- acs.snapshot(toc3.timestamp)
      } yield {
        assert(create.successful, "creation succeeds")
        assert(
          in1.isResult && in1.nonaborts.toList.contains(ChangeBeforeCreation(coid00, toc3, toc1)),
          s"transfer-in before creation fails",
        )
        assert(fetch3.contains(ContractState(active, toc3.rc, toc3.timestamp)))
        assert(in4.successful, "transfer-in after creation succeeds")
        assert(
          out2.isResult && out2.nonaborts.toList.contains(
            ChangeBeforeCreation(coid00, toc3, toc2)
          )
        )
        assert(
          snapshot1 == Map(coid00 -> (toc1.timestamp)),
          "contract is active after the first transfer-in",
        )
        assert(snapshot2 == Map.empty, "transfer-out deactivates the contract")
        assert(
          snapshot3 == Map(coid00 -> (toc3.timestamp)),
          "creation activates the contract again",
        )
      }
    }

    "prune exactly the old archived contracts" in {
      val acs = mk()
      val rc3 = RequestCounter(2)
      val ts3 = ts2.addMicros(1)
      val coid11 = ExampleTransactionFactory.suffixedId(1, 1)
      val coid20 = ExampleTransactionFactory.suffixedId(2, 0)
      val coid21 = ExampleTransactionFactory.suffixedId(2, 1)

      val toc = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc2, ts2)
      val toc3 = TimeOfChange(rc3, ts3)
      for {
        _ <- acs
          .markContractsActive(
            Seq(coid00, coid01),
            toc,
          )
          .value
        _ <- acs
          .markContractsActive(
            Seq(coid10, coid11),
            toc,
          )
          .value
        _ <- acs.archiveContracts(Seq(coid00), toc2).value
        _ <- acs.transferOutContract(coid11, toc2, targetDomain1).value
        _ <- acs.archiveContracts(Seq(coid01), toc3).value
        _ <- acs
          .markContractsActive(
            Seq(coid20, coid21),
            toc3,
          )
          .value
        _ <- acs
          .archiveContract(coid21, toc3)
          .value // Transient contract coid21
        _ <- acs.prune(ts2)
        status <- acs.pruningStatus
        fetch <- acs.fetchStates(Seq(coid00, coid01, coid10, coid11, coid20, coid21))
        count <- acs.contractCount(ts3)
        _ <- acs.prune(ts3)
        fetcha <- acs.fetchStates(Seq(coid20, coid21))
      } yield {
        status shouldBe Some(PruningStatus(PruningPhase.Completed, ts2, Some(ts2)))
        fetch shouldBe Map(
          // pruned the first contract
          coid01 -> ContractState(
            Archived,
            toc3,
          ), // didn't prune the second contract (archived only after pruning timestamp)
          coid10 -> ContractState(active, toc), // didn't prune the third (active) contract
          // pruned the fourth contract
          coid20 -> ContractState(
            active,
            toc3,
          ), // didn't prune contract created after the timestamp
          coid21 -> ContractState(
            Archived,
            toc3,
          ), // didn't prune contract created after the timestamp
        )
        count shouldBe 4
        fetcha shouldBe Map(
          coid20 -> ContractState(
            active,
            toc3,
          ) // didn't prune the contract created at the timestamp
          // pruned the transient contract at the pruning timestamp
        )
      }
    }

    "only prune contracts with deactivations" in {
      val acs = mk()
      val Seq(toc1, toc2, toc3, toc4) =
        (0L to 3L).map(i => TimeOfChange(rc + i, ts.addMicros(i)))
      val activations = List(toc1, toc2, toc3)
      for {
        transferIns <- activations.parTraverse { case toc =>
          acs.transferInContract(coid00, toc, sourceDomain1).value
        }
        _ <- acs.prune(toc4.timestamp)
        snapshotsTakenAfterIgnoredPrune <- activations.parTraverse(toc =>
          acs.snapshot(toc.timestamp)
        )
        countsAfterIgnoredPrune <- activations.parTraverse(toc => acs.contractCount(toc.timestamp))
        // the presence of an archival/deactivation enables pruning
        _ <- acs.archiveContract(coid00, toc4).value
        _ <- acs.prune(toc4.timestamp)

        snapshotsTakenAfterActualPrune <- activations.parTraverse(toc =>
          acs.snapshot(toc.timestamp)
        )
        countsAfterActualPrune <- activations.parTraverse(toc => acs.contractCount(toc.timestamp))
      } yield {
        transferIns.foreach { in =>
          assert(in.successful, s"transfer-in succeeds")
        }
        countsAfterIgnoredPrune.foreach(count =>
          assert(count == 1, "should not have pruned contract")
        )

        snapshotsTakenAfterActualPrune.foreach(s =>
          assert(s == Map.empty, "contract supposed to be pruned")
        )
        countsAfterActualPrune.foreach(count => assert(count == 0, "should have pruned contract"))
        succeed
      }
    }

    "return a snapshot sorted in the contract ID order" in {

      val acs = mk()

      val coid11 = ExampleTransactionFactory.suffixedId(1, 1)
      val ts1 = ts.plusSeconds(1)
      val ts2 = ts.plusSeconds(2)
      val toc = TimeOfChange(rc, ts)
      val toc1 = TimeOfChange(rc + 1, ts1)
      val toc2 = TimeOfChange(rc + 2, ts2)

      for {
        _ <- acs.markContractActive(coid00, toc1).value
        _ <- acs
          .markContractsActive(
            Seq(coid10, coid11),
            toc,
          )
          .value
        _ <- acs.markContractActive(coid01, toc2).value
        snapshot <- acs.snapshot(ts2)
      } yield {
        val idOrdering = Ordering[LfContractId]
        val resultOrdering = Ordering.Tuple2[LfContractId, CantonTimestamp]
        snapshot.toList shouldBe snapshot.toList.sorted(resultOrdering)
        snapshot.keys.toList shouldBe snapshot.keys.toList.sorted(idOrdering)
      }
    }

    "deleting from request counter" in {
      val acs = mk()

      val ts1 = ts.plusSeconds(1)
      val ts2 = ts.plusSeconds(2)
      val ts4 = ts.plusSeconds(4)
      val toc0 = TimeOfChange(rc, ts)
      val toc1 = TimeOfChange(rc + 1, ts1)
      val toc2 = TimeOfChange(rc + 2, ts2)
      val toc32 = TimeOfChange(rc + 3, ts2)
      val toc4 = TimeOfChange(rc + 4, ts4)
      for {
        _ <- valueOrFail(acs.markContractActive(coid00, toc1))(
          s"create $coid00"
        )
        _ <- acs.transferInContract(coid00, toc0, sourceDomain1).value
        _ <- valueOrFail(acs.transferOutContract(coid00, toc32, targetDomain2))(
          s"transfer-out $coid00"
        )
        _ <- valueOrFail(acs.transferInContract(coid00, toc4, sourceDomain1))(
          s"transfer-in $coid00"
        )
        _ <- acs.archiveContract(coid00, toc2).value
        fetch004 <- acs.fetchState(coid00)
        _ <- acs.deleteSince(rc + 3)
        fetch003 <- acs.fetchState(coid00)
        _ <- acs.deleteSince(rc + 2)
        fetch002 <- acs.fetchState(coid00)
        _ <- acs.deleteSince(rc + 1)
        fetch001 <- acs.fetchState(coid00)
        _ <- valueOrFail(acs.archiveContract(coid00, toc32))(
          s"archive $coid00 at $toc32"
        )
        _ <- valueOrFail(
          acs.markContractActive(
            coid00,
            TimeOfChange(rc - 1, ts.plusSeconds(-1)),
          )
        )(
          s"re-create $coid00"
        )
      } yield {
        fetch004 shouldBe Some(ContractState(Active, toc4.rc, toc4.timestamp))
        fetch003 shouldBe Some(ContractState(Archived, toc2.rc, toc2.timestamp))
        fetch002 shouldBe Some(ContractState(active, toc1.rc, toc1.timestamp))
        fetch001 shouldBe Some(ContractState(active, toc0.rc, toc0.timestamp))
      }
    }

    "contract snapshot" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc2, ts2)
      val toc3 = TimeOfChange(rc3, ts3)
      for {
        _ <- valueOrFail(
          acs.markContractsActive(
            Seq(coid00, coid01),
            toc1,
          )
        )(
          s"create contracts at $toc1"
        )
        _ <- valueOrFail(acs.markContractsActive(Seq(coid10), toc2))(
          s"create contracts at $toc2"
        )
        snapshot1 <- acs.contractSnapshot(Set(coid00, coid10), toc1.timestamp)
        snapshot2 <- acs.contractSnapshot(Set(coid00, coid10), toc2.timestamp)
        empty <- acs.contractSnapshot(Set(), toc2.timestamp)
        _ <- valueOrFail(acs.archiveContract(coid01, toc3))(
          s"archive contract $coid10"
        )
        snapshot3 <- acs.contractSnapshot(Set(coid10, coid01), toc3.timestamp)
        inactive <- acs.contractSnapshot(Set(coid01), toc3.timestamp)
      } yield {
        snapshot1 shouldBe Map(coid00 -> toc1.timestamp)
        snapshot2 shouldBe Map(coid00 -> toc1.timestamp, coid10 -> toc2.timestamp)
        empty shouldBe Map.empty[LfContractId, CantonTimestamp]
        snapshot3 shouldBe Map(coid10 -> toc2.timestamp)
        inactive shouldBe Map.empty[LfContractId, CantonTimestamp]
      }
    }

    "snapshot with same timestamp and different request counters" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc + 1L, ts)
      val toc3 = TimeOfChange(rc + 2L, ts)
      for {
        _ <- valueOrFail(
          acs.transferInContracts(
            Seq(
              (coid00, sourceDomain1, toc1),
              (coid01, sourceDomain2, toc1),
            )
          )
        )(
          s"transfer-in contracts at $toc1"
        )
        _ <- valueOrFail(
          acs.transferOutContracts(Seq((coid01, targetDomain1, toc1)))
        )(
          s"transferOut contracts at $toc1"
        )
        snapshot1 <- acs.snapshot(ts)
        csnapshot1 <- acs.contractSnapshot(Set(coid00, coid01), ts)

        _ <- valueOrFail(
          acs.transferInContract(coid01, toc2, sourceDomain1)
        )(
          s"transferIn contract at $toc2"
        )
        _ <- valueOrFail(
          acs.transferOutContract(coid00, toc2, targetDomain1)
        )(
          s"transferOut contract at $toc2"
        )
        snapshot2 <- acs.snapshot(ts)
        csnapshot2 <- acs.contractSnapshot(Set(coid00, coid01), ts)

        _ <- valueOrFail(
          acs.transferInContract(coid00, toc3, sourceDomain2)
        )(
          s"transferIn contract at $toc3"
        )
        snapshot3 <- acs.snapshot(ts)
        csnapshot3 <- acs.contractSnapshot(Set(coid00, coid01), ts)
      } yield {
        snapshot1 shouldBe Map(coid00 -> (toc1.timestamp))
        csnapshot1 shouldBe Map(coid00 -> toc1.timestamp)

        snapshot2 shouldBe Map(coid01 -> (toc2.timestamp))
        csnapshot2 shouldBe Map(coid01 -> toc2.timestamp)

        snapshot3 shouldBe Map(
          coid00 -> toc3.timestamp,
          coid01 -> toc2.timestamp,
        )
        csnapshot3 shouldBe Map(coid00 -> toc3.timestamp, coid01 -> toc2.timestamp)
      }
    }

    "snapshot with same request counter and different timestamps" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc, ts.plusSeconds(1))
      val toc3 = TimeOfChange(rc, ts.plusSeconds(2))

      for {
        _ <- valueOrFail(
          acs.transferInContracts(
            Seq(
              (coid00, sourceDomain1, toc1),
              (coid01, sourceDomain2, toc1),
            )
          )
        )(
          s"transfer-in contracts at $toc1"
        )
        _ <- valueOrFail(
          acs.transferOutContracts(Seq((coid01, targetDomain1, toc1)))
        )(
          s"transferOut contracts at $toc1"
        )
        snapshot1 <- acs.snapshot(rc)

        _ <- valueOrFail(
          acs.transferInContract(coid01, toc2, sourceDomain1)
        )(
          s"transferIn contract at $toc2"
        )
        _ <- valueOrFail(
          acs.transferOutContract(coid00, toc2, targetDomain1)
        )(
          s"transferOut contract at $toc2"
        )
        snapshot2 <- acs.snapshot(rc)

        _ <- valueOrFail(
          acs.transferInContract(coid00, toc3, sourceDomain2)
        )(
          s"transferIn contract at $toc3"
        )
        snapshot3 <- acs.snapshot(rc)
      } yield {
        snapshot1 shouldBe Map(coid00 -> (toc1.rc))
        snapshot2 shouldBe Map(coid01 -> (toc2.rc))
        snapshot3 shouldBe Map(
          coid00 -> (toc3.rc),
          coid01 -> (toc2.rc),
        )
      }
    }

    val rc4 = rc3 + 1
    val ts4 = ts3.plusMillis(1)
    val rc5 = rc4 + 1
    val ts5 = ts4.plusMillis(1)
    val rc6 = rc5 + 1
    val ts6 = ts5.plusMillis(1)

    "return correct changes" in {
      val acs = mk()
      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc2, ts2)
      val toc3 = TimeOfChange(rc3, ts3)
      val toc4 = TimeOfChange(rc4, ts4)
      val toc5 = TimeOfChange(rc5, ts5)
      val toc6 = TimeOfChange(rc6, ts6)

      for {
        _ <- valueOrFail(
          acs.markContractsActive(
            Seq(coid00, coid01),
            toc1,
          )
        )(
          s"create contracts at $toc1"
        )

        _ <- valueOrFail(acs.markContractsActive(Seq(coid10), toc2))(
          s"create contracts at $toc2"
        )
        _ <- valueOrFail(acs.archiveContract(coid01, toc2))(
          s"archive contract $coid10"
        )
        _ <- valueOrFail(acs.transferInContract(coid11, toc2, sourceDomain2))(
          s"transfer in $coid11"
        )
        _ <- valueOrFail(
          acs.transferOutContract(coid11, toc3, targetDomain2)
        )(
          s"transfer out $coid11"
        )
        _ <- valueOrFail(acs.archiveContract(coid10, toc3))(
          s"archive contract $coid10"
        )

        _ <- valueOrFail(acs.transferInContract(coid11, toc4, sourceDomain2))(
          s"transfer in $coid11 again"
        )

        _ <- valueOrFail(acs.archiveContract(coid11, toc5))(s"archive contract $coid11")

        _ <- valueOrFail(acs.archiveContract(coid00, toc6))(
          s"archive contract $coid00"
        )
        changes <- acs.changesBetween(toc1, toc5)
      } yield {
        changes.toList shouldBe List(
          (
            toc2,
            ActiveContractIdsChange(
              activations = Map(
                coid10 -> StateChangeType(ContractChange.Created),
                coid11 -> StateChangeType(ContractChange.Assigned),
              ),
              deactivations = Map(coid01 -> StateChangeType(ContractChange.Archived)),
            ),
          ),
          (
            toc3,
            ActiveContractIdsChange(
              activations = Map.empty,
              deactivations = Map(
                coid10 -> StateChangeType(ContractChange.Archived),
                coid11 -> StateChangeType(ContractChange.Unassigned),
              ),
            ),
          ),
          (
            toc4,
            ActiveContractIdsChange(
              activations = Map(coid11 -> StateChangeType(ContractChange.Assigned)),
              deactivations = Map.empty,
            ),
          ),
          (
            toc5,
            ActiveContractIdsChange(
              activations = Map.empty,
              deactivations = Map(coid11 -> StateChangeType(ContractChange.Archived)),
            ),
          ),
        )
      }
    }

    "look up active contracts for a package" when {

      val packageId = LfPackageId.assertFromString("package-id-0")
      val packageId1 = LfPackageId.assertFromString("package-id-1")
      val packageId2 = LfPackageId.assertFromString("package-id-2")
      val packageId3 = LfPackageId.assertFromString("package-id-3")

      val toc1 = TimeOfChange(rc, ts)
      val toc2 = TimeOfChange(rc + 1, ts.plusSeconds(1))
      val toc3 = TimeOfChange(rc + 2, ts.plusSeconds(3))

      val moduleName = QualifiedName.assertFromString("acsmodule:acstemplate")

      def addContractsToStore(
          contractStore: ContractStore,
          contracts: List[(LfContractId, LfPackageId)],
      ): Future[Unit] = {
        contracts.parTraverse_ { case (contractId, pkg) =>
          contractStore.storeCreatedContract(
            RequestCounter(0),
            transactionId(1),
            asSerializable(
              contractId,
              contractInstance = contractInstance(
                templateId = Ref.Identifier(
                  pkg,
                  moduleName,
                )
              ),
            ),
          )
        }
      }

      def activateMaybeDeactivate(
          activate: ActiveContractStore => CheckedT[Future, AcsError, AcsWarning, Unit] = { acs =>
            acs.markContractActive(coid00, toc1)
          },
          deactivate: Option[ActiveContractStore => CheckedT[Future, AcsError, AcsWarning, Unit]] =
            None,
      ): Future[Option[LfContractId]] = {
        val acs = mk()
        val contractStore = mkCS()
        for {
          _ <- addContractsToStore(contractStore, List(coid00 -> packageId))

          _ <- valueOrFail(activate(acs))(
            s"activate contracts"
          )

          _ <- valueOrFail(
            deactivate.fold(CheckedT.pure[Future, AcsError, AcsWarning](()))(f => f(acs))
          )(s"deactivate contracts")

          result <- acs.packageUsage(packageId, contractStore)
        } yield {
          result
        }
      }

      "there are no active contracts" in {
        val acs = mk()
        val contractStore = mkCS()
        for {
          none <- acs.packageUsage(packageId, contractStore)
        } yield {
          none shouldBe None
        }
      }

      "there is one active contract for the package" in {
        for {
          resO <-
            activateMaybeDeactivate()
        } yield { resO shouldBe Some(coid00) }
      }

      "a contract is transferred-in for the package" in {
        for {
          resO <- activateMaybeDeactivate(activate = { acs =>
            acs.transferInContract(
              coid00,
              toc1,
              SourceDomainId(acsDomainId),
            )
          })
        } yield { resO shouldBe Some(coid00) }
      }

      "a contract from the package has been created and archived" in {
        for {
          resO <- activateMaybeDeactivate(deactivate =
            Some(acs => acs.archiveContract(coid00, toc2))
          )
        } yield { resO shouldBe None }
      }

      "a contract from the package has been created and transferred out" in {
        for {
          resO <- activateMaybeDeactivate(deactivate =
            Some(acs => acs.transferOutContract(coid00, toc2, targetDomain2))
          )
        } yield { resO shouldBe None }
      }

      "contracts from other packages are active" in {
        val acs = mk()
        val contractStore = mkCS()
        val contracts = List(coid00 -> packageId, coid01 -> packageId1, coid10 -> packageId2)
        for {
          _ <- addContractsToStore(contractStore, contracts)

          activate = acs.markContractsActive(
            contracts.map(_._1),
            toc1,
          )
          _ <- valueOrFail(activate)(
            s"activate contracts $contracts"
          )

          some <- acs.packageUsage(packageId, contractStore)
          some1 <- acs.packageUsage(packageId1, contractStore)
          some2 <- acs.packageUsage(packageId2, contractStore)
          none <- acs.packageUsage(packageId3, contractStore)
        } yield {
          forEvery(List(some, some1, some2).zip(contracts)) { case (result, contract) =>
            result shouldBe Some(contract._1)
          }
          none shouldBe None
        }
      }

      "contract is transferred out then in again" in {
        val acs = mk()
        val contractStore = mkCS()
        for {
          _ <- addContractsToStore(contractStore, List(coid00 -> packageId))

          _ <- valueOrFail(acs.markContractActive(coid00, toc1))(
            s"create contract at $toc1"
          )

          _ <- valueOrFail(
            acs.transferOutContract(coid00, toc2, targetDomain1)
          )(
            s"transfer out contract at $toc2"
          )
          _ <- valueOrFail(
            acs.transferInContract(
              coid00,
              toc3,
              SourceDomainId(acsDomainId),
            )
          )(
            s"transfer in contract at $toc3"
          )
          some <- acs.packageUsage(packageId, contractStore)
        } yield {
          some shouldBe Some(coid00)
        }
      }

      "multiple contracts from the package are active" in {
        val acs = mk()
        val contractStore = mkCS()
        val contracts =
          List(coid00 -> packageId, coid01 -> packageId, coid10 -> packageId, coid11 -> packageId2)
        for {
          _ <- addContractsToStore(contractStore, contracts)

          activate = acs.markContractsActive(
            contracts.map(_._1),
            toc1,
          )
          _ <- valueOrFail(activate)(
            s"activate contracts $contracts"
          )
          _ <- valueOrFail(acs.archiveContract(coid00, toc2))(
            s"archive $coid00 at $toc2"
          )

          some <- acs.packageUsage(packageId, contractStore)
          some2 <- acs.packageUsage(packageId2, contractStore)
          none <- acs.packageUsage(packageId3, contractStore)
        } yield {
          some should (
            (be(Some(coid10)))
              or (be(Some(coid01)))
          )

          some2 shouldBe Some(coid11)
          none shouldBe None
        }

      }
    }
  }
}
