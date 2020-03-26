// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.time.Instant

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Node.{KeyWithMaintainers, NodeCreate, NodeFetch}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ValueText, VersionedValue}
import com.digitalasset.daml.lf.value.ValueVersions
import com.digitalasset.ledger.api.domain.{
  Filters,
  InclusiveFilters,
  RejectionReason,
  TransactionFilter
}
import com.digitalasset.platform.store.PersistenceEntry
import com.digitalasset.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, LoneElement, Matchers}

import scala.collection.immutable.HashMap
import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoLedgerEntriesSpec extends LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (ledger entries)"

  it should "be able to persist and load a checkpoint" in {
    val checkpoint = LedgerEntry.Checkpoint(Instant.now)
    val offset = nextOffset()

    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(offset, PersistenceEntry.Checkpoint(checkpoint))
      entry <- ledgerDao.lookupLedgerEntry(offset)
      endingOffset <- ledgerDao.lookupLedgerEnd()
    } yield {
      entry shouldEqual Some(checkpoint)
      endingOffset should be > startingOffset
    }
  }

  it should "be able to persist and load a rejection" in {
    val offset = nextOffset()
    val rejection = LedgerEntry.Rejection(
      Instant.now,
      s"commandId-${offset.toLong}",
      s"applicationId-${offset.toLong}",
      "party",
      RejectionReason.Inconsistent("\uED7Eᇫ뭳ꝳꍆꃓ왎"))

    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(offset, PersistenceEntry.Rejection(rejection))
      entry <- ledgerDao.lookupLedgerEntry(offset)
      endingOffset <- ledgerDao.lookupLedgerEnd()
    } yield {
      entry shouldEqual Some(rejection)
      endingOffset should be > startingOffset
    }
  }

  it should "be able to persist and load a transaction" in {
    val offset = nextOffset()
    val absCid = AbsoluteContractId("cId2")
    val let = Instant.now
    val txid = "trId2"
    val event1 = event(txid, 1)
    val event2 = event(txid, 2)

    val keyWithMaintainers = KeyWithMaintainers(
      VersionedValue(ValueVersions.acceptedVersions.head, ValueText("key2")),
      Set(Ref.Party.assertFromString("Alice"))
    )

    val transaction = LedgerEntry.Transaction(
      Some("commandId2"),
      txid,
      Some("appID2"),
      Some("Alice"),
      Some("workflowId"),
      let,
      let,
      GenTransaction(
        HashMap(
          event1 -> NodeCreate(
            nodeSeed = None,
            coid = absCid,
            coinst = someContractInstance,
            optLocation = None,
            signatories = Set(alice, bob),
            stakeholders = Set(alice, bob),
            key = Some(keyWithMaintainers)
          )),
        ImmArray(event1),
      ),
      Map(event1 -> Set("Alice", "Bob"), event2 -> Set("Alice", "In", "Chains"))
    )

    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(
        offset,
        PersistenceEntry.Transaction(transaction, Map.empty, List.empty))
      entry <- ledgerDao.lookupLedgerEntry(offset)
      endingOffset <- ledgerDao.lookupLedgerEnd()
    } yield {
      entry shouldEqual Some(transaction)
      endingOffset should be > startingOffset
    }
  }

  it should "be able to load contracts within a transaction" in {
    val offset = nextOffset()
    val offsetString = offset.toLong
    val absCid = AbsoluteContractId(s"cId$offsetString")
    val let = Instant.now

    val transactionId = s"trId$offsetString"
    val event1 = event(transactionId, 1)
    val event2 = event(transactionId, 2)

    val transaction = LedgerEntry.Transaction(
      Some(s"commandId$offsetString"),
      transactionId,
      Some(s"appID$offsetString"),
      Some("Alice"),
      Some("workflowId"),
      let,
      // normally the record time is some time after the ledger effective time
      let.plusMillis(42),
      GenTransaction(
        HashMap(
          event1 -> NodeCreate(
            nodeSeed = None,
            coid = absCid,
            coinst = someContractInstance,
            optLocation = None,
            signatories = Set(alice, bob),
            stakeholders = Set(alice, bob),
            key = None
          ),
          event2 -> NodeFetch(
            absCid,
            someContractInstance.template,
            None,
            Some(Set(alice, bob)),
            Set(alice, bob),
            Set(alice, bob),
            None,
          )
        ),
        ImmArray(event1, event2),
      ),
      Map(event1 -> Set("Alice", "Bob"), event2 -> Set("Alice", "In", "Chains"))
    )

    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(
        offset,
        PersistenceEntry.Transaction(transaction, Map.empty, List.empty))
      entry <- ledgerDao.lookupLedgerEntry(offset)
      endingOffset <- ledgerDao.lookupLedgerEnd()
    } yield {
      entry shouldEqual Some(transaction)
      endingOffset should be > startingOffset
    }
  }

  it should "be able to produce a valid snapshot" in {

    val sumSink = Sink.fold[Int, Int](0)(_ + _)
    val N = 1000
    val M = 10

    def runSequentially[U](n: Int, f: Int => Future[U]): Future[Seq[U]] =
      Source(1 to n).mapAsync(1)(f).runWith(Sink.seq)

    // Perform the following operations:
    // - Create N contracts
    // - Archive 1 contract
    // - Take a snapshot
    // - Create another M contracts
    // The resulting snapshot should contain N-1 contracts
    val aliceWildcardFilter =
      TransactionFilter(Map(alice -> Filters(None)))
    val aliceSpecificTemplatesFilter =
      TransactionFilter(Map(alice -> Filters(InclusiveFilters(Set(someTemplateId)))))

    val charlieWildcardFilter =
      TransactionFilter(Map(charlie -> Filters(None)))
    val charlieSpecificFilter =
      TransactionFilter(Map(charlie -> Filters(InclusiveFilters(Set(someTemplateId)))))

    val mixedFilter =
      TransactionFilter(
        Map(
          alice -> Filters(InclusiveFilters(Set(someTemplateId))),
          bob -> Filters(None),
          charlie -> Filters(None),
        ))

    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      aliceStartingSnapshot <- ledgerDao.getActiveContractSnapshot(
        startingOffset,
        aliceWildcardFilter)
      charlieStartingSnapshot <- ledgerDao.getActiveContractSnapshot(
        startingOffset,
        charlieWildcardFilter)

      mixedStartingSnapshot <- ledgerDao.getActiveContractSnapshot(startingOffset, mixedFilter)

      created <- runSequentially(N, _ => store(singleCreate))
      _ <- store(singleExercise(nonTransient(created.head._2).loneElement))

      snapshotOffset <- ledgerDao.lookupLedgerEnd()

      aliceWildcardSnapshot <- ledgerDao.getActiveContractSnapshot(
        snapshotOffset,
        aliceWildcardFilter)
      aliceSpecificTemplatesSnapshot <- ledgerDao.getActiveContractSnapshot(
        snapshotOffset,
        aliceSpecificTemplatesFilter)

      charlieWildcardSnapshot <- ledgerDao.getActiveContractSnapshot(
        snapshotOffset,
        charlieWildcardFilter)
      charlieSpecificTemplateSnapshot <- ledgerDao.getActiveContractSnapshot(
        snapshotOffset,
        charlieSpecificFilter)

      mixedSnapshot <- ledgerDao.getActiveContractSnapshot(snapshotOffset, mixedFilter)

      _ <- runSequentially(M, _ => store(singleCreate))

      endingOffset <- ledgerDao.lookupLedgerEnd()

      aliceStartingSnapshotSize <- aliceStartingSnapshot.acs.map(_ => 1).runWith(sumSink)
      aliceWildcardSnapshotSize <- aliceWildcardSnapshot.acs.map(_ => 1).runWith(sumSink)
      aliceSpecificTemplatesSnapshotSize <- aliceSpecificTemplatesSnapshot.acs
        .map(_ => 1)
        .runWith(sumSink)

      charlieStartingSnapshotSize <- charlieStartingSnapshot.acs.map(_ => 1).runWith(sumSink)
      charlieWildcardSnapshotSize <- charlieWildcardSnapshot.acs.map(_ => 1).runWith(sumSink)
      charlieSpecificTemplateSnapshotSize <- charlieSpecificTemplateSnapshot.acs
        .map(_ => 1)
        .runWith(sumSink)

      mixedStartingSnapshotSize <- mixedStartingSnapshot.acs.map(_ => 1).runWith(sumSink)
      mixedSnapshotSize <- mixedSnapshot.acs.map(_ => 1).runWith(sumSink)

    } yield {
      withClue("starting offset: ") {
        aliceStartingSnapshot.offset shouldEqual startingOffset
      }
      withClue("snapshot offset: ") {
        aliceWildcardSnapshot.offset shouldEqual snapshotOffset
        aliceSpecificTemplatesSnapshot.offset shouldEqual snapshotOffset
      }
      withClue("snapshot offset (2): ") {
        snapshotOffset.toLong shouldEqual (startingOffset.toLong + N + 1)
      }
      withClue("ending offset: ") {
        endingOffset.toLong shouldEqual (snapshotOffset.toLong + M)
      }
      withClue("alice wildcard snapshot size: ") {
        (aliceWildcardSnapshotSize - aliceStartingSnapshotSize) shouldEqual (N - 1)
      }
      withClue("alice specific template snapshot size: ") {
        (aliceSpecificTemplatesSnapshotSize - aliceStartingSnapshotSize) shouldEqual (N - 1)
      }
      withClue("charlie wildcard snapshot size: ") {
        (charlieWildcardSnapshotSize - charlieStartingSnapshotSize) shouldEqual 0
      }
      withClue("charlie specific template snapshot size: ") {
        (charlieSpecificTemplateSnapshotSize - charlieStartingSnapshotSize) shouldEqual 0
      }
      withClue("mixed snapshot size: ") {
        (mixedSnapshotSize - mixedStartingSnapshotSize) shouldEqual (N - 1)
      }
    }
  }

}
