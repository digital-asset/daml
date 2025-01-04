// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.option.*
import com.digitalasset.canton.FailOnShutdown
import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import org.scalatest.wordspec.AsyncWordSpec

trait DownloadTopologyStateForInitializationServiceTest
    extends AsyncWordSpec
    with TopologyStoreTestBase
    with FailOnShutdown {

  protected def createTopologyStore(synchronizerId: SynchronizerId): TopologyStore[DomainStore]

  val testData = new TopologyStoreTestData(testedProtocolVersion, loggerFactory, executionContext)
  import testData.*

  val bootstrapTransactions = StoredTopologyTransactions(
    Seq[
      (CantonTimestamp, (GenericSignedTopologyTransaction, Option[CantonTimestamp]))
    ](
      ts4 -> (dnd_p1seq, None),
      ts5 -> (ptp_fred_p1, None),
      ts5 -> (dtc_p2_domain1, None),
      ts6 -> (mds_med1_domain1, None),
      ts8 -> (sds_seq1_domain1, None),
    ).map { case (from, (tx, until)) =>
      StoredTopologyTransaction(
        SequencedTime(from),
        EffectiveTime(from),
        until.map(EffectiveTime(_)),
        tx,
        None,
      )
    }
  )

  val bootstrapTransactionsWithUpdates = StoredTopologyTransactions(
    Seq[
      (CantonTimestamp, (GenericSignedTopologyTransaction, Option[CantonTimestamp]))
    ](
      ts4 -> (dnd_p1seq, None),
      ts5 -> (ptp_fred_p1, None),
      ts5 -> (dtc_p2_domain1, ts6.some),
      ts6 -> (dtc_p2_domain1_update, None),
      ts6 -> (mds_med1_domain1, ts7.some),
      ts7 -> (mds_med1_domain1_update, None),
      ts8 -> (sds_seq1_domain1, None),
    ).map { case (from, (tx, until)) =>
      StoredTopologyTransaction(
        SequencedTime(from),
        EffectiveTime(from),
        until.map(EffectiveTime(_)),
        tx,
        None,
      )
    }
  )

  private def initializeStore(
      storedTransactions: GenericStoredTopologyTransactions
  ): FutureUnlessShutdown[TopologyStore[DomainStore]] = {
    val store = createTopologyStore(domain1_p1p2_synchronizerId)
    val groupedBySequencedTime =
      storedTransactions.result.groupBy(tx => (tx.sequenced, tx.validFrom)).toSeq.sortBy {
        case (sequenced, _) => sequenced
      }
    import com.digitalasset.canton.util.MonadUtil.syntax.*
    groupedBySequencedTime
      .sequentialTraverse_[FutureUnlessShutdown] {
        case ((sequencedTime, effectiveTime), transactions) =>
          store.update(
            sequencedTime,
            effective = EffectiveTime(effectiveTime.value),
            removeMapping = transactions.map(tx => tx.mapping.uniqueKey -> tx.serial).toMap,
            removeTxs = transactions.map(_.hash).toSet,
            additions = transactions.map(stored => ValidatedTopologyTransaction(stored.transaction)),
          )
      }
      .map(_ => store)
  }

  "DownloadTopologyStateForInitializationService" should {
    "return a valid topology state" when {
      "there's only one DomainTrustCertificate" in {
        for {
          store <- initializeStore(bootstrapTransactions)
          service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
          storedTxs <- service.initialSnapshot(dtc_p2_domain1.mapping.participantId)
        } yield {
          import storedTxs.result
          // all transactions should be valid and not expired
          result.foreach(_.validUntil shouldBe empty)
          result.map(_.transaction) shouldBe Seq(dnd_p1seq, ptp_fred_p1, dtc_p2_domain1)
        }
      }
      "the first DomainTrustCertificate is superseded by another one" in {
        for {
          store <- initializeStore(bootstrapTransactionsWithUpdates)
          service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
          storedTxs <- service.initialSnapshot(dtc_p2_domain1.mapping.participantId)
        } yield {
          import storedTxs.result
          // all transactions should be valid and not expired
          result.foreach(_.validUntil shouldBe empty)
          result.map(_.transaction) shouldBe Seq(dnd_p1seq, ptp_fred_p1, dtc_p2_domain1)
          result.last.validUntil shouldBe None
        }
      }

      "there's only one MediatorDomainState" in {
        for {
          store <- initializeStore(bootstrapTransactions)
          service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
          storedTxs <- service.initialSnapshot(med1Id)
        } yield {
          import storedTxs.result
          // all transactions should be valid and not expired
          result.foreach(_.validUntil shouldBe empty)
          result.map(_.transaction) shouldBe Seq(
            dnd_p1seq,
            ptp_fred_p1,
            dtc_p2_domain1,
            mds_med1_domain1,
          )
        }
      }

      "the first MediatorDomainState is superseded by another one" in {
        for {
          store <- initializeStore(bootstrapTransactionsWithUpdates)
          service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
          storedTxs <- service.initialSnapshot(med1Id)
        } yield {
          import storedTxs.result
          // all transactions should be valid and not validUntil capped at ts6
          result.foreach(_.validUntil.foreach(_.value should be <= ts6))
          result.map(_.transaction) shouldBe Seq(
            dnd_p1seq,
            ptp_fred_p1,
            dtc_p2_domain1,
            dtc_p2_domain1_update,
            mds_med1_domain1,
          )
          result.last.validUntil shouldBe None
        }
      }
    }
    "provide the snapshot with all rejected transactions and all proposals" in {
      val snapshot = StoredTopologyTransactions(
        Seq[
          (
              CantonTimestamp,
              (GenericSignedTopologyTransaction, Option[CantonTimestamp], Option[String256M]),
          )
        ](
          ts4 -> (dnd_p1seq, None, None),
          // expiring the proposal at ts6. the snapshot itself is inconsistent, but that's not what we're testing here
          ts4 -> (otk_p2_proposal, ts6.some, None),
          // expiring the transaction immediately
          ts5 -> (ptp_fred_p1, ts5.some, Some(String256M.tryCreate("rejection"))),
          ts5 -> (dtc_p2_domain1, ts6.some, None),
          ts6 -> (dtc_p2_domain1_update, None, None),
        ).map { case (from, (tx, until, rejection)) =>
          StoredTopologyTransaction(
            SequencedTime(from),
            EffectiveTime(from),
            until.map(EffectiveTime(_)),
            tx,
            rejection,
          )
        }
      )
      for {
        store <- initializeStore(snapshot)
        service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
        storedTxs <- service.initialSnapshot(p2Id)
      } yield {
        import storedTxs.result
        // all transactions should be valid and not expired
        result.foreach(_.validUntil.foreach(_.value should be < ts6))
        result
          .map(_.transaction) shouldBe Seq(dnd_p1seq, otk_p2_proposal, ptp_fred_p1, dtc_p2_domain1)
        succeed
      }
    }

    // TODO(#13371) explore all edge cases that the logic for determining a topology snapshot for initialization has to deal with

  }
}
