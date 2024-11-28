// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.option.*
import com.digitalasset.canton.FailOnShutdown
import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import org.scalatest.wordspec.AsyncWordSpec

trait DownloadTopologyStateForInitializationServiceTest
    extends AsyncWordSpec
    with TopologyStoreTestBase
    with FailOnShutdown {

  protected def createTopologyStore(domainId: DomainId): TopologyStore[DomainStore]

  val testData = new TopologyStoreTestData(loggerFactory, executionContext)
  import testData.*

  val bootstrapTransactions = StoredTopologyTransactions(
    Seq[
      (CantonTimestamp, (GenericSignedTopologyTransaction, Option[CantonTimestamp]))
    ](
      ts4 -> (tx4_DND, None),
      ts5 -> (tx5_PTP, None),
      ts5 -> (tx5_DTC, None),
      ts6 -> (tx6_MDS, None),
      ts8 -> (tx8_SDS, None),
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
      ts4 -> (tx4_DND, None),
      ts5 -> (tx5_PTP, None),
      ts5 -> (tx5_DTC, ts6.some),
      ts6 -> (tx6_DTC_Update, None),
      ts6 -> (tx6_MDS, ts7.some),
      ts7 -> (tx7_MDS_Update, None),
      ts8 -> (tx8_SDS, None),
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
    val store = createTopologyStore(domainId1)
    store.bootstrap(storedTransactions).map(_ => store)
  }

  "DownloadTopologyStateForInitializationService" should {
    "return a valid topology state" when {
      "there's only one DomainTrustCertificate" in {
        for {
          store <- initializeStore(bootstrapTransactions)
          service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
          storedTxs <- service.initialSnapshot(tx5_DTC.mapping.participantId)
        } yield {
          import storedTxs.result
          // all transactions should be valid and not expired
          result.foreach(_.validUntil shouldBe empty)
          result.map(_.transaction) shouldBe Seq(tx4_DND, tx5_PTP, tx5_DTC)
        }
      }
      "the first DomainTrustCertificate is superseded by another one" in {
        for {
          store <- initializeStore(bootstrapTransactionsWithUpdates)
          service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
          storedTxs <- service.initialSnapshot(tx5_DTC.mapping.participantId)
        } yield {
          import storedTxs.result
          // all transactions should be valid and not expired
          result.foreach(_.validUntil shouldBe empty)
          result.map(_.transaction) shouldBe Seq(tx4_DND, tx5_PTP, tx5_DTC)
          result.last.validUntil shouldBe None
        }
      }

      "there's only one MediatorDomainState" in {
        for {
          store <- initializeStore(bootstrapTransactions)
          service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
          storedTxs <- service.initialSnapshot(mediatorId1)
        } yield {
          import storedTxs.result
          // all transactions should be valid and not expired
          result.foreach(_.validUntil shouldBe empty)
          result.map(_.transaction) shouldBe Seq(tx4_DND, tx5_PTP, tx5_DTC, tx6_MDS)
        }
      }

      "the first MediatorDomainState is superseded by another one" in {
        for {
          store <- initializeStore(bootstrapTransactionsWithUpdates)
          service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
          storedTxs <- service.initialSnapshot(mediatorId1)
        } yield {
          import storedTxs.result
          // all transactions should be valid and not validUntil capped at ts6
          result.foreach(_.validUntil.foreach(_.value should be <= ts6))
          result.map(_.transaction) shouldBe Seq(tx4_DND, tx5_PTP, tx5_DTC, tx6_DTC_Update, tx6_MDS)
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
          ts4 -> (tx4_DND, None, None),
          // expiring the proposal at ts6. the snapshot itself is inconsistent, but that's not what we're testing here
          ts4 -> (tx4_OTK_Proposal, ts6.some, None),
          // expiring the transaction immediately
          ts5 -> (tx5_PTP, ts5.some, Some(String256M.tryCreate("rejection"))),
          ts5 -> (tx5_DTC, ts6.some, None),
          ts6 -> (tx6_DTC_Update, None, None),
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
        storedTxs <- service.initialSnapshot(participantId2)
      } yield {
        import storedTxs.result
        // all transactions should be valid and not expired
        result.foreach(_.validUntil.foreach(_.value should be < ts6))
        result.map(_.transaction) shouldBe Seq(tx4_DND, tx4_OTK_Proposal, tx5_PTP, tx5_DTC)
        succeed
      }
    }

    // TODO(#13371) explore all edge cases that the logic for determining a topology snapshot for initialization has to deal with

  }
}
