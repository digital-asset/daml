// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.option.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DownloadTopologyStateForInitializationServiceTest
    extends AsyncWordSpec
    with TopologyStoreXTestBase {

  protected def createTopologyStore(): TopologyStoreX[DomainStore]

  val testData = new TopologyStoreXTestData(loggerFactory, executionContext)
  import testData.*

  val bootstrapTransactions = StoredTopologyTransactionsX(
    Seq[
      (CantonTimestamp, (GenericSignedTopologyTransactionX, Option[CantonTimestamp]))
    ](
      ts4 -> (tx4_DND, None),
      ts5 -> (tx5_PTP, None),
      ts5 -> (tx5_DTC, None),
      ts6 -> (tx6_MDS, None),
      ts8 -> (tx8_SDS, None),
    ).map { case (from, (tx, until)) =>
      StoredTopologyTransactionX(
        SequencedTime(from),
        EffectiveTime(from),
        until.map(EffectiveTime(_)),
        tx,
      )
    }
  )

  val bootstrapTransactionsWithUpdates = StoredTopologyTransactionsX(
    Seq[
      (CantonTimestamp, (GenericSignedTopologyTransactionX, Option[CantonTimestamp]))
    ](
      ts4 -> (tx4_DND, None),
      ts5 -> (tx5_PTP, None),
      ts5 -> (tx5_DTC, ts6.some),
      ts6 -> (tx6_DTC_Update, None),
      ts6 -> (tx6_MDS, ts7.some),
      ts7 -> (tx7_MDS_Update, None),
      ts8 -> (tx8_SDS, None),
    ).map { case (from, (tx, until)) =>
      StoredTopologyTransactionX(
        SequencedTime(from),
        EffectiveTime(from),
        until.map(EffectiveTime(_)),
        tx,
      )
    }
  )

  private def initializeStore(
      storedTransactions: GenericStoredTopologyTransactionsX
  ): Future[TopologyStoreX[DomainStore]] = {
    val store = createTopologyStore()
    store.bootstrap(storedTransactions).map(_ => store)
  }

  "DownloadTopologyStateForInitializationService" should {
    "return a valid topology state" when {
      "there's only one DomainTrustCertificateX" in {
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
    }
    "the first DomainTrustCertificateX is superseded by another one" in {
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

    "there's only one MediatorDomainStateX" in {
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

    "the first MediatorDomainStateX is superseded by another one" in {
      for {
        store <- initializeStore(bootstrapTransactionsWithUpdates)
        service = new StoreBasedTopologyStateForInitializationService(store, loggerFactory)
        storedTxs <- service.initialSnapshot(mediatorId1)
      } yield {
        import storedTxs.result
        // all transactions should be valid and not expired
        result.foreach(_.validUntil.foreach(_.value should be < ts7))
        result.map(_.transaction) shouldBe Seq(tx4_DND, tx5_PTP, tx5_DTC, tx6_DTC_Update, tx6_MDS)
        result.last.validUntil shouldBe None
      }
    }

    // TODO(#13371) explore all edge cases that the logic for determining a topology snapshot for initialization has to deal with

  }
}
