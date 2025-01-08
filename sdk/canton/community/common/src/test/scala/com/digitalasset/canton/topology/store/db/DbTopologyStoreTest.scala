// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SynchronizerCryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  InitialTopologySnapshotValidator,
  SequencedTime,
}
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStoreTest,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.transaction.{HostingParticipant, PartyToParticipant}

trait DbTopologyStoreTest extends TopologyStoreTest with DbTopologyStoreHelper {
  this: DbTest =>

  "DbPartyMetadataStore" should {
    behave like partyMetadataStore(() => new DbPartyMetadataStore(storage, timeouts, loggerFactory))
  }

  "DbTopologyStore" should {
    behave like topologyStore(createTopologyStore)

    "properly handle insertion order for large topology snapshots" in {
      val store = createTopologyStore(testData.domain1_p1p2_synchronizerId)

      val domainSetup = Seq(
        0 -> testData.nsd_p1,
        0 -> testData.nsd_p2,
        0 -> testData.dnd_p1p2,
        0 -> testData.dop_domain1,
        0 -> testData.otk_p1,
        0 -> testData.dtc_p1_domain1,
      )

      val partyAllocations = (1 to maxItemsInSqlQuery.value * 2 + 3) map { i =>
        i -> testData.makeSignedTx(
          PartyToParticipant.tryCreate(
            PartyId.tryCreate(s"party$i", testData.p1Namespace),
            threshold = PositiveInt.one,
            participants = Seq(HostingParticipant(testData.p1Id, Submission)),
          )
        )(testData.p1Key)
      }

      val transactions = (domainSetup ++ partyAllocations).map { case (timeOffset, tx) =>
        val ts = CantonTimestamp.Epoch.plusSeconds(timeOffset.toLong)
        // the actual transaction and the consistency is not important for this test
        StoredTopologyTransaction(
          SequencedTime(ts),
          EffectiveTime(ts),
          None,
          tx,
          None,
        )
      }
      val topologySnapshot = StoredTopologyTransactions(transactions)

      for {
        _ <- new InitialTopologySnapshotValidator(
          testData.domain1_p1p2_synchronizerId,
          new SynchronizerCryptoPureApi(
            defaultStaticSynchronizerParameters,
            testData.factory.cryptoApi.crypto.pureCrypto,
          ),
          store,
          timeouts,
          loggerFactory,
        ).validateAndApplyInitialTopologySnapshot(topologySnapshot)
          .valueOrFail("topology bootstrap")

        maxTimestamp <- store
          .maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true)
      } yield {
        val lastSequenced = transactions.last.sequenced
        val lastEffective = transactions.last.validFrom
        maxTimestamp shouldBe Some((lastSequenced, lastEffective))
      }
    }
  }
}

class TopologyStoreTestPostgres extends DbTopologyStoreTest with PostgresTest

class TopologyStoreTestH2 extends DbTopologyStoreTest with H2Test
