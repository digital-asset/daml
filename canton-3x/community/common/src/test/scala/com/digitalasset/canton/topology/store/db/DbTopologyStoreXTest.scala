// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionX,
  StoredTopologyTransactionsX,
  TopologyStoreXTest,
}

trait DbTopologyStoreXTest extends TopologyStoreXTest with DbTopologyStoreXHelper {
  this: DbTest =>

  "DbTopologyStore" should {
    behave like topologyStore(() => createTopologyStore())

    "properly handle insertion order for large topology snapshots" in {
      val store = createTopologyStore()

      val transactions = (0 to maxItemsInSqlQuery.value * 2 + 3) map { i =>
        val ts = CantonTimestamp.Epoch.plusSeconds(i.toLong)
        // the actual transaction and the consistency is not important for this test
        StoredTopologyTransactionX(SequencedTime(ts), EffectiveTime(ts), None, testData.tx2_OTK)
      }
      val topologySnapshot = StoredTopologyTransactionsX(transactions)

      for {
        _ <- store.bootstrap(topologySnapshot)
        maxTimestamp <- store.maxTimestamp()
      } yield {
        val lastSequenced = transactions.last.sequenced
        val lastEffective = transactions.last.validFrom
        maxTimestamp shouldBe Some((lastSequenced, lastEffective))
      }
    }
  }
}

class TopologyStoreXTestPostgres extends DbTopologyStoreXTest with PostgresTest

class TopologyStoreXTestH2 extends DbTopologyStoreXTest with H2Test
