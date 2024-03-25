// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.client.DbStoreBasedTopologySnapshotTest
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  SignedTopologyTransactions,
  TopologyStoreId,
  TopologyStoreTest,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.{
  DomainParametersChange,
  SignedTopologyTransaction,
  TopologyChangeOp,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  TestingIdentityFactory,
  TestingOwnerWithKeys,
}

import scala.concurrent.Future

trait DbTopologyStoreTest extends TopologyStoreTest {

  this: DbTest =>

  val pureCryptoApi: CryptoPureApi = TestingIdentityFactory.pureCrypto()

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table party_metadata",
        sqlu"truncate table topology_transactions",
      ),
      operationName = s"${this.getClass}: Truncate tables party_metadata and topology_transactions",
    )
  }

  "DbPartyMetadataStore" should {
    behave like partyMetadataStore(() => new DbPartyMetadataStore(storage, timeouts, loggerFactory))
  }

  private def createTopologyStore(
      storeId: TopologyStoreId = TopologyStoreId.AuthorizedStore
  ): DbTopologyStore[TopologyStoreId] =
    new DbTopologyStore(
      storage,
      storeId,
      timeouts,
      loggerFactory,
      futureSupervisor,
      maxItemsInSqlQuery = PositiveInt.one,
    )

  "DbTopologyStore" should {
    behave like topologyStore(() => createTopologyStore())
  }

  // TODO(#15208) remove once we've deprecated this in 3.0
  "backfill legacy topology transactions sequencing times" in {
    val store = createTopologyStore(DomainStore(DefaultTestIdentities.domainId))
    val owner = new TestingOwnerWithKeys(
      DefaultTestIdentities.domainManager,
      loggerFactory,
      parallelExecutionContext,
    )
    import owner.TestingTransactions.*
    def ts(ms: Long) = CantonTimestamp.Epoch.plusMillis(ms)
    def validated(
        txs: SignedTopologyTransaction[TopologyChangeOp]*
    ): List[ValidatedTopologyTransaction] = {
      txs.toList.map(ValidatedTopologyTransaction(_, None))
    }
    def append(
        sequenced: CantonTimestamp,
        effective: CantonTimestamp,
        txs: List[ValidatedTopologyTransaction],
    ): Future[Unit] = {
      val appendF = store.append(SequencedTime(sequenced), EffectiveTime(effective), txs)
      val (deactivate, positive) =
        SignedTopologyTransactions(txs.map(_.transaction)).splitForStateUpdate
      val updateF =
        store.updateState(SequencedTime(sequenced), EffectiveTime(effective), deactivate, positive)
      for {
        _ <- appendF
        _ <- updateF
      } yield ()
    }
    val defaultDomainParameters = TestDomainParameters.defaultDynamic
    val dpc1 = owner.mkDmGov(
      DomainParametersChange(
        DefaultTestIdentities.domainId,
        defaultDomainParameters
          .tryUpdate(topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(10)),
      ),
      namespaceKey,
    )
    val dpc2 = owner.mkDmGov(
      DomainParametersChange(
        DefaultTestIdentities.domainId,
        defaultDomainParameters
          .tryUpdate(topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(100)),
      ),
      namespaceKey,
    )
    for {
      _ <- append(ts(0), ts(0), validated(ns1k1))
      _ <- append(ts(10), ts(10), validated(id1k1, dpc1))
      _ <- append(ts(50), ts(60), validated(ns1k2))
      _ <- append(ts(100), ts(110), validated(dpc2))
      /// will be projected by dpc1
      _ <- append(ts(100).immediateSuccessor, ts(110).immediateSuccessor, validated(okm1))
      _ <- append(ts(110), ts(120), validated(ps1))
      // will be projected by dpc2
      _ <- append(ts(110).immediateSuccessor, ts(210).immediateSuccessor, validated(ps2))
      _ <- {
        // purge sequenced
        import storage.api.*
        storage.update_(sqlu"UPDATE topology_transactions SET sequenced = NULL", "testing")
      }
      // load transactions (triggers recomputation)
      txs <- store.headTransactions
    } yield {
      def grabTs(tx: SignedTopologyTransaction[TopologyChangeOp]): CantonTimestamp = {
        txs.result
          .find(_.transaction == tx)
          .valueOrFail(s"can not find transaction ${tx}")
          .sequenced
          .value
      }
      grabTs(ns1k1) shouldBe ts(0)
      grabTs(id1k1) shouldBe ts(10)
      grabTs(ns1k2) shouldBe ts(50)
      grabTs(okm1) shouldBe ts(100).immediateSuccessor
      grabTs(ps1) shouldBe ts(110)
      grabTs(ps2) shouldBe ts(110).immediateSuccessor
    }

  }

  "Storage implicits" should {
    "should be stack safe" in {
      val tmp = storage
      import DbStorage.Implicits.BuilderChain.*
      import tmp.api.*
      (1 to 100000).map(_ => sql" 1 == 1").toList.intercalate(sql" AND ").discard
      assert(true)
    }
  }

}

class TopologyStoreTestH2
    extends DbTopologyStoreTest
    with DbStoreBasedTopologySnapshotTest
    with H2Test

// merging both tests into a single runner, as both tests will write to topology_transactions, creating conflicts
class TopologyStoreTestPostgres
    extends DbTopologyStoreTest
    with DbStoreBasedTopologySnapshotTest
    with PostgresTest
