// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.store.MediatorDeduplicationStore.DeduplicationData
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.DefaultTestIdentities
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.Future

trait MediatorDeduplicationStoreTest extends AsyncWordSpec with BaseTest { this: HasCloseContext =>

  def mkStore(
      firstEventTs: CantonTimestamp = CantonTimestamp.MaxValue
  ): MediatorDeduplicationStore

  lazy val uuids: Seq[UUID] = List(
    "51f3ffff-9248-453b-807b-91dd7ed23298",
    "c0175d4a-def2-481e-a979-ae9d335b5d35",
    "b9f66e2a-4867-465e-b51f-c727f2d0a18f",
  ).map(UUID.fromString)

  "The MediatorDeduplicationStore" should {
    "store UUIDs" in {
      val store = mkStore()
      val data1 = DeduplicationData(
        uuids(0),
        CantonTimestamp.Epoch,
        CantonTimestamp.ofEpochSecond(10),
      )
      val data2 = DeduplicationData(
        uuids(1),
        CantonTimestamp.ofEpochSecond(1),
        CantonTimestamp.ofEpochSecond(11),
      )
      val data3 = DeduplicationData(
        uuids(2),
        CantonTimestamp.ofEpochSecond(2),
        CantonTimestamp.ofEpochSecond(12),
      )

      store.allData() shouldBe Set.empty

      for {
        _ <- store.store(data1)
        _ <- store.store(data2)
        _ <- store.store(data3)
      } yield store.allData() shouldBe Set(data1, data2, data3)
    }

    "store UUIDs with unusual request ids and expiration times" in {
      val store = mkStore()
      val data1 = DeduplicationData(
        uuids(0),
        CantonTimestamp.Epoch,
        CantonTimestamp.ofEpochSecond(10),
      )
      // same request id
      val data2 = DeduplicationData(
        uuids(1),
        CantonTimestamp.Epoch,
        CantonTimestamp.ofEpochSecond(10),
      )
      // expiration time in the past
      val data3 = DeduplicationData(
        uuids(2),
        CantonTimestamp.Epoch,
        CantonTimestamp.ofEpochSecond(-10),
      )
      // same uuid as previous data, storing before expiration
      val data4 = DeduplicationData(
        uuids(0),
        CantonTimestamp.ofEpochSecond(1),
        CantonTimestamp.ofEpochSecond(11),
      )
      // same uuid as previous data, storing after expiration
      val data5 = DeduplicationData(
        uuids(0),
        CantonTimestamp.ofEpochSecond(12),
        CantonTimestamp.ofEpochSecond(13),
      )

      for {
        _ <- store.store(data1)
        _ <- store.store(data1) // store data1 again
        _ <- store.store(data2)
        _ <- store.store(data3)
        _ <- store.store(data4)
        _ <- store.store(data5)
      } yield store.allData() shouldBe Set(data1, data2, data3, data4, data5)
    }

    "store extreme values" in {
      val store = mkStore()
      val minData =
        DeduplicationData(uuids(0), CantonTimestamp.MinValue, CantonTimestamp.MinValue)
      val maxData =
        DeduplicationData(uuids(1), CantonTimestamp.MaxValue, CantonTimestamp.MaxValue)

      for {
        _ <- store.store(minData)
        _ <- store.store(maxData)
      } yield store.allData() shouldBe Set(minData, maxData)
    }

    "retrieve existing UUIDs taking expiration into account" in {
      val store = mkStore()
      val data1 = DeduplicationData(
        uuids(0),
        CantonTimestamp.Epoch,
        CantonTimestamp.ofEpochSecond(10),
      )
      val data2 = DeduplicationData(
        uuids(1),
        CantonTimestamp.ofEpochSecond(1),
        CantonTimestamp.ofEpochSecond(11),
      )

      for {
        _ <- store.store(data1)
        _ <- store.store(data2)
      } yield {
        store.findUuid(uuids(0), CantonTimestamp.Epoch) shouldBe Set(data1)
        store.findUuid(uuids(0), CantonTimestamp.ofEpochSecond(-1)) shouldBe Set(data1)
        store.findUuid(uuids(0), CantonTimestamp.ofEpochSecond(10)) shouldBe Set(data1)
        store.findUuid(uuids(0), CantonTimestamp.ofEpochSecond(11)) shouldBe empty

        store.findUuid(uuids(1), CantonTimestamp.Epoch) shouldBe Set(data2)
        store.findUuid(uuids(1), CantonTimestamp.ofEpochSecond(11)) shouldBe Set(data2)
        store.findUuid(uuids(1), CantonTimestamp.ofEpochSecond(12)) shouldBe empty
      }
    }

    "retrieve with extreme values" in {
      val store = mkStore()
      val data = DeduplicationData(
        uuids(0),
        CantonTimestamp.Epoch,
        CantonTimestamp.MaxValue,
      )

      for {
        _ <- store.store(data)
      } yield {
        store.findUuid(uuids(0), CantonTimestamp.Epoch) shouldBe Set(data)
        store.findUuid(uuids(0), CantonTimestamp.MinValue) shouldBe Set(data)
        store.findUuid(uuids(0), CantonTimestamp.MaxValue) shouldBe Set(data)
      }
    }

    "not retrieve non-existing UUIDs" in {
      val store = mkStore()
      store.findUuid(uuids(0), CantonTimestamp.Epoch) shouldBe empty
    }

    "prune stored UUIDs" in {
      val store = mkStore()
      val data1 = DeduplicationData(
        uuids(0),
        CantonTimestamp.Epoch,
        CantonTimestamp.ofEpochSecond(10),
      )
      val data2 = DeduplicationData(
        uuids(0),
        CantonTimestamp.ofEpochSecond(1),
        CantonTimestamp.ofEpochSecond(11),
      )
      val data3 = DeduplicationData(
        uuids(2),
        CantonTimestamp.ofEpochSecond(2),
        CantonTimestamp.ofEpochSecond(12),
      )

      for {
        _ <- store.store(data1)
        _ <- store.store(data2)
        _ <- store.store(data3)

        _ <- store.prune(CantonTimestamp.ofEpochSecond(10))
        _ = store.allData() shouldBe Set(data2, data3)

        _ <- store.prune(CantonTimestamp.MinValue)
        _ = store.allData() shouldBe Set(data2, data3)

        _ <- store.prune(CantonTimestamp.ofEpochSecond(11))
        _ = store.allData() shouldBe Set(data3)

        _ <- store.prune(CantonTimestamp.MaxValue)
      } yield store.allData() shouldBe empty
    }
  }
}

class MediatorDeduplicationStoreTestInMemory
    extends MediatorDeduplicationStoreTest
    with FlagCloseable
    with HasCloseContext {
  override def mkStore(firstEventTs: CantonTimestamp): MediatorDeduplicationStore = {
    val store = new InMemoryMediatorDeduplicationStore(loggerFactory, timeouts)
    store.initialize(firstEventTs).futureValue
    store
  }
}

trait DbMediatorDeduplicationStoreTest extends MediatorDeduplicationStoreTest with DbTest {

  override def mkStore(
      firstEventTs: CantonTimestamp
  ): DbMediatorDeduplicationStore = {
    val store = new DbMediatorDeduplicationStore(
      DefaultTestIdentities.mediator,
      storage,
      timeouts,
      BatchAggregatorConfig.defaultsForTesting,
      loggerFactory,
    )
    store.initialize(firstEventTs).futureValue
    store
  }

  override protected def cleanDb(storage: DbStorage): Future[_] = {
    import storage.api.*
    storage.update_(sqlu"truncate table mediator_deduplication_store", functionFullName)
  }

  "The DbMediatorDeduplicationStore" should {
    "cleanup and initialize caches" in {
      val store = mkStore()

      val data1 = DeduplicationData(
        uuids(0),
        CantonTimestamp.Epoch,
        CantonTimestamp.ofEpochSecond(10),
      )
      val data2 = DeduplicationData(
        uuids(1),
        CantonTimestamp.ofEpochSecond(1),
        CantonTimestamp.ofEpochSecond(11),
      )

      for {
        _ <- store.store(data1)
        _ <- store.store(data2)
      } yield {
        val store2 = mkStore(CantonTimestamp.ofEpochSecond(2))
        store2.allData() shouldBe Set(data1, data2)

        val store3 = mkStore(CantonTimestamp.ofEpochSecond(1))
        store3.allData() shouldBe Set(data1)

        val store4 = mkStore(CantonTimestamp.Epoch)
        store4.allData() shouldBe empty
      }
    }
  }
}

class MediatorDeduplicationStoreTestH2 extends DbMediatorDeduplicationStoreTest with H2Test

class MediatorDeduplicationStoreTestPostgres
    extends DbMediatorDeduplicationStoreTest
    with PostgresTest
