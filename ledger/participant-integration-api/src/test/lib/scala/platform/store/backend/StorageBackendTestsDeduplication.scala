// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Time.Timestamp
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

private[backend] trait StorageBackendTestsDeduplication
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "DeduplicationStorageBackend"

  import StorageBackendTestValues._

  it should "only allow one upsertDeduplicationEntry to insert a new entry" in {
    val key = "deduplication key"
    val submittedAt = Timestamp.assertFromLong(0L)
    val deduplicateUntil = Timestamp.assertFromLong(1000L)
    val n = 8

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      insertedRows <- Future.sequence(
        Vector.fill(n)(
          executeSql(backend.upsertDeduplicationEntry(key, submittedAt, deduplicateUntil))
        )
      )
      foundDeduplicateUntil <- executeSql(backend.deduplicatedUntil(key))
    } yield {
      insertedRows.count(_ == 1) shouldBe 1 // One of the calls inserts a new row
      insertedRows.count(_ == 0) shouldBe (n - 1) // All other calls don't write anything
      foundDeduplicateUntil shouldBe deduplicateUntil
      succeed
    }
  }

  it should "only allow one upsertDeduplicationEntry to update an existing expired entry" in {
    val key = "deduplication key"
    val submittedAt = Timestamp.assertFromLong(0L)
    val deduplicateUntil = Timestamp.assertFromLong(1000L)
    val submittedAt2 = Timestamp.assertFromLong(2000L)
    val deduplicateUntil2 = Timestamp.assertFromLong(3000L)
    val n = 8

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      insertedRows <- executeSql(
        backend.upsertDeduplicationEntry(key, submittedAt, deduplicateUntil)
      )
      foundDeduplicateUntil <- executeSql(backend.deduplicatedUntil(key))
      updatedRows <- Future.sequence(
        Vector.fill(n)(
          executeSql(backend.upsertDeduplicationEntry(key, submittedAt2, deduplicateUntil2))
        )
      )
      foundDeduplicateUntil2 <- executeSql(backend.deduplicatedUntil(key))
    } yield {
      insertedRows shouldBe 1 // First call inserts a new row
      updatedRows.count(
        _ == 1
      ) shouldBe 1 // One of the subsequent calls updates the now expired row
      updatedRows.count(_ == 0) shouldBe (n - 1) // All other calls don't write anything
      foundDeduplicateUntil shouldBe deduplicateUntil
      foundDeduplicateUntil2 shouldBe deduplicateUntil2
      succeed
    }
  }

  it should "not update or insert anything if there is an existing active entry" in {
    val key = "deduplication key"
    val submittedAt = Timestamp.assertFromLong(0L)
    val deduplicateUntil = Timestamp.assertFromLong(5000L)
    val submittedAt2 = Timestamp.assertFromLong(1000L)
    val deduplicateUntil2 = Timestamp.assertFromLong(6000L)

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      insertedRows <- executeSql(
        backend.upsertDeduplicationEntry(key, submittedAt, deduplicateUntil)
      )
      foundDeduplicateUntil <- executeSql(backend.deduplicatedUntil(key))
      updatedRows <- executeSql(
        backend.upsertDeduplicationEntry(key, submittedAt2, deduplicateUntil2)
      )
      foundDeduplicateUntil2 <- executeSql(backend.deduplicatedUntil(key))
    } yield {
      insertedRows shouldBe 1 // First call inserts a new row
      updatedRows shouldBe 0 // Second call doesn't write anything
      foundDeduplicateUntil shouldBe deduplicateUntil
      foundDeduplicateUntil2 shouldBe deduplicateUntil
      succeed
    }
  }

}
