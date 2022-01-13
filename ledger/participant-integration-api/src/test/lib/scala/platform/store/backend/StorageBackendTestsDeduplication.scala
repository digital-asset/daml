// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Time.Timestamp
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsDeduplication
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "DeduplicationStorageBackend"

  import StorageBackendTestValues._

  it should "only allow one upsertDeduplicationEntry to insert a new entry" in {
    val key = "deduplication key"
    val submittedAt = Timestamp.assertFromLong(0L)
    val deduplicateUntil = submittedAt.addMicros(1000L)
    val n = 8

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    val insertedRows = executeParallelSql(
      Vector.fill(n)(c =>
        backend.deduplication.upsertDeduplicationEntry(key, submittedAt, deduplicateUntil)(c)
      )
    )
    val foundDeduplicateUntil = executeSql(backend.deduplication.deduplicatedUntil(key))

    insertedRows.count(_ == 1) shouldBe 1 // One of the calls inserts a new row
    insertedRows.count(_ == 0) shouldBe (n - 1) // All other calls don't write anything
    foundDeduplicateUntil shouldBe deduplicateUntil
  }

  it should "only allow one upsertDeduplicationEntry to update an existing expired entry" in {
    val key = "deduplication key"
    val submittedAt = Timestamp.assertFromLong(0L)
    val deduplicateUntil = submittedAt.addMicros(1000L)
    // Second submission is after the deduplication window of the first one
    val submittedAt2 = Timestamp.assertFromLong(2000L)
    val deduplicateUntil2 = submittedAt2.addMicros(1000L)
    val n = 8

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    val insertedRows =
      executeSql(backend.deduplication.upsertDeduplicationEntry(key, submittedAt, deduplicateUntil))
    val foundDeduplicateUntil = executeSql(backend.deduplication.deduplicatedUntil(key))
    val updatedRows = executeParallelSql(
      Vector.fill(n)(c =>
        backend.deduplication.upsertDeduplicationEntry(
          key,
          submittedAt2,
          deduplicateUntil2,
        )(c)
      )
    )
    val foundDeduplicateUntil2 = executeSql(backend.deduplication.deduplicatedUntil(key))

    insertedRows shouldBe 1 // First call inserts a new row
    updatedRows.count(
      _ == 1
    ) shouldBe 1 // One of the subsequent calls updates the now expired row
    updatedRows.count(_ == 0) shouldBe (n - 1) // All other calls don't write anything
    foundDeduplicateUntil shouldBe deduplicateUntil
    foundDeduplicateUntil2 shouldBe deduplicateUntil2
  }

  it should "not update or insert anything if there is an existing active entry" in {
    val key = "deduplication key"
    val submittedAt = Timestamp.assertFromLong(0L)
    val deduplicateUntil = submittedAt.addMicros(5000L)
    // Second submission is within the deduplication window of the first one
    val submittedAt2 = Timestamp.assertFromLong(1000L)
    val deduplicateUntil2 = submittedAt2.addMicros(5000L)

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    val insertedRows = executeSql(
      backend.deduplication.upsertDeduplicationEntry(key, submittedAt, deduplicateUntil)
    )
    val foundDeduplicateUntil = executeSql(backend.deduplication.deduplicatedUntil(key))
    val updatedRows = executeSql(
      backend.deduplication.upsertDeduplicationEntry(key, submittedAt2, deduplicateUntil2)
    )
    val foundDeduplicateUntil2 = executeSql(backend.deduplication.deduplicatedUntil(key))

    insertedRows shouldBe 1 // First call inserts a new row
    updatedRows shouldBe 0 // Second call doesn't write anything
    foundDeduplicateUntil shouldBe deduplicateUntil
    foundDeduplicateUntil2 shouldBe deduplicateUntil
  }

}
