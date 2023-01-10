// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsIntegrity extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues._

  behavior of "IntegrityStorageBackend"

  it should "find duplicate event ids" in {
    val updates = Vector(
      dtoCreate(offset(7), 7L, hashCid("#7")),
      dtoCreate(offset(7), 7L, hashCid("#7")), // duplicate id
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(7), 7L))
    val failure = intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))

    // Error message should contain the duplicate event sequential id
    failure.getMessage should include("7")
  }

  it should "find non-consecutive event ids" in {
    val updates = Vector(
      dtoCreate(offset(1), 1L, hashCid("#1")),
      dtoCreate(offset(3), 3L, hashCid("#3")), // non-consecutive id
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(3), 3L))
    val failure = intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))

    failure.getMessage should include("consecutive")

  }

  it should "not find errors beyond the ledger end" in {
    val updates = Vector(
      dtoCreate(offset(1), 1L, hashCid("#1")),
      dtoCreate(offset(2), 2L, hashCid("#2")),
      dtoCreate(offset(7), 7L, hashCid("#7")), // beyond the ledger end
      dtoCreate(offset(7), 7L, hashCid("#7")), // duplicate id (beyond ledger end)
      dtoCreate(offset(9), 9L, hashCid("#9")), // non-consecutive id (beyond ledger end)
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    executeSql(backend.integrity.verifyIntegrity())

    // Succeeds if verifyIntegrity() doesn't throw
    succeed
  }
}
