// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsDebug extends Matchers with StorageBackendSpec {
  this: AsyncFlatSpec =>

  import StorageBackendTestValues._

  behavior of "DebugStorageBackend"

  it should "find duplicate event ids" in {
    val updates = Vector(
      dtoCreate(offset(7), 7L, "#7"),
      dtoCreate(offset(7), 7L, "#7"), // duplicate id
    )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(updates, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(7), 7L)))
      failure <- executeSql(backend.verifyIntegrity()).failed
    } yield {
      // Error message should contain the duplicate event sequential id
      failure.getMessage should include("7")
    }
  }

  it should "find non-consecutive event ids" in {
    val updates = Vector(
      dtoCreate(offset(1), 1L, "#1"),
      dtoCreate(offset(3), 3L, "#3"), // non-consecutive id
    )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(updates, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(3), 3L)))
      failure <- executeSql(backend.verifyIntegrity()).failed
    } yield {
      failure.getMessage should include("consecutive")
    }
  }

  it should "not find errors beyond the ledger end" in {
    val updates = Vector(
      dtoCreate(offset(1), 1L, "#1"),
      dtoCreate(offset(2), 2L, "#2"),
      dtoCreate(offset(7), 7L, "#7"), // beyond the ledger end
      dtoCreate(offset(7), 7L, "#7"), // duplicate id (beyond ledger end)
      dtoCreate(offset(9), 9L, "#9"), // non-consecutive id (beyond ledger end)
    )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(updates, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(2), 2L)))
      _ <- executeSql(backend.verifyIntegrity())
    } yield {
      succeed
    }
  }
}
