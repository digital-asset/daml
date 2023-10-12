// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import com.digitalasset.canton.platform.store.backend.{
  DBLockStorageBackend,
  StorageBackendTestsDBLock,
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Connection

class TestDBLockStorageBackendSpec
    extends AnyFlatSpec
    with StorageBackendTestsDBLock
    with BeforeAndAfter {

  private var _dbLock: TestDBLockStorageBackend = _

  before {
    _dbLock = new TestDBLockStorageBackend
  }

  override def dbLock: DBLockStorageBackend = _dbLock

  override def lockIdSeed: Int = 1000 // Seeding not needed for this test

  override def getConnection: Connection = new TestConnection
}
