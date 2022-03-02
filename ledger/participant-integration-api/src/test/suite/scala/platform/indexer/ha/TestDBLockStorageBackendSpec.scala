// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.sql.Connection

import com.daml.platform.store.backend.{DBLockStorageBackend, StorageBackendTestsDBLock}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

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
