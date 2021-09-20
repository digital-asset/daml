// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.sql.Connection

import com.daml.platform.store.backend.{DBLockStorageBackend, StorageBackendTestsDBLock}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AsyncFlatSpec

class TestDBLockStorageBackendSpec
    extends AsyncFlatSpec
    with StorageBackendTestsDBLock
    with BeforeAndAfter {

  private var _dbLock: TestDBLockStorageBackend = _

  before {
    _dbLock = new TestDBLockStorageBackend
  }

  override def dbLock: DBLockStorageBackend = _dbLock

  override def getConnection: Connection = new TestConnection
}
