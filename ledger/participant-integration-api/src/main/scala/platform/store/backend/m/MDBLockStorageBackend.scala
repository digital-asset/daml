// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.platform.store.backend.DBLockStorageBackend

object MDBLockStorageBackend extends DBLockStorageBackend {
  override def tryAcquire(
      lockId: DBLockStorageBackend.LockId,
      lockMode: DBLockStorageBackend.LockMode,
  )(connection: Connection): Option[DBLockStorageBackend.Lock] =
    throw new UnsupportedOperationException

  override def release(lock: DBLockStorageBackend.Lock)(connection: Connection): Boolean =
    throw new UnsupportedOperationException

  override def lock(id: Int): DBLockStorageBackend.LockId = throw new UnsupportedOperationException

  override def dbLockSupported: Boolean = false
}
