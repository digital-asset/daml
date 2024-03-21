// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection

import anorm.SqlStringInterpolation
import anorm.SqlParser.get
import com.daml.platform.store.backend.DBLockStorageBackend

object PostgresDBLockStorageBackend extends DBLockStorageBackend {

  override def tryAcquire(
      lockId: DBLockStorageBackend.LockId,
      lockMode: DBLockStorageBackend.LockMode,
  )(connection: Connection): Option[DBLockStorageBackend.Lock] = {
    val lockFunction = lockMode match {
      case DBLockStorageBackend.LockMode.Exclusive => "pg_try_advisory_lock"
      case DBLockStorageBackend.LockMode.Shared => "pg_try_advisory_lock_shared"
    }
    SQL"SELECT #$lockFunction(${pgBigintLockId(lockId)})"
      .as(get[Boolean](1).single)(connection) match {
      case true => Some(DBLockStorageBackend.Lock(lockId, lockMode))
      case false => None
    }
  }

  override def release(lock: DBLockStorageBackend.Lock)(connection: Connection): Boolean = {
    val lockFunction = lock.lockMode match {
      case DBLockStorageBackend.LockMode.Exclusive => "pg_advisory_unlock"
      case DBLockStorageBackend.LockMode.Shared => "pg_advisory_unlock_shared"
    }
    SQL"SELECT #$lockFunction(${pgBigintLockId(lock.lockId)})"
      .as(get[Boolean](1).single)(connection)
  }

  case class PGLockId(id: Long) extends DBLockStorageBackend.LockId

  private def pgBigintLockId(lockId: DBLockStorageBackend.LockId): Long =
    lockId match {
      case PGLockId(id) => id
      case unknown =>
        throw new Exception(
          s"LockId $unknown not supported. Probable cause: LockId was created by a different StorageBackend"
        )
    }

  override def lock(id: Int): DBLockStorageBackend.LockId = PGLockId(id.toLong)

  override def dbLockSupported: Boolean = true
}
