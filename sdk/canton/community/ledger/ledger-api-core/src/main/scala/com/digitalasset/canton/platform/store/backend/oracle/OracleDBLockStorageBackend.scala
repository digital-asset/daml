// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import anorm.SqlParser.get
import anorm.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.DBLockStorageBackend

import java.sql.Connection

object OracleDBLockStorageBackend extends DBLockStorageBackend {
  override def tryAcquire(
      lockId: DBLockStorageBackend.LockId,
      lockMode: DBLockStorageBackend.LockMode,
  )(connection: Connection): Option[DBLockStorageBackend.Lock] = {
    val oracleLockMode = lockMode match {
      case DBLockStorageBackend.LockMode.Exclusive => "6" // "DBMS_LOCK.x_mode"
      case DBLockStorageBackend.LockMode.Shared => "4" // "DBMS_LOCK.s_mode"
    }
    SQL"""
          SELECT DBMS_LOCK.REQUEST(
            id => ${oracleIntLockId(lockId)},
            lockmode => #$oracleLockMode,
            timeout => 0
          ) FROM DUAL"""
      .as(get[Int](1).single)(connection) match {
      case 0 => Some(DBLockStorageBackend.Lock(lockId, lockMode))
      case 1 => None
      case 2 => throw new Exception("DBMS_LOCK.REQUEST Error 2: Acquiring lock caused a deadlock!")
      case 3 => throw new Exception("DBMS_LOCK.REQUEST Error 3: Parameter error as acquiring lock")
      case 4 => Some(DBLockStorageBackend.Lock(lockId, lockMode))
      case 5 =>
        throw new Exception("DBMS_LOCK.REQUEST Error 5: Illegal lock handle as acquiring lock")
      case unknown => throw new Exception(s"Invalid result from DBMS_LOCK.REQUEST: $unknown")
    }
  }

  override def release(lock: DBLockStorageBackend.Lock)(connection: Connection): Boolean = {
    SQL"""
          SELECT DBMS_LOCK.RELEASE(
            id => ${oracleIntLockId(lock.lockId)}
          ) FROM DUAL"""
      .as(get[Int](1).single)(connection) match {
      case 0 => true
      case 3 => throw new Exception("DBMS_LOCK.RELEASE Error 3: Parameter error as releasing lock")
      case 4 => false
      case 5 =>
        throw new Exception("DBMS_LOCK.RELEASE Error 5: Illegal lock handle as releasing lock")
      case unknown => throw new Exception(s"Invalid result from DBMS_LOCK.RELEASE: $unknown")
    }
  }

  final case class OracleLockId(id: Int) extends DBLockStorageBackend.LockId {
    // respecting Oracle limitations: https://docs.oracle.com/cd/B19306_01/appdev.102/b14258/d_lock.htm#ARPLS021
    assert(id >= 0, s"Lock id $id is too small for Oracle")
    assert(id <= 1073741823, s"Lock id $id is too large for Oracle")
  }

  private def oracleIntLockId(lockId: DBLockStorageBackend.LockId): Int =
    lockId match {
      case OracleLockId(id) => id
      case unknown =>
        throw new Exception(
          s"LockId $unknown not supported. Probable cause: LockId was created by a different StorageBackend"
        )
    }

  override def lock(id: Int): DBLockStorageBackend.LockId = OracleLockId(id)

  override def dbLockSupported: Boolean = true
}
