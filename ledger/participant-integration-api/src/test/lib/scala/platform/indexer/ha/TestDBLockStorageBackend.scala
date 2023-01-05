// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.sql.{
  Blob,
  CallableStatement,
  Clob,
  Connection,
  DatabaseMetaData,
  NClob,
  PreparedStatement,
  SQLWarning,
  SQLXML,
  Savepoint,
  Statement,
  Struct,
}
import java.util.Properties
import java.util.concurrent.Executor
import java.{sql, util}

import com.daml.platform.store.backend.DBLockStorageBackend

class TestDBLockStorageBackend extends DBLockStorageBackend {

  private var locks: Map[DBLockStorageBackend.Lock, Set[Connection]] = Map.empty

  override def tryAcquire(
      lockId: DBLockStorageBackend.LockId,
      lockMode: DBLockStorageBackend.LockMode,
  )(connection: Connection): Option[DBLockStorageBackend.Lock] = synchronized {
    if (connection.isClosed) throw new Exception("trying to acquire on a closed connection")
    if (!lockId.isInstanceOf[TestLockId]) throw new Exception("foreign lockId")
    removeClosedConnectionRelatedLocks()
    val lock = DBLockStorageBackend.Lock(lockId, lockMode)
    def doLock(): Option[DBLockStorageBackend.Lock] = {
      locks = locks + (lock -> (locks.getOrElse(lock, Set.empty) + connection))
      Some(lock)
    }
    lockMode match {
      case DBLockStorageBackend.LockMode.Exclusive =>
        (
          locks.get(lock),
          locks.get(lock.copy(lockMode = DBLockStorageBackend.LockMode.Shared)),
        ) match {
          case (None, None) => doLock()
          case (Some(connections), None) if connections == Set(connection) => doLock()
          case _ => None // if any shared lock held, we cannot lock exclusively
        }
      case DBLockStorageBackend.LockMode.Shared =>
        (
          locks.get(lock),
          locks.get(lock.copy(lockMode = DBLockStorageBackend.LockMode.Exclusive)),
        ) match {
          case (_, None) => doLock()
          case _ => None // if any exclusive lock held, we cannot lock shared
        }
    }
  }

  override def release(lock: DBLockStorageBackend.Lock)(connection: Connection): Boolean =
    synchronized {
      if (connection.isClosed) throw new Exception("trying to release on a closed connection")
      if (!lock.lockId.isInstanceOf[TestLockId]) throw new Exception("foreign lockId")
      removeClosedConnectionRelatedLocks()
      locks.get(lock) match {
        case None => false
        case Some(connections) if connections contains connection =>
          if (connections.size == 1) locks = locks - lock
          else locks = locks + (lock -> (connections - connection))
          true
        case _ => false
      }
    }

  private def removeClosedConnectionRelatedLocks(): Unit =
    locks = locks.collect {
      case (lock, conns) if conns.exists(!_.isClosed) => (lock, conns.filter(!_.isClosed))
    }

  override def lock(id: Int): DBLockStorageBackend.LockId =
    TestLockId(id)

  def cutExclusiveLockHoldingConnection(lockId: Int): Unit = synchronized {
    val mainExclusiveIndexerLock =
      DBLockStorageBackend.Lock(TestLockId(lockId), DBLockStorageBackend.LockMode.Exclusive)
    locks
      .getOrElse(mainExclusiveIndexerLock, Set.empty)
      .foreach(_.close())
    locks = locks - mainExclusiveIndexerLock
  }

  override def dbLockSupported: Boolean = true
}

case class TestLockId(id: Int) extends DBLockStorageBackend.LockId

class TestConnection extends Connection {
  override def createStatement(): Statement = throw new UnsupportedOperationException

  override def prepareStatement(s: String): PreparedStatement =
    throw new UnsupportedOperationException

  override def prepareCall(s: String): CallableStatement = throw new UnsupportedOperationException

  override def nativeSQL(s: String): String = throw new UnsupportedOperationException

  override def setAutoCommit(b: Boolean): Unit = throw new UnsupportedOperationException

  override def getAutoCommit: Boolean = throw new UnsupportedOperationException

  override def commit(): Unit = throw new UnsupportedOperationException

  override def rollback(): Unit = throw new UnsupportedOperationException

  private var _closed: Boolean = false

  override def close(): Unit = synchronized {
    _closed = true
  }

  override def isClosed: Boolean = synchronized(_closed)

  override def getMetaData: DatabaseMetaData = throw new UnsupportedOperationException

  override def setReadOnly(b: Boolean): Unit = throw new UnsupportedOperationException

  override def isReadOnly: Boolean = throw new UnsupportedOperationException

  override def setCatalog(s: String): Unit = throw new UnsupportedOperationException

  override def getCatalog: String = throw new UnsupportedOperationException

  override def setTransactionIsolation(i: Int): Unit = throw new UnsupportedOperationException

  override def getTransactionIsolation: Int = throw new UnsupportedOperationException

  override def getWarnings: SQLWarning = throw new UnsupportedOperationException

  override def clearWarnings(): Unit = throw new UnsupportedOperationException

  override def createStatement(i: Int, i1: Int): Statement = throw new UnsupportedOperationException

  override def prepareStatement(s: String, i: Int, i1: Int): PreparedStatement =
    throw new UnsupportedOperationException

  override def prepareCall(s: String, i: Int, i1: Int): CallableStatement =
    throw new UnsupportedOperationException

  override def getTypeMap: util.Map[String, Class[_]] = throw new UnsupportedOperationException

  override def setTypeMap(map: util.Map[String, Class[_]]): Unit =
    throw new UnsupportedOperationException

  override def setHoldability(i: Int): Unit = throw new UnsupportedOperationException

  override def getHoldability: Int = throw new UnsupportedOperationException

  override def setSavepoint(): Savepoint = throw new UnsupportedOperationException

  override def setSavepoint(s: String): Savepoint = throw new UnsupportedOperationException

  override def rollback(savepoint: Savepoint): Unit = throw new UnsupportedOperationException

  override def releaseSavepoint(savepoint: Savepoint): Unit =
    throw new UnsupportedOperationException

  override def createStatement(i: Int, i1: Int, i2: Int): Statement =
    throw new UnsupportedOperationException

  override def prepareStatement(s: String, i: Int, i1: Int, i2: Int): PreparedStatement =
    throw new UnsupportedOperationException

  override def prepareCall(s: String, i: Int, i1: Int, i2: Int): CallableStatement =
    throw new UnsupportedOperationException

  override def prepareStatement(s: String, i: Int): PreparedStatement =
    throw new UnsupportedOperationException

  override def prepareStatement(s: String, ints: Array[Int]): PreparedStatement =
    throw new UnsupportedOperationException

  override def prepareStatement(s: String, strings: Array[String]): PreparedStatement =
    throw new UnsupportedOperationException

  override def createClob(): Clob = throw new UnsupportedOperationException

  override def createBlob(): Blob = throw new UnsupportedOperationException

  override def createNClob(): NClob = throw new UnsupportedOperationException

  override def createSQLXML(): SQLXML = throw new UnsupportedOperationException

  override def isValid(i: Int): Boolean = throw new UnsupportedOperationException

  override def setClientInfo(s: String, s1: String): Unit = throw new UnsupportedOperationException

  override def setClientInfo(properties: Properties): Unit = throw new UnsupportedOperationException

  override def getClientInfo(s: String): String = throw new UnsupportedOperationException

  override def getClientInfo: Properties = throw new UnsupportedOperationException

  override def createArrayOf(s: String, objects: Array[AnyRef]): sql.Array =
    throw new UnsupportedOperationException

  override def createStruct(s: String, objects: Array[AnyRef]): Struct =
    throw new UnsupportedOperationException

  override def setSchema(s: String): Unit = throw new UnsupportedOperationException

  override def getSchema: String = throw new UnsupportedOperationException

  override def abort(executor: Executor): Unit = throw new UnsupportedOperationException

  override def setNetworkTimeout(executor: Executor, i: Int): Unit =
    throw new UnsupportedOperationException

  override def getNetworkTimeout: Int = throw new UnsupportedOperationException

  override def unwrap[T](aClass: Class[T]): T = throw new UnsupportedOperationException

  override def isWrapperFor(aClass: Class[_]): Boolean = throw new UnsupportedOperationException
}
