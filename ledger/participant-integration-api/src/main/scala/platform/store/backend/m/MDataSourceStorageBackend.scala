// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.io.PrintWriter
import java.{sql, util}
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
import java.util.logging.Logger

import com.daml.logging.LoggingContext
import com.daml.platform.store.backend.DataSourceStorageBackend
import javax.sql.DataSource

object MDataSourceStorageBackend extends DataSourceStorageBackend {
  override def createDataSource(
      jdbcUrl: String,
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      connectionInitHook: Option[Connection => Unit],
  )(implicit loggingContext: LoggingContext): DataSource = {
    val mprefix = "jdbc:m:"
    val h2prefix = "jdbc:h2"
    if (jdbcUrl.startsWith(mprefix)) {
      assert(jdbcUrl.startsWith(mprefix))
      val id = jdbcUrl.substring(mprefix.length)
      MStore.globalMap.putIfAbsent(id, new MStore)
      new MDataSource(id)
    } else if (jdbcUrl.startsWith(h2prefix)) {
      val id = jdbcUrl.substring(h2prefix.length)
      MStore.globalMap.putIfAbsent(id, new MStore)
      new MDataSource(id)
    } else throw new UnsupportedOperationException("only h2 and m jdbc urls supported")
  }

  override def checkDatabaseAvailable(connection: Connection): Unit = {
    MStore(connection)
    ()
  }

  def purge(id: String): Unit = {
    MStore.globalMap.remove(id)
    ()
  }
}

class MDataSource(id: String) extends DataSource {
  override val getConnection: Connection = new MConnection(id)

  override def getConnection(s: String, s1: String): Connection =
    throw new UnsupportedOperationException

  override def unwrap[T](aClass: Class[T]): T = throw new UnsupportedOperationException

  override def isWrapperFor(aClass: Class[_]): Boolean = throw new UnsupportedOperationException

  override def getLogWriter: PrintWriter = throw new UnsupportedOperationException

  override def setLogWriter(printWriter: PrintWriter): Unit =
    throw new UnsupportedOperationException

  override def setLoginTimeout(i: Int): Unit = throw new UnsupportedOperationException

  override def getLoginTimeout: Int = throw new UnsupportedOperationException

  override def getParentLogger: Logger = throw new UnsupportedOperationException
}

class MConnection(val id: String) extends Connection {
  override def createStatement(): Statement = throw new UnsupportedOperationException

  override def prepareStatement(s: String): PreparedStatement =
    throw new UnsupportedOperationException

  override def prepareCall(s: String): CallableStatement = throw new UnsupportedOperationException

  override def nativeSQL(s: String): String = throw new UnsupportedOperationException

  override def setAutoCommit(b: Boolean): Unit = ()

  override def getAutoCommit: Boolean = throw new UnsupportedOperationException

  override def commit(): Unit = ()

  override def rollback(): Unit = ()

  override def close(): Unit = ()

  override def isClosed: Boolean = throw new UnsupportedOperationException

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
