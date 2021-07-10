// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.io.PrintWriter
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
import java.{sql, util}

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import javax.sql.DataSource

private[backend] object InitHookDataSourceProxy {
  val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  def apply(
      delegate: DataSource,
      initHooks: List[Connection => Unit],
  )(implicit loggingContext: LoggingContext): DataSource =
    if (initHooks.isEmpty) delegate
    else InitHookDataSourceProxy(delegate, c => initHooks.foreach(_(c)))
}

import com.daml.platform.store.backend.common.InitHookDataSourceProxy._

private[backend] case class InitHookDataSourceProxy(
    delegate: DataSource,
    initHook: Connection => Unit,
)(implicit loggingContext: LoggingContext)
    extends DataSource {
  override def getConnection: Connection = {
    val connectionId =
      List.fill(8)(scala.util.Random.nextPrintableChar()).mkString // TODO FIXME maybe not needed
    logger.debug(s"$connectionId Creating new connection")
    val connection = delegate.getConnection
    try {
      logger.debug(s"$connectionId Applying connection init hook")
      initHook(connection)
    } catch {
      case t: Throwable =>
        logger.info(s"$connectionId Init hook execution failed: ${t.getMessage}")
        throw t
    }
    logger.info(s"$connectionId Init hook execution finished successfully, connection ready")
    new LoggingConnectionProxy(connection, connectionId)
  }

  override def getConnection(s: String, s1: String): Connection = {
    val connection = delegate.getConnection(s, s1)
    initHook(connection)
    connection
  }

  override def getLogWriter: PrintWriter = delegate.getLogWriter

  override def setLogWriter(printWriter: PrintWriter): Unit = delegate.setLogWriter(printWriter)

  override def setLoginTimeout(i: Int): Unit = delegate.setLoginTimeout(i)

  override def getLoginTimeout: Int = delegate.getLoginTimeout

  override def getParentLogger: Logger = delegate.getParentLogger

  override def unwrap[T](aClass: Class[T]): T = delegate.unwrap(aClass)

  override def isWrapperFor(aClass: Class[_]): Boolean = delegate.isWrapperFor(aClass)
}

// TODO consider to remove this is only for logging the closures of connections
private[backend] class LoggingConnectionProxy(
    delegate: Connection,
    connectionId: String,
)(implicit loggingContext: LoggingContext)
    extends Connection {
  override def createStatement(): Statement = delegate.createStatement()

  override def prepareStatement(s: String): PreparedStatement = delegate.prepareStatement(s)

  override def prepareCall(s: String): CallableStatement = delegate.prepareCall(s)

  override def nativeSQL(s: String): String = delegate.nativeSQL(s)

  override def setAutoCommit(b: Boolean): Unit = delegate.setAutoCommit(b)

  override def getAutoCommit: Boolean = delegate.getAutoCommit

  override def commit(): Unit = delegate.commit()

  override def rollback(): Unit = delegate.rollback()

  override def close(): Unit = {
    logger.info(s"$connectionId Connection is closing")
    delegate.close()
    logger.info(s"$connectionId Connection is closed")
  }

  override def isClosed: Boolean = delegate.isClosed

  override def getMetaData: DatabaseMetaData = delegate.getMetaData

  override def setReadOnly(b: Boolean): Unit = delegate.setReadOnly(b)

  override def isReadOnly: Boolean = delegate.isReadOnly

  override def setCatalog(s: String): Unit = delegate.setCatalog(s)

  override def getCatalog: String = delegate.getCatalog

  override def setTransactionIsolation(i: Int): Unit = delegate.setTransactionIsolation(i)

  override def getTransactionIsolation: Int = delegate.getTransactionIsolation

  override def getWarnings: SQLWarning = delegate.getWarnings

  override def clearWarnings(): Unit = delegate.clearWarnings()

  override def createStatement(i: Int, i1: Int): Statement = delegate.createStatement(i, i1)

  override def prepareStatement(s: String, i: Int, i1: Int): PreparedStatement =
    delegate.prepareStatement(s, i, i1)

  override def prepareCall(s: String, i: Int, i1: Int): CallableStatement =
    delegate.prepareCall(s, i, i1)

  override def getTypeMap: util.Map[String, Class[_]] = delegate.getTypeMap

  override def setTypeMap(map: util.Map[String, Class[_]]): Unit = delegate.setTypeMap(map)

  override def setHoldability(i: Int): Unit = delegate.setHoldability(i)

  override def getHoldability: Int = delegate.getHoldability

  override def setSavepoint(): Savepoint = delegate.setSavepoint()

  override def setSavepoint(s: String): Savepoint = delegate.setSavepoint(s)

  override def rollback(savepoint: Savepoint): Unit = delegate.rollback(savepoint)

  override def releaseSavepoint(savepoint: Savepoint): Unit = delegate.releaseSavepoint(savepoint)

  override def createStatement(i: Int, i1: Int, i2: Int): Statement =
    delegate.createStatement(i, i1, i2)

  override def prepareStatement(s: String, i: Int, i1: Int, i2: Int): PreparedStatement =
    delegate.prepareStatement(s, i, i1, i2)

  override def prepareCall(s: String, i: Int, i1: Int, i2: Int): CallableStatement =
    delegate.prepareCall(s, i, i1, i2)

  override def prepareStatement(s: String, i: Int): PreparedStatement =
    delegate.prepareStatement(s, i)

  override def prepareStatement(s: String, ints: Array[Int]): PreparedStatement =
    delegate.prepareStatement(s, ints)

  override def prepareStatement(s: String, strings: Array[String]): PreparedStatement =
    delegate.prepareStatement(s, strings)

  override def createClob(): Clob = delegate.createClob()

  override def createBlob(): Blob = delegate.createBlob()

  override def createNClob(): NClob = delegate.createNClob()

  override def createSQLXML(): SQLXML = delegate.createSQLXML()

  override def isValid(i: Int): Boolean = delegate.isValid(i)

  override def setClientInfo(s: String, s1: String): Unit = delegate.setClientInfo(s, s1)

  override def setClientInfo(properties: Properties): Unit = delegate.setClientInfo(properties)

  override def getClientInfo(s: String): String = delegate.getClientInfo(s)

  override def getClientInfo: Properties = delegate.getClientInfo

  override def createArrayOf(s: String, objects: Array[AnyRef]): sql.Array =
    delegate.createArrayOf(s, objects)

  override def createStruct(s: String, objects: Array[AnyRef]): Struct =
    delegate.createStruct(s, objects)

  override def setSchema(s: String): Unit = delegate.setSchema(s)

  override def getSchema: String = delegate.getSchema

  override def abort(executor: Executor): Unit = delegate.abort(executor)

  override def setNetworkTimeout(executor: Executor, i: Int): Unit =
    delegate.setNetworkTimeout(executor, i)

  override def getNetworkTimeout: Int = delegate.getNetworkTimeout

  override def unwrap[T](aClass: Class[T]): T = delegate.unwrap(aClass)

  override def isWrapperFor(aClass: Class[_]): Boolean = delegate.isWrapperFor(aClass)
}
