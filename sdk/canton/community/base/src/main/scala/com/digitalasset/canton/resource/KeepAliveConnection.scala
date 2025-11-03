// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.syntax.either.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.resource.DbStorage.NoConnectionAvailable
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.typesafe.scalalogging.Logger
import org.slf4j.event.Level
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcDataSource
import slick.util.AsyncExecutor

import java.sql.{Array as _, *}
import java.util
import java.util.Properties
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean

object KeepAliveConnection {

  implicit val pretty: Pretty[KeepAliveConnection] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*

    implicit val prettyConnection: Pretty[Connection] = prettyOfString(_.toString)

    prettyOfClass(unnamedParam(_.underlying))
  }

  /** Single threaded database.
    *
    * Should only be used for low-volume workloads.
    */
  def createDatabaseFromConnection(
      connection: KeepAliveConnection,
      logger: Logger,
      asyncExecutor: AsyncExecutor,
  ): Database =
    Database.forSource(
      new JdbcDataSource {
        def createConnection(): Connection =
          if (connection.markInUse())
            connection
          else
            throw NoConnectionAvailable()
        def close(): Unit = {
          import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
          connection.closeUnderlying(Level.WARN)(
            ErrorLoggingContext.fromTracedLogger(TracedLogger(logger))
          )
        }
        val maxConnections: Option[Int] = Some(1)
      },
      asyncExecutor,
    )

}

/** Connection wrapper to prevent closing of the connection. */
class KeepAliveConnection(conn: Connection) extends Connection {

  private[resource] val inUse: AtomicBoolean = new AtomicBoolean(false)

  private[resource] def markInUse(): Boolean = inUse.compareAndSet(false, true)

  private[resource] def markFree(): Unit = inUse.compareAndSet(true, false).discard

  def close(): Unit =
    // Only mark the connection as free, but not closing the actual connection
    markFree().discard

  private[resource] def underlying: Connection = conn

  private[resource] def closeUnderlying(
      logLevel: Level
  )(implicit errorLoggingContext: ErrorLoggingContext): Unit =
    Either
      .catchOnly[SQLException](conn.close())
      .valueOr { err =>
        LoggerUtil.logAtLevel(logLevel, show"Failed to close connection: $err")(errorLoggingContext)
      }
  // Delegated methods below
  def createStatement(): Statement = conn.createStatement()
  def setAutoCommit(autoCommit: Boolean): Unit = conn.setAutoCommit(autoCommit)
  def setHoldability(holdability: Int): Unit = conn.setHoldability(holdability)
  def clearWarnings(): Unit = conn.clearWarnings()
  def getNetworkTimeout: Int = conn.getNetworkTimeout
  def createBlob(): Blob = conn.createBlob()
  def createSQLXML(): SQLXML = conn.createSQLXML()
  def setSavepoint(): Savepoint = conn.setSavepoint()
  def setSavepoint(name: String): Savepoint = conn.setSavepoint(name)
  def createNClob(): NClob = conn.createNClob()
  def getTransactionIsolation: Int = conn.getTransactionIsolation
  def getClientInfo(name: String): String = conn.getClientInfo(name)
  def getClientInfo: Properties = conn.getClientInfo
  def getSchema: String = conn.getSchema
  def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit =
    conn.setNetworkTimeout(executor, milliseconds)
  def getMetaData: DatabaseMetaData = conn.getMetaData
  def getTypeMap: util.Map[String, Class[?]] = conn.getTypeMap
  def rollback(): Unit = conn.rollback()
  def rollback(savepoint: Savepoint): Unit = conn.rollback(savepoint)
  def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement =
    conn.createStatement(resultSetType, resultSetConcurrency)
  def createStatement(
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int,
  ): Statement =
    conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)
  def getHoldability: Int = conn.getHoldability
  def setReadOnly(readOnly: Boolean): Unit = conn.setReadOnly(readOnly)
  def setClientInfo(name: String, value: String): Unit = conn.setClientInfo(name, value)
  def setClientInfo(properties: Properties): Unit = conn.setClientInfo(properties)
  def isReadOnly: Boolean = conn.isReadOnly
  def setTypeMap(map: util.Map[String, Class[?]]): Unit = conn.setTypeMap(map)
  def getCatalog: String = conn.getCatalog
  def createClob(): Clob = conn.createClob
  def setTransactionIsolation(level: Int): Unit = conn.setTransactionIsolation(level)
  def nativeSQL(sql: String): String = conn.nativeSQL(sql)
  def prepareCall(sql: String): CallableStatement = conn.prepareCall(sql)
  def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement =
    conn.prepareCall(sql, resultSetType, resultSetConcurrency)
  def prepareCall(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int,
  ): CallableStatement =
    conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
  def createArrayOf(typeName: String, elements: Array[AnyRef]): java.sql.Array =
    conn.createArrayOf(typeName, elements)
  def setCatalog(catalog: String): Unit = conn.setCatalog(catalog)
  def getAutoCommit: Boolean = conn.getAutoCommit
  def abort(executor: Executor): Unit = conn.abort(executor)
  def isValid(timeout: Int): Boolean = conn.isValid(timeout)
  def prepareStatement(sql: String): PreparedStatement = conn.prepareStatement(sql)
  def prepareStatement(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
  ): PreparedStatement =
    conn.prepareStatement(sql, resultSetType, resultSetConcurrency)
  def prepareStatement(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int,
  ): PreparedStatement =
    conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
  def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement =
    conn.prepareStatement(sql, autoGeneratedKeys)
  def prepareStatement(sql: String, columnIndexes: Array[Int]): PreparedStatement =
    conn.prepareStatement(sql, columnIndexes)
  def prepareStatement(sql: String, columnNames: Array[String]): PreparedStatement =
    conn.prepareStatement(sql, columnNames)
  def releaseSavepoint(savepoint: Savepoint): Unit = conn.releaseSavepoint(savepoint)
  def isClosed: Boolean = conn.isClosed
  def createStruct(typeName: String, attributes: Array[AnyRef]): Struct =
    conn.createStruct(typeName, attributes)
  def getWarnings: SQLWarning = conn.getWarnings
  def setSchema(schema: String): Unit = conn.setSchema(schema)
  def commit(): Unit = conn.commit()
  def unwrap[T](iface: Class[T]): T = conn.unwrap[T](iface)
  def isWrapperFor(iface: Class[?]): Boolean = conn.isWrapperFor(iface)
}
