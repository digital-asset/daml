// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.io.PrintWriter
import java.sql.Connection
import java.util.logging.Logger

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

  private def getConnection(connectionBody: => Connection): Connection = {
    logger.debug(s"Creating new connection")
    val connection = connectionBody
    try {
      logger.debug(s"Applying connection init hook")
      initHook(connection)
    } catch {
      case t: Throwable =>
        logger.warn(s"Init hook execution failed", t)
        try {
          connection.close() // releasing resources in case of initialisation issues
        } catch {
          case _: Throwable => () // catching all resource-releasing exceptions
        }
        throw t
    }
    logger.info(s"Init hook execution finished successfully, connection ready")
    connection
  }

  override def getConnection: Connection = getConnection(delegate.getConnection)

  override def getConnection(s: String, s1: String): Connection = getConnection(
    delegate.getConnection(s, s1)
  )

  override def getLogWriter: PrintWriter = delegate.getLogWriter

  override def setLogWriter(printWriter: PrintWriter): Unit = delegate.setLogWriter(printWriter)

  override def setLoginTimeout(i: Int): Unit = delegate.setLoginTimeout(i)

  override def getLoginTimeout: Int = delegate.getLoginTimeout

  override def getParentLogger: Logger = delegate.getParentLogger

  override def unwrap[T](aClass: Class[T]): T = delegate.unwrap(aClass)

  override def isWrapperFor(aClass: Class[_]): Boolean = delegate.isWrapperFor(aClass)
}
