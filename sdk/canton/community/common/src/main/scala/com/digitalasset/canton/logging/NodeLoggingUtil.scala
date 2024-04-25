// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import cats.syntax.functor.*
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger}
import ch.qos.logback.core.spi.AppenderAttachable
import ch.qos.logback.core.{Appender, FileAppender}
import com.google.protobuf.ByteString

import scala.jdk.CollectionConverters.*

/** Set of utility methods to access logging information of this node */
object NodeLoggingUtil {

  /** Set the global canton logger level
    *
    * This method works based on the logback.xml and the CantonFilterEvaluator
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def setLevel(loggerName: String = "com.digitalasset.canton", level: String): Unit = {

    // TODO(#18445): setting the log level for a particular logger doesn't work due to this one here
    //    the fix would include to set it explicitly for the logger here as a property
    //    and then to check in the CantonFilterEvaluator whether there exists are more detailed
    //    configuration
    if (Seq("com.digitalasset.canton", "com.daml").exists(loggerName.startsWith))
      System.setProperty("LOG_LEVEL_CANTON", level)

    val logger = getLogger(loggerName)
    if (level == "null")
      logger.setLevel(null)
    else
      logger.setLevel(Level.valueOf(level))
  }

  def getLogger(loggerName: String): Logger = {
    import org.slf4j.LoggerFactory
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val logger: Logger = LoggerFactory.getLogger(loggerName).asInstanceOf[Logger]
    logger
  }

  private def getAppenders(logger: Logger): List[Appender[ILoggingEvent]] = {
    def go(currentAppender: Appender[ILoggingEvent]): List[Appender[ILoggingEvent]] = {
      currentAppender match {
        case attachable: AppenderAttachable[ILoggingEvent @unchecked] =>
          attachable.iteratorForAppenders().asScala.toList.flatMap(go)
        case appender: Appender[ILoggingEvent] => List(appender)
      }
    }

    logger.iteratorForAppenders().asScala.toList.flatMap(go)
  }

  private lazy val rootLogger = getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)

  private lazy val allAppenders = getAppenders(rootLogger)

  private def findAppender(appenderName: String): Option[Appender[ILoggingEvent]] =
    Option(rootLogger.getAppender(appenderName))
      .orElse(allAppenders.find(_.getName == appenderName))

  private lazy val lastErrorsAppender: Option[LastErrorsAppender] = {
    findAppender("LAST_ERRORS") match {
      case Some(lastErrorsAppender: LastErrorsAppender) => Some(lastErrorsAppender)
      case _ => None
    }
  }

  private def renderError(errorEvent: ILoggingEvent): String = {
    findAppender("FILE") match {
      case Some(appender: FileAppender[ILoggingEvent]) =>
        ByteString.copyFrom(appender.getEncoder.encode(errorEvent)).toStringUtf8
      case _ => errorEvent.getFormattedMessage
    }
  }

  def lastErrors(): Option[Map[String, String]] =
    lastErrorsAppender.map(_.lastErrors.fmap(renderError))

  def lastErrorTrace(traceId: String): Option[Seq[String]] = {
    lastErrorsAppender.flatMap(_.lastErrorTrace(traceId)).map(_.map(renderError))
  }

}
