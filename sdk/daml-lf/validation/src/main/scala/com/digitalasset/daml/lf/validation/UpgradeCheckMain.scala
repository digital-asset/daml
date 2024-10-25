// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.canton.ledger.error.PackageServiceErrors.Validation
import java.io.File
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.archive.{Error => ArchiveError}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast

import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}

import com.typesafe.scalalogging.Logger
import org.slf4j.helpers.AbstractLogger
import org.slf4j.event.Level
import org.slf4j.Marker
import org.slf4j.helpers.MessageFormatter
import scala.collection.immutable.ListMap

import scala.collection.mutable.Buffer
import com.daml.scalautil.Statement.discard

class StringLogger(val msgs: Buffer[String], val stringLoggerName: String) extends AbstractLogger {
  override def handleNormalizedLoggingCall(lvl: Level, marker: Marker, str: String, args: Array[Object], err: Throwable) = {
    discard(msgs.append(MessageFormatter.basicArrayFormat(str, args)))
  }

  def getFullyQualifiedCallerName() = ""
  def isDebugEnabled(marker: Marker): Boolean = true
  def isDebugEnabled(): Boolean = true
  def isErrorEnabled(marker: Marker): Boolean = true
  def isErrorEnabled(): Boolean = true
  def isInfoEnabled(marker: Marker): Boolean = true
  def isInfoEnabled(): Boolean = true
  def isTraceEnabled(marker: Marker): Boolean = true
  def isTraceEnabled(): Boolean = true
  def isWarnEnabled(marker: Marker): Boolean = true
  def isWarnEnabled(): Boolean = true
}

case class StringLoggerFactory(
  val name: String,
  val properties: ListMap[String, String] = ListMap(),
  val msgs: Buffer[String] = Buffer(),
) extends NamedLoggerFactory {
  override def getLogger(fullName: String): Logger = Logger(new StringLogger(msgs, fullName))

  override def append(key: String, value: String): NamedLoggerFactory = {
    validateKey(key, value)
    new StringLoggerFactory(concat(name, nameFromKv(key, value)), properties.+((key, value)))
  }

  private def nameFromKv(key: String, value: String) = s"$key=$value"

  override def appendUnnamedKey(key: String, value: String): NamedLoggerFactory = {
    validateKey(key, value)
    new StringLoggerFactory(concat(name, value), properties.+((key, value)))
  }

  private def validateKey(key: String, value: String): Unit = {
    // If the key contains a dot, a formatter may abbreviate everything preceding the dot, including the name of the logging class.
    // E.g. "com.digitalasset.canton.logging.NamedLoggerFactory:first.name=john" may get abbreviated to "c.d.c.l.N.name=john".
    if (key.contains("."))
      throw new IllegalArgumentException(
        s"Refusing to use key '$key' containing '.', as that would confuse the log formatters."
      )
    if (properties.get(key).exists(_ != value))
      throw new IllegalArgumentException(
        s"Duplicate adding of key '$key' with value $value to named logger factory properties $properties"
      )
    if (key.isEmpty)
      throw new IllegalArgumentException(
        "Refusing to use empty key for named logger factory property."
      )
  }

  private def concat(a: String*) =
    a.filter(_.nonEmpty)
      .map(
        // We need to sanitize invalid values (instead of throwing an exception),
        // because values contain user input and it would be pretty hard to educate the user to not use dots.
        _.replaceFirst("\\.+$", "") // Remove trailing dots.
          .replaceAll("\\.", "_") // Replace any other "." by "_".
      )
      .mkString("/")
}

final case class CouldNotReadDar(path: String, err: ArchiveError) {
  val message: String = s"Error reading DAR from ${path}: ${err.msg}"
}

case class UpgradeCheckMain (loggerFactory: NamedLoggerFactory) {
  def logger = loggerFactory.getLogger(classOf[UpgradeCheckMain])

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.empty

  private def decodeDar(
      path: String
  ): Either[CouldNotReadDar, Dar[(Ref.PackageId, Ast.Package)]] = {
    val s: String = s"Decoding DAR from ${path}"
    logger.debug(s)
    val result = DarDecoder.readArchiveFromFile(new File(path))
    result.left.map(CouldNotReadDar(path, _))
  }

  val validator = new PackageUpgradeValidator(
    getPackageMap = _ => Map.empty,
    getLfArchive = _ => _ => Future(None),
    loggerFactory = loggerFactory,
  )

  def check(paths: Array[String]): Int = {
    logger.debug(s"Called UpgradeCheckMain with paths: ${paths.toSeq.mkString("\n")}")

    val (failures, dars) = paths.partitionMap(decodeDar(_))
    if (failures.nonEmpty) {
      failures.foreach((e: CouldNotReadDar) => logger.error(e.message))
      1
    } else {
      val archives = for { dar <- dars; archive <- dar.all.toSeq } yield {
        logger.debug(s"Package with ID ${archive._1} and metadata ${archive._2.pkgNameVersion}")
        archive
      }

      val validation = validator.validateUpgrade(archives.toList)
      Await.result(validation.value, Duration.Inf) match {
        case Left(err: Validation.Upgradeability.Error) =>
          logger.error(s"Error while checking two DARs:\n${err.cause}")
          1
        case Left(err) =>
          logger.error(s"Error while checking two DARs:\n${err.cause}")
          1
        case Right(()) => 0
      }
    }
  }

  def main(args: Array[String]) = sys.exit(check(args))
}

object UpgradeCheckMain {
  def default: UpgradeCheckMain = {
    UpgradeCheckMain(NamedLoggerFactory.root)
  }

  def test: (UpgradeCheckMain, Buffer[String]) = {
    val loggerFactory = StringLoggerFactory("")
    UpgradeCheckMain(loggerFactory) -> loggerFactory.msgs
  }
}
