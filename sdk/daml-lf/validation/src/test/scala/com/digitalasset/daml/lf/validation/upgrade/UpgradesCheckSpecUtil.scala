// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation
package upgrade

import com.digitalasset.canton.logging.NamedLoggerFactory

import com.typesafe.scalalogging.Logger
import org.slf4j.helpers.AbstractLogger
import org.slf4j.event.Level
import org.slf4j.Marker
import org.slf4j.helpers.MessageFormatter
import scala.collection.immutable.ListMap

import scala.collection.mutable.Buffer
import com.daml.scalautil.Statement.discard

class StringLogger(val msgs: Buffer[String], val stringLoggerName: String) extends AbstractLogger {
  override def handleNormalizedLoggingCall(
      lvl: Level,
      marker: Marker,
      str: String,
      args: Array[Object],
      err: Throwable,
  ) = {
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
