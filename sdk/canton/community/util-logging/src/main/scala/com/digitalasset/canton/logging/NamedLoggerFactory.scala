// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap

/** NamedLoggerFactory augments a regular class-based slf4j logger with one annotated with a "name" where the name provides
  * human readable context identifying a class instance, e.g. which participant in a set of participants has logged
  * a particular message.
  *
  * The name can be constructed in a nested, left-to-right append manner.
  */
trait NamedLoggerFactory {

  /** Name for the logger. Can be empty. */
  val name: String

  /** Yields the name in a form that is suitable as thread name. */
  lazy val threadName: String = {
    if (properties.isEmpty) {
      "canton"
    } else {
      properties.values.mkString("-").replaceAll("[^A-Za-z0-9_-]", "_")
    }
  }

  /** Key-value map enabling structured logging, e.g. in ledger api server. */
  val properties: ListMap[String, String]

  /** Regular append method to augment log properties and append both key and value to logger factory name
    *
    * @param key   log property key
    * @param value log property value
    * @return new named logger factory
    * @throws java.lang.IllegalArgumentException if key already set
    */
  def append(key: String, value: String): NamedLoggerFactory

  /** Special purpose append method to augment log properties to append only the value to logger factory name for
    * reduced verbosity
    *
    * @param key   log property key, only added to property map, but not appended to name
    * @param value log property value
    * @return new named logger factory
    * @throws java.lang.IllegalArgumentException if key already set
    */
  def appendUnnamedKey(key: String, value: String): NamedLoggerFactory

  private[logging] def getLogger(fullName: String): Logger

  /** get a loggers in factory methods
    *
    * Sometimes, the NamedLogging trait can not be used, e.g. in a factory method. In these
    * cases, a logger can be created using this function.
    */
  def getLogger(klass: Class[_]): Logger = {
    val fullName = Array(klass.getName, name)
      .filterNot(_.isEmpty)
      .mkString(":")
    getLogger(fullName)
  }

  /** same as get logger for traced loggers */
  def getTracedLogger(klass: Class[_]): TracedLogger = TracedLogger(klass, this)

}

// Named Logger Factory implementation
private[logging] final class SimpleNamedLoggerFactory(
    val name: String,
    val properties: ListMap[String, String],
) extends NamedLoggerFactory {
  import NamedLoggerFactory.nameFromKv

  override def append(key: String, value: String): NamedLoggerFactory = {
    validateKey(key, value)
    new SimpleNamedLoggerFactory(concat(name, nameFromKv(key, value)), properties.+((key, value)))
  }

  override def appendUnnamedKey(key: String, value: String): NamedLoggerFactory = {
    validateKey(key, value)
    new SimpleNamedLoggerFactory(concat(name, value), properties.+((key, value)))
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

  override private[logging] def getLogger(fullName: String): Logger =
    Logger(LoggerFactory.getLogger(fullName))

  private[logging] def concat(a: String*) =
    a.filter(_.nonEmpty)
      .map(
        // We need to sanitize invalid values (instead of throwing an exception),
        // because values contain user input and it would be pretty hard to educate the user to not use dots.
        _.replaceFirst("\\.+$", "") // Remove trailing dots.
          .replaceAll("\\.", "_") // Replace any other "." by "_".
      )
      .mkString("/")
}

object NamedLoggerFactory {
  def apply(key: String, value: String) =
    new SimpleNamedLoggerFactory(nameFromKv(key, value), ListMap(key -> value))

  /** Adds to kv-map (to appear in upstream logs) and to name without "key/" prefix for brevity (#2768, #2793),
    * e.g. used for test where the test name makes it clear that value refers to a test
    */
  def unnamedKey(key: String, value: String) =
    new SimpleNamedLoggerFactory(value, ListMap(key -> value))

  def root: NamedLoggerFactory = new SimpleNamedLoggerFactory("", ListMap.empty)

  private[logging] def nameFromKv(key: String, value: String) = s"$key=$value"
}
