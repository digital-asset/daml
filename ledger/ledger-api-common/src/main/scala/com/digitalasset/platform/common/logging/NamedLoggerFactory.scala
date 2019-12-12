// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.common.logging

import org.slf4j.{Logger, LoggerFactory}

/**
  * NamedLoggerFactory augments a regular class-based slf4j logger with one annotated with a "name" where the name provides
  * human readable context identifying a class instance, e.g. which participant in a set of participants has logged
  * a particular message.
  *
  * The name can be constructed in a nested, left-to-right append manner.
  */
trait NamedLoggerFactory {

  /** Name for the logger. Can be empty. */
  val name: String

  /** Augment the name with another sub-name. Useful when transitioning from one caller to one specific named callee */
  def append(subName: String): NamedLoggerFactory

  def forParticipant(id: String): NamedLoggerFactory = append(s"participant/$id")

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

}

// Named Logger Factory implementation
private[logging] final class SimpleNamedLoggerFactory(val name: String) extends NamedLoggerFactory {
  override def append(subName: String): NamedLoggerFactory =
    if (name.isEmpty) new SimpleNamedLoggerFactory(subName)
    else new SimpleNamedLoggerFactory(s"$name/$subName")

  override private[logging] def getLogger(fullName: String) = LoggerFactory.getLogger(fullName)
}

object NamedLoggerFactory {
  def apply(name: String): NamedLoggerFactory = new SimpleNamedLoggerFactory(name)
  def apply(cls: Class[_]): NamedLoggerFactory = apply(cls.getSimpleName)

  def forParticipant(name: String): NamedLoggerFactory = root.forParticipant(name)
  def root: NamedLoggerFactory = NamedLoggerFactory("")
}
