// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.common.logging

import org.slf4j.Logger

/**
  * NamedLoggerFactory augments a regular class-based slf4j logger with one annotated with a "name"
  * where the name provides human readable context identifying a class instance, e.g. which
  * participant in a set of participants has logged a particular message.
  *
  * The name can be constructed in a nested, left-to-right append manner.
  */
trait NamedLoggerFactory {
  protected val name: String

  def getLogger(cls: Class[_]): Logger =
    getLogger(nameOfClass(cls))

  protected def nameOfClass(cls: Class[_]): String =
    Array(cls.getName, name)
      .filterNot(_.isEmpty)
      .mkString(":")

  protected def getLogger(fullName: String): Logger
}

object NamedLoggerFactory {
  def apply(name: String): NamedLoggerFactory = new SimpleNamedLoggerFactory(name)

  def apply(cls: Class[_]): NamedLoggerFactory = apply(cls.getSimpleName)

  def forParticipant(id: String): NamedLoggerFactory = NamedLoggerFactory(s"participant/$id")

  def root: NamedLoggerFactory = NamedLoggerFactory("")
}
