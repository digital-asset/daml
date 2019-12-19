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
  val name: String

  def append(subName: String): NamedLoggerFactory

  def forParticipant(id: String): NamedLoggerFactory =
    append(s"participant/$id")

  def getLogger(klass: Class[_]): Logger = {
    val fullName = Array(klass.getName, name)
      .filterNot(_.isEmpty)
      .mkString(":")
    getLogger(fullName)
  }

  protected def getLogger(fullName: String): Logger
}

object NamedLoggerFactory {
  def apply(name: String): NamedLoggerFactory = new SimpleNamedLoggerFactory(name)

  def apply(cls: Class[_]): NamedLoggerFactory = apply(cls.getSimpleName)

  def forParticipant(name: String): NamedLoggerFactory = root.forParticipant(name)

  def root: NamedLoggerFactory = NamedLoggerFactory("")
}
