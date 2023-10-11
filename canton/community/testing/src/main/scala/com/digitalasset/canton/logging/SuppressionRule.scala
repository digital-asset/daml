// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import org.slf4j.event
import org.slf4j.event.Level

import scala.reflect.ClassTag

/** Specify which log messages should be suppressed and captured. */
trait SuppressionRule {

  /** Determines whether an event we the given level should be suppressed.
    */
  def isSuppressed(loggerName: String, eventLevel: Level): Boolean
}

object SuppressionRule {

  /** Suppress events at this level for loggers of [[T]] class type. */
  final case class LoggerAndLevel[T: ClassTag](level: event.Level) extends SuppressionRule {
    private val className = implicitly[ClassTag[T]].runtimeClass.getSimpleName
    override def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      loggerName.contains(className) && eventLevel.toInt == level.toInt
  }

  /** Suppress events that are at this level or above (error is highest). */
  final case class LevelAndAbove(level: event.Level) extends SuppressionRule {
    def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      eventLevel.toInt >= level.toInt
  }

  /** Supress only this level of events */
  final case class Level(level: event.Level) extends SuppressionRule {
    def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      eventLevel.toInt == level.toInt
  }

  /** No suppression */
  case object NoSuppression extends SuppressionRule {
    override def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean = false
  }
}
