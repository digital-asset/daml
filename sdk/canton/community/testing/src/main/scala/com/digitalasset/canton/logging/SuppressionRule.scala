// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import org.slf4j.event
import org.slf4j.event.Level

import scala.reflect.ClassTag

/** Specify which log messages should be suppressed and captured. */
sealed trait SuppressionRule {

  /** Determines whether an event we the given level should be suppressed.
    */
  def isSuppressed(loggerName: String, eventLevel: Level): Boolean

  def `&&`(that: SuppressionRule): SuppressionRule =
    SuppressionRule.And(this, that)

  def `||`(that: SuppressionRule): SuppressionRule =
    SuppressionRule.Or(this, that)

  def unary_! : SuppressionRule =
    SuppressionRule.Not(this)
}

object SuppressionRule {

  private final case class And(s1: SuppressionRule, s2: SuppressionRule) extends SuppressionRule {
    override def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      s1.isSuppressed(loggerName, eventLevel) && s2.isSuppressed(loggerName, eventLevel)
  }

  private final case class Or(s1: SuppressionRule, s2: SuppressionRule) extends SuppressionRule {
    override def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      s1.isSuppressed(loggerName, eventLevel) || s2.isSuppressed(loggerName, eventLevel)
  }

  private final case class Not(s: SuppressionRule) extends SuppressionRule {
    override def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      !s.isSuppressed(loggerName, eventLevel)
  }

  // Ideally, use `forLogger`. This is prone to errors if the relevant class is renamed
  // It is useful if you need to not have the logging class as a visible dependency
  final case class LoggerNameContains(className: String) extends SuppressionRule {
    override def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      loggerName.contains(className)
  }

  def forLogger[A](implicit ct: ClassTag[A]): SuppressionRule =
    LoggerNameContains(ct.runtimeClass.getSimpleName)

  /** Suppress events that are at this level or above (error is highest). */
  final case class LevelAndAbove(level: event.Level) extends SuppressionRule {
    def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      eventLevel.toInt >= level.toInt
  }

  /** Suppress only this level of events */
  final case class Level(level: event.Level) extends SuppressionRule {
    def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      eventLevel.toInt == level.toInt
  }

  case object FullSuppression extends SuppressionRule {
    override def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean = true
  }

  case object NoSuppression extends SuppressionRule {
    override def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean = false
  }
}
