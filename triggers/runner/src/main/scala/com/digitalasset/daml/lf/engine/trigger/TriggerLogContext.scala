// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.engine.trigger.Runner.Implicits._
import com.daml.logging.LoggingContextOf
import com.daml.logging.LoggingContextOf.label
import com.daml.logging.entries.LoggingValue.OfString
import com.daml.logging.entries.{LoggingEntries, LoggingValue}

import java.util.UUID

object ToLoggingContext {
  implicit def `TriggerLogContext to LoggingContextOf[Trigger]`(implicit
      triggerContext: TriggerLogContext
  ): LoggingContextOf[Trigger] =
    LoggingContextOf
      .withEnrichedLoggingContext(
        label[Trigger],
        "trigger" -> LoggingValue.Nested(LoggingEntries(triggerContext.entries: _*)),
      )(triggerContext.loggingContext)
      .run(identity)
}

final case class TriggerLogContext private (
    loggingContext: LoggingContextOf[Trigger],
    entries: (String, LoggingValue)*
) {

  def enrichTriggerContext[A](
      additionalEntries: (String, LoggingValue)*
  )(f: TriggerLogContext => A): A = {
    f(TriggerLogContext(loggingContext, entries ++ additionalEntries: _*))
  }

  def nextSpan[A](spanName: String)(f: TriggerLogContext => A): A = {
    val baseSpan: Seq[(String, LoggingValue)] =
      Seq("span.name" -> spanName, "span.id" -> UUID.randomUUID())
    val spanContext: Seq[(String, LoggingValue)] =
      entries.toMap.get("span.id").fold(baseSpan)(id => ("span.parent" -> id) +: baseSpan)
    val baseContext = entries.filterNot { case (key, _) =>
      Seq("span.name", "span.id", "span.parent").contains(key)
    }

    f(TriggerLogContext(loggingContext, baseContext ++ spanContext: _*))
  }

  def subSpan[A](name: String)(f: TriggerLogContext => A): A = {
    val spanName = entries.toMap
      .get("span.name")
      .fold(name)(prefix => s"${prefix.asInstanceOf[OfString].value}.$name")
    val baseSpan: Seq[(String, LoggingValue)] =
      Seq("span.name" -> spanName, "span.id" -> UUID.randomUUID())
    val spanContext: Seq[(String, LoggingValue)] =
      entries.toMap.get("span.id").fold(baseSpan)(id => ("span.parent" -> id) +: baseSpan)
    val baseContext = entries.filterNot { case (key, _) =>
      Seq("span.name", "span.id", "span.parent").contains(key)
    }

    f(TriggerLogContext(loggingContext, baseContext ++ spanContext: _*))
  }
}

object TriggerLogContext {
  def newSpan[A](
      name: String
  )(f: TriggerLogContext => A)(implicit loggingContext: LoggingContextOf[Trigger]): A = {
    TriggerLogContext(
      loggingContext,
      "span.name" -> name,
      "span.id" -> UUID.randomUUID(),
    ).enrichTriggerContext()(f)
  }
}
