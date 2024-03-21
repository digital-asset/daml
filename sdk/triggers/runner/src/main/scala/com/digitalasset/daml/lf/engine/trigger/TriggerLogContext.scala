// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.data.{BackStack, NoCopy}
import com.daml.lf.engine.trigger.Runner.Implicits._
import com.daml.lf.engine.trigger.TriggerLogContext._
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.logging.LoggingContextOf.label
import com.daml.logging.entries.{LoggingEntries, LoggingValue}

import java.util.UUID

object ToLoggingContext {
  implicit def `TriggerLogContext to LoggingContextOf[Trigger]`(implicit
      triggerContext: TriggerLogContext
  ): LoggingContextOf[Trigger] = {
    val parentEntries = if (triggerContext.span.parent.isEmpty) {
      LoggingEntries.empty
    } else if (triggerContext.span.parent.size == 1) {
      LoggingEntries("parent" -> triggerContext.span.parent.head)
    } else {
      LoggingEntries("parent" -> triggerContext.span.parent)
    }
    val spanEntries = LoggingEntries(
      "span" -> LoggingValue.Nested(
        LoggingEntries(
          "name" -> triggerContext.span.path.toImmArray.foldLeft("trigger")((path, name) =>
            s"$path.$name"
          ),
          "id" -> triggerContext.span.id,
        ) ++ parentEntries
      )
    )

    LoggingContextOf
      .withEnrichedLoggingContext(
        label[Trigger],
        "trigger" -> LoggingValue.Nested(LoggingEntries(triggerContext.entries: _*) ++ spanEntries),
      )(triggerContext.loggingContext)
      .run(identity)
  }
}

final class TriggerLogContext private (
    private[trigger] val loggingContext: LoggingContextOf[Trigger],
    private[trigger] val entries: Seq[(String, LoggingValue)],
    private[trigger] val span: TriggerLogSpan,
    private[trigger] val callback: (String, TriggerLogContext) => Unit,
) extends NoCopy {

  import ToLoggingContext._

  def enrichTriggerContext[A](
      additionalEntries: (String, LoggingValue)*
  )(f: TriggerLogContext => A): A = {
    f(new TriggerLogContext(loggingContext, entries ++ additionalEntries, span, callback))
  }

  def nextSpan[A](
      name: String,
      additionalEntries: (String, LoggingValue)*
  )(f: TriggerLogContext => A)(implicit
      logger: ContextualizedLogger
  ): A = {
    val context = new TriggerLogContext(
      loggingContext,
      entries ++ additionalEntries,
      span.nextSpan(name),
      callback,
    )

    try {
      context.logInfo("span entry")
      f(context)
    } finally {
      context.logInfo("span exit")
    }
  }

  def childSpan[A](
      name: String,
      additionalEntries: (String, LoggingValue)*
  )(f: TriggerLogContext => A)(implicit
      logger: ContextualizedLogger
  ): A = {
    val context = new TriggerLogContext(
      loggingContext,
      entries ++ additionalEntries,
      span.childSpan(name),
      callback,
    )

    try {
      context.logInfo("span entry")
      f(context)
    } finally {
      context.logInfo("span exit")
    }
  }

  def groupWith(contexts: TriggerLogContext*): TriggerLogContext = {
    val groupEntries = contexts.foldLeft(entries.toSet) { case (entries, context) =>
      entries ++ context.entries.toSet
    }
    val groupSpans = contexts.foldLeft(span) { case (span, context) =>
      span.groupWith(context.span)
    }

    new TriggerLogContext(loggingContext, groupEntries.toSeq, groupSpans, callback)
  }

  def logError(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
      callback(message, triggerContext)
      logger.error(message)
    }
  }

  def logWarning(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
      callback(message, triggerContext)
      logger.warn(message)
    }
  }

  def logInfo(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
      callback(message, triggerContext)
      logger.info(message)
    }
  }

  def logDebug(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
      callback(message, triggerContext)
      logger.debug(message)
    }
  }

  def logTrace(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
      callback(message, triggerContext)
      logger.trace(message)
    }
  }
}

object TriggerLogContext {
  def newRootSpan[A](
      span: String,
      entries: (String, LoggingValue)*
  )(f: TriggerLogContext => A)(implicit loggingContext: LoggingContextOf[Trigger]): A = {
    new TriggerLogContext(
      loggingContext,
      entries,
      TriggerLogSpan(BackStack(span)),
      (_, _) => (),
    ).enrichTriggerContext()(f)
  }

  private[trigger] def newRootSpanWithCallback[A](
      span: String,
      callback: (String, TriggerLogContext) => Unit,
      entries: (String, LoggingValue)*
  )(f: TriggerLogContext => A)(implicit loggingContext: LoggingContextOf[Trigger]): A = {
    new TriggerLogContext(
      loggingContext,
      entries,
      TriggerLogSpan(BackStack(span)),
      callback,
    ).enrichTriggerContext()(f)
  }

  private[trigger] final case class TriggerLogSpan(
      path: BackStack[String],
      id: UUID = UUID.randomUUID(),
      parent: Set[UUID] = Set.empty,
  ) {
    def nextSpan(name: String): TriggerLogSpan = {
      val basePath = path.pop.fold(BackStack.empty[String])(_._1)

      TriggerLogSpan(basePath :+ name, parent = parent)
    }

    def childSpan(name: String): TriggerLogSpan = {
      TriggerLogSpan(path :+ name, parent = Set(id))
    }

    def groupWith(span: TriggerLogSpan): TriggerLogSpan = {
      copy(parent = parent + span.id)
    }
  }
}
