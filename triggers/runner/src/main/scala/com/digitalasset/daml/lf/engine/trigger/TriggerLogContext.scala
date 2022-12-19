// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    val spanEntries = LoggingEntries(
      "span" -> LoggingValue.Nested(
        LoggingEntries(
          "name" -> triggerContext.span.path.toImmArray.foldLeft("trigger")((path, name) =>
            s"$path.$name"
          ),
          "id" -> triggerContext.span.id,
        ) ++ triggerContext.span.parent.fold(LoggingEntries.empty)(id =>
          LoggingEntries("parent" -> id)
        )
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
) extends NoCopy {

  import ToLoggingContext._

  def enrichTriggerContext[A](
      additionalEntries: (String, LoggingValue)*
  )(f: TriggerLogContext => A): A = {
    f(new TriggerLogContext(loggingContext, entries ++ additionalEntries, span))
  }

  def nextSpan[A](
      name: String,
      additionalEntries: (String, LoggingValue)*
  )(f: TriggerLogContext => A): A = {
    f(new TriggerLogContext(loggingContext, entries ++ additionalEntries, span.nextSpan(name)))
  }

  def childSpan[A](
      name: String,
      additionalEntries: (String, LoggingValue)*
  )(f: TriggerLogContext => A): A = {
    f(new TriggerLogContext(loggingContext, entries ++ additionalEntries, span.childSpan(name)))
  }

  def groupWith(context: TriggerLogContext): TriggerLogContext = {
    new TriggerLogContext(loggingContext, entries, span.groupWith(context.span))
  }

  def logError(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
      logger.error(message)
    }
  }

  def logWarning(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
      logger.warn(message)
    }
  }

  def logInfo(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
      logger.info(message)
    }
  }

  def logDebug(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
      logger.debug(message)
    }
  }

  def logTrace(message: String, additionalEntries: (String, LoggingValue)*)(implicit
      logger: ContextualizedLogger
  ): Unit = {
    enrichTriggerContext(additionalEntries: _*) { implicit triggerContext: TriggerLogContext =>
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
    ).enrichTriggerContext()(f)
  }

  private[trigger] final case class TriggerLogSpan(
      path: BackStack[String],
      id: UUID = UUID.randomUUID(),
      parent: Option[UUID] = None,
  ) {
    def nextSpan(name: String): TriggerLogSpan = {
      val basePath = path.pop.fold(BackStack.empty[String])(_._1)

      TriggerLogSpan(basePath :+ name, parent = parent)
    }

    def childSpan(name: String): TriggerLogSpan = {
      TriggerLogSpan(path :+ name, parent = Some(id))
    }

    def groupWith(span: TriggerLogSpan): TriggerLogSpan = {
      copy(parent = Some(span.id))
    }
  }
}
