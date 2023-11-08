// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap

object ContextualizedLogger {

  // Caches loggers to prevent them from needlessly wasting memory
  // Replicates the behavior of the underlying Slf4j logger factory
  private[this] val cache = TrieMap.empty[String, ContextualizedLogger]

  // Allows to explicitly pass a logger, should be used for testing only
  private[logging] def createFor(withoutContext: Logger): ContextualizedLogger =
    new ContextualizedLogger(withoutContext)

  // Slf4j handles the caching of the underlying logger itself
  def createFor(name: String): ContextualizedLogger =
    cache.getOrElseUpdate(name, createFor(LoggerFactory.getLogger(name)))

  /** Gets from cache (or creates) a [[ContextualizedLogger]].
    * Automatically strips the `$` at the end of Scala `object`s' name.
    */
  def get(clazz: Class[_]): ContextualizedLogger =
    createFor(clazz.getName.stripSuffix("$"))

}

final class ContextualizedLogger private (val withoutContext: Logger) {

  val trace = new LeveledLogger.Trace(withoutContext)
  val debug = new LeveledLogger.Debug(withoutContext)
  val info = new LeveledLogger.Info(withoutContext)
  val warn = new LeveledLogger.Warn(withoutContext)
  val error = new LeveledLogger.Error(withoutContext)

  def debugStream[Out](
      toLoggable: Out => String
  )(implicit loggingContext: LoggingContext): Flow[Out, Out, NotUsed] =
    Flow[Out].map { item =>
      debug(toLoggable(item))
      item
    }

}
