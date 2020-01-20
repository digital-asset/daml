// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.logging

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
  private[logging] def createFor(name: String): ContextualizedLogger =
    createFor(LoggerFactory.getLogger(name))

  /**
    * Gets from cache (or creates) a [[ContextualizedLogger]].
    * Automatically strips the `$` at the end of Scala `object`s' name.
    */
  def get(clazz: Class[_]): ContextualizedLogger = {
    val name = clazz.getName.stripSuffix("$")
    cache.getOrElseUpdate(name, createFor(name))
  }

}

final class ContextualizedLogger private (val withoutContext: Logger) {

  val trace = new LeveledLogger.Trace(withoutContext)
  val debug = new LeveledLogger.Debug(withoutContext)
  val info = new LeveledLogger.Info(withoutContext)
  val warn = new LeveledLogger.Warn(withoutContext)
  val error = new LeveledLogger.Error(withoutContext)

}
