// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase

import scala.beans.BeanProperty
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.reflect.ClassTag

object LogCollector {

  private val log =
    TrieMap
      .empty[String, TrieMap[String, mutable.Builder[(Level, String), Vector[(Level, String)]]]]

  def read[Test, Logger](
      implicit test: ClassTag[Test],
      logger: ClassTag[Logger]): IndexedSeq[(Level, String)] =
    log
      .get(test.runtimeClass.getName)
      .flatMap(_.get(logger.runtimeClass.getName))
      .fold(IndexedSeq.empty[(Level, String)])(_.result())

  def clear[Test](implicit test: ClassTag[Test]): Unit = {
    log.remove(test.runtimeClass.getName)
    ()
  }

}

final class LogCollector extends AppenderBase[ILoggingEvent] {

  @BeanProperty
  var test: String = _

  override def append(e: ILoggingEvent): Unit = {
    if (test == null) {
      addError("Test identifier undefined, skipping logging")
    } else {
      val log = LogCollector.log
        .getOrElseUpdate(test, TrieMap.empty)
        .getOrElseUpdate(e.getLoggerName, Vector.newBuilder)
      val _ = log.synchronized { log += e.getLevel -> e.getMessage }
    }
  }
}
