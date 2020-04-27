// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.test.runner

import com.typesafe.scalalogging.LazyLogging

import scala.sys.process.{Process, ProcessLogger}
import java.io.File

object Runner extends LazyLogging {

  class LazyProcessLogger(val prefix: String = "") extends ProcessLogger with LazyLogging {
    def out(s: => String): Unit = logger.info(prefix + s)
    def err(s: => String): Unit = logger.warn(prefix + s)
    def buffer[T](f: => T): T = f
  }

  def execute(
      command: Seq[String],
      log: Option[ProcessLogger] = None,
      cwd: Option[File] = None): Int = {
    logger.info(s"Executing `${command.mkString(" ")}`${cwd.map(f => s" in `$f`").getOrElse("")}")
    log.fold(Process(command, cwd).!)(l => Process(command, cwd).!(l))
  }

  def executeAsync(
      command: Seq[String],
      log: Option[ProcessLogger] = None,
      cwd: Option[File] = None): Process = {
    logger.info(s"Executing `${command.mkString(" ")}`${cwd.map(f => s" in `$f`").getOrElse("")}")
    val process = log.fold(Process(command, cwd).run())(l => Process(command, cwd).run(l))
    sys addShutdownHook {
      if (process.isAlive()) {
        process.destroy()
      }
    }
    process
  }
}
