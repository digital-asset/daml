// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import better.files.File
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing

import scala.sys.process.Process
import scala.util.{Failure, Success, Try}

class ExternalCommandExecutor(protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging
    with NoTracing {

  /** Helper to execute external process commands. Returns output string if successful, else throws
    */
  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def exec(cmd: String, errorHint: String, dir: Option[File] = None): String = {
    val processLogger = new BufferedProcessLogger()
    Try {
      Process(cmd, dir.map(_.toJava)) ! processLogger
    }.transform(
      exitCode =>
        if (exitCode == 0) {
          val out = processLogger.output()
          logger.info(s"Successfully executed '$cmd': $out")
          Success(out)
        } else {
          val msg =
            s"Failed execution of '$cmd'. $errorHint Exit code $exitCode: ${processLogger.output()}"
          logger.warn(msg)
          Failure(new IllegalStateException(msg))
        },
      t => {
        val msg =
          s"Exception while executing '$cmd'. $errorHint Exception ${t.getMessage}: ${processLogger.output()}"
        logger.warn(msg, t)
        Failure(t)
      },
    ).get
  }
}
