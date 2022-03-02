// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.doobie.logging

import org.slf4j.{Logger, LoggerFactory}
import doobie.util.log.{ExecFailure, LogHandler, ProcessingFailure, Success}

object Slf4jLogHandler {

  def apply(clazz: Class[_]): LogHandler =
    apply(LoggerFactory.getLogger(clazz))

  def apply(logger: Logger): LogHandler =
    LogHandler {
      case Success(s, a, e1, e2) =>
        logger.debug(s"""Successful Statement Execution:
                        |
                        |  ${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")}
                        |
                        | arguments = [${a.mkString(", ")}]
                        |   elapsed = ${e1.toMillis.toString} ms exec + ${e2.toMillis.toString} ms processing (${(e1 + e2).toMillis.toString} ms total)
          """.stripMargin)
      case ProcessingFailure(s, a, e1, e2, t) =>
        logger.error(s"""Failed Resultset Processing:
                        |
                        |  ${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")}
                        |
                        | arguments = [${a.mkString(", ")}]
                        |   elapsed = ${e1.toMillis.toString} ms exec + ${e2.toMillis.toString} ms processing (failed) (${(e1 + e2).toMillis.toString} ms total)
                        |   failure = ${t.getMessage}
          """.stripMargin)
      case ExecFailure(s, a, e1, t) =>
        logger.error(s"""Failed Statement Execution:
                        |
                        |  ${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")}
                        |
                        | arguments = [${a.mkString(", ")}]
                        |   elapsed = ${e1.toMillis.toString} ms exec (failed)
                        |   failure = ${t.getMessage}
          """.stripMargin)
    }
}
