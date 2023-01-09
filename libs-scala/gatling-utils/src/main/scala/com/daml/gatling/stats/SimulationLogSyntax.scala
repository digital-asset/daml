// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats

import java.nio.file.Path
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.daml.gatling.stats.SimulationLog.RequestTypeStats
import com.daml.scalautil.Statement.discard

object SimulationLogSyntax {
  implicit class SimulationLogOps(val log: SimulationLog) extends AnyVal {

    /** Will write a summary.csv given a Gatling result directory.
      * @param targetDirectory the directory where the summary.csv will be created.
      */
    def writeSummaryCsv(targetDirectory: Path): Unit = {
      discard {
        Files.write(
          targetDirectory.resolve("summary.csv"),
          log.toCsvString.getBytes(StandardCharsets.UTF_8),
        )
      }
    }

    def writeSummaryText(targetDirectory: Path): String = {
      val summary = formatTextReport(log.requests)
      discard {
        Files.write(
          targetDirectory.resolve("summary.txt"),
          summary.getBytes(StandardCharsets.UTF_8),
        )
      }
      summary
    }

    private def formatTextReport(requests: Map[String, RequestTypeStats]): String = {
      val buf = new StringBuffer()
      requests.foreach { case (name, stats) =>
        buf.append(stats.formatted(name))
      }
      buf.toString
    }
  }
}
