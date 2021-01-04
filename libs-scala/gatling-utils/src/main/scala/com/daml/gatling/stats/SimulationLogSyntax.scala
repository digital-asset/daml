// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.daml.gatling.stats.SimulationLog.ScenarioStats
import com.daml.scalautil.Statement.discard

object SimulationLogSyntax {
  implicit class SimulationLogOps(val log: SimulationLog) extends AnyVal {

    /**
      * Will write a summary.csv given a Gatling result directory.
      * @param targetDirectory the directory where the summary.csv will be created.
      */
    def writeSummaryCsv(targetDirectory: File): Unit = {
      discard {
        Files.write(
          new File(targetDirectory, "summary.csv").toPath,
          log.toCsvString.getBytes(StandardCharsets.UTF_8))
      }
    }

    def writeSummaryText(targetDirectory: File): String = {
      val summary = formatTextReport(log.scenarios)
      discard {
        Files.write(
          new File(targetDirectory, "summary.txt").toPath,
          summary.getBytes(StandardCharsets.UTF_8))
      }
      summary
    }

    private def formatTextReport(scenarios: List[ScenarioStats]): String = {
      val buf = new StringBuffer()
      scenarios.foreach { x =>
        x.requestsByType.foreach {
          case (name, stats) =>
            buf.append(stats.formatted(name))
        }
      }
      buf.toString
    }
  }
}
