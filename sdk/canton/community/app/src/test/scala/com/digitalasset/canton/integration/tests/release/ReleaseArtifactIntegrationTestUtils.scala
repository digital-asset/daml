// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.BufferedProcessLogger
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.scalatest.{Outcome, SuiteMixin}

/** Mixin this trait in test suite that test against the release artifact
  */
trait ReleaseArtifactIntegrationTestUtils extends FixtureAnyWordSpec with BaseTest with SuiteMixin {

  override protected def withFixture(test: OneArgTest): Outcome = test(new BufferedProcessLogger)

  protected def isEnterprise: Boolean

  override type FixtureParam = BufferedProcessLogger

  protected lazy val packageName = if (isEnterprise) "enterprise" else "community"
  protected lazy val cantonDir = s"$packageName/app/target/release/canton"
  protected lazy val repositoryRootFromCantonDir = "../../../../.."
  protected lazy val cantonBin = s"$cantonDir/bin/canton"
  protected lazy val resourceDir = "community/app/src/test/resources"
  // this warning is potentially thrown when starting Canton with --no-tty
  private lazy val ttyWarning =
    "WARN  org.jline - Unable to create a system terminal, creating a dumb terminal (enable debug logging for more information)"
  private lazy val jsonTtyWarning =
    "\"message\":\"Unable to create a system terminal, creating a dumb terminal (enable debug logging for more information)\",\"logger_name\":\"org.jline\",\"thread_name\":\"main\",\"level\":\"WARN\""
  protected lazy val regexpCommitmentCatchUpWarning =
    "WARN  c\\.d\\.c\\.p\\.p\\.AcsCommitmentProcessor:(.)*ACS_COMMITMENT_DEGRADATION(.)*The participant has activated ACS catchup mode to combat computation problem.(.)*"

  protected def checkOutput(
      logger: BufferedProcessLogger,
      shouldContain: Seq[String] = Seq(),
      shouldNotContain: Seq[String] = Seq(),
      shouldSucceed: Boolean = true,
  ): Unit = {
    // Filter out false positives in help message for last-errors option
    val filters = List(
      jsonTtyWarning,
      ttyWarning,
      "last_errors",
      "last-errors",
      // slow ExecutionContextMonitor warnings
      "WARN  c.d.c.c.ExecutionContextMonitor - Execution context",
    )
    val log = filters
      .foldLeft(logger.output()) { case (log, filter) =>
        log.replace(filter, "")
      }
      .toLowerCase
      // slow participants might activate ACS commitment catch-up mode
      .replaceAll(regexpCommitmentCatchUpWarning, "")

    shouldContain.foreach(str => assert(log.contains(str.toLowerCase())))
    shouldNotContain.foreach(str => assert(!log.contains(str.toLowerCase())))
    val undesirables = Seq("warn", "error", "exception")
    if (shouldSucceed) undesirables.foreach(str => assert(!log.contains(str.toLowerCase())))
  }
}
