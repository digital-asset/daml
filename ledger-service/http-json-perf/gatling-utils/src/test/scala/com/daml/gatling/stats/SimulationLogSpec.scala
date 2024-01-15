// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats

import org.scalactic.TypeCheckedTripleEquals

import com.daml.gatling.stats.util.ReadFileSyntax._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.bazeltools.BazelRunfiles.requiredResource

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SimulationLogSpec extends AnyFlatSpec with Matchers with TypeCheckedTripleEquals {

  private val simulationLog =
    "ledger-service/http-json-perf/gatling-utils/src/test/resources/simulation-log"

  behavior of "SimulationLog"

  it should "produce the expected CSV" in {
    val log = SimulationLog
      .fromFiles(
        requiredResource(s"$simulationLog/stats.json").toPath,
        requiredResource(s"$simulationLog/assertions.json").toPath,
      )
      .getOrElse(fail("Failed to parse simulation log"))
    val expected = requiredResource(s"$simulationLog/expected.csv").toPath.contentsAsString
      .getOrElse(fail("Failed to read expected csv"))
    log.toCsvString shouldBe expected

  }

}
