// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import better.files.File
import com.daml.test.evidence.generator.ScalaTestGeneratorSupport.testEntries
import com.daml.test.evidence.generator.TestEntry.{ReliabilityTestEntry, SecurityTestEntry}
import com.daml.test.evidence.generator.TestEntryCsvEncoder
import com.daml.test.evidence.generator.TestEntryCsvEncoder.{
  ReliabilityTestEntryCsv,
  SecurityTestEntryCsv,
}
import com.daml.test.evidence.scalatest.JsonCodec.ReliabilityJson.*
import com.daml.test.evidence.scalatest.JsonCodec.SecurityJson.*
import com.daml.test.evidence.tag.Reliability.{ReliabilityTest, ReliabilityTestSuite}
import com.daml.test.evidence.tag.Security.{SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.integration.util.BackgroundRunnerHelpers
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.scalatest.daml.ScalaTestAdapter

object SystematicTestingGenerator {

  def main(args: Array[String]): Unit = {
    val cp = BackgroundRunnerHelpers.extractTestClassPath()
    val runpathList = cp.filter(_.endsWith("classes")).toList
    val testSuites = ScalaTestAdapter.loadTestSuites(runpathList)

    println(s"Found #${testSuites.size} test suites, instantiating them:\n")

    println("Writing security tests inventory..")
    val securityTests =
      testEntries[SecurityTest, SecurityTestSuite, SecurityTestEntry](
        testSuites,
        SecurityTestEntry.apply,
      )
    File("security-tests.json").write(securityTests.asJson.spaces2)
    TestEntryCsvEncoder.write(
      File("security-tests.csv"),
      securityTests.map(SecurityTestEntryCsv.apply),
    )

    println("Writing reliability tests inventory..")
    val reliabilityTests =
      testEntries[ReliabilityTest, ReliabilityTestSuite, ReliabilityTestEntry](
        testSuites,
        ReliabilityTestEntry.apply,
      )
    File("reliability-tests.json").write(reliabilityTests.asJson.spaces2)
    TestEntryCsvEncoder.write(
      File("reliability-tests.csv"),
      reliabilityTests.map(ReliabilityTestEntryCsv.apply),
    )

    sys.exit()
  }
}
