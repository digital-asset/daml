// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import com.daml.bazeltools.BazelRunfiles
import com.daml.test.evidence.generator.TestEntry.{ReliabilityTestEntry, SecurityTestEntry}
import com.daml.test.evidence.tag.Reliability.{ReliabilityTest, ReliabilityTestSuite}
import com.daml.test.evidence.tag.Security.{SecurityTest, SecurityTestSuite}
import org.scalatest.Suite
import org.scalatest.daml.ScalaTestAdapter

import scala.reflect.ClassTag
import scala.io.Source

object TestEntryLookup {

  private def collectTestEvidence[TT: ClassTag, TS: ClassTag, TE](
      scalaTestSuites: List[Suite],
      testEntry: (String, String, TT, Boolean, Option[TS]) => TE,
  ): List[TE] =
    List.empty
      .concat(ScalaTestGeneratorSupport.testEntries(scalaTestSuites, testEntry))

  private def collectEntries[TT: ClassTag, TS: ClassTag, TE](
      testEntry: (String, String, TT, Boolean, Option[TS]) => TE
  ): List[TE] = {
    val runpathList: List[String] =
      Source
        .fromFile(BazelRunfiles.rlocation("test-evidence/generator.runpath"))
        .getLines()
        .map(BazelRunfiles.rlocation)
        .toList

    val testSuites: List[Suite] = ScalaTestAdapter.loadTestSuites(runpathList, fatalWarnings = true)

    collectTestEvidence[TT, TS, TE](
      testSuites,
      testEntry,
    )
  }

  def securityTestEntries: List[SecurityTestEntry] =
    collectEntries[SecurityTest, SecurityTestSuite, SecurityTestEntry](
      SecurityTestEntry.apply
    ).sorted

  def reliabilityTestEntries: List[ReliabilityTestEntry] =
    collectEntries[ReliabilityTest, ReliabilityTestSuite, ReliabilityTestEntry](
      ReliabilityTestEntry.apply
    ).sorted

}
