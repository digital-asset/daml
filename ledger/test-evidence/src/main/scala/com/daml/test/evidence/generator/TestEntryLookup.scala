// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.suites
import com.daml.test.evidence.generator.TestEntry.{ReliabilityTestEntry, SecurityTestEntry}
import com.daml.test.evidence.tag.Reliability.{ReliabilityTest, ReliabilityTestSuite}
import com.daml.test.evidence.tag.Security.{SecurityTest, SecurityTestSuite}
import org.scalatest.Suite
import org.scalatest.daml.ScalaTestAdapter

import scala.reflect.ClassTag

object TestEntryLookup {

  private def collectTestEvidence[TT: ClassTag, TS: ClassTag, TE](
      scalaTestSuites: List[Suite],
      ledgerApiSuites: List[LedgerTestSuite],
      testEntry: (String, String, TT, Boolean, Option[TS]) => TE,
  ): List[TE] =
    List.empty
      .concat(ScalaTestGeneratorSupport.testEntries(scalaTestSuites, testEntry))
      .concat(LedgerApiTestGeneratorSupport.testEntries(ledgerApiSuites, testEntry))

  private def loadClasspath(): Option[String] = Option(System.getProperty("java.class.path"))

  private def collectEntries[TT: ClassTag, TS: ClassTag, TE](
      testEntry: (String, String, TT, Boolean, Option[TS]) => TE
  ): List[TE] = {
    val runpathList: List[String] = loadClasspath()
      .map(_.split(":").toList)
      .getOrElse(List.empty)

    val ledgerApiTests = List()
      .concat(suites.v1_14.default(timeoutScaleFactor = 0L))
      .concat(suites.v1_14.optional(tlsConfig = None))

    val testSuites: List[Suite] = ScalaTestAdapter.loadTestSuites(runpathList)

    collectTestEvidence[TT, TS, TE](
      testSuites,
      ledgerApiTests,
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
