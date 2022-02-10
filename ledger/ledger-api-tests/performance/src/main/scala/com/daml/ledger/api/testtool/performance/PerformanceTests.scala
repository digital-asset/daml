// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.performance

import java.nio.file.Path

import com.daml.ledger.api.testtool.infrastructure.{
  BenchmarkReporter,
  LedgerTestCase,
  LedgerTestSuite,
}

import scala.collection.immutable.{SortedMap, SortedSet}

final class PerformanceTests(tests: SortedMap[String, LedgerTestSuite]) {
  def names: SortedSet[String] = tests.keySet

  def isEmpty: Boolean = tests.isEmpty

  def nonEmpty: Boolean = tests.nonEmpty

  def filterByName(predicate: String => Boolean): PerformanceTests =
    new PerformanceTests(tests.view.filterKeys(predicate).to(SortedMap))

  def cases: Vector[LedgerTestCase] = tests.values.view.flatMap(_.tests).toVector
}

object PerformanceTests {
  def from(envelopes: Seq[Envelope], outputPath: Option[Path]): PerformanceTests = {
    val target =
      outputPath
        .map(BenchmarkReporter.toFile)
        .getOrElse(BenchmarkReporter.toStream(System.out))
    new PerformanceTests(
      envelopes.iterator
        .map(e => e.name -> PerformanceEnvelope(e, target.addReport))
        .to(SortedMap)
    )
  }
}
