// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.nio.file.Path

import com.daml.ledger.api.testtool.infrastructure.{BenchmarkReporter, Envelope, LedgerTestSuite}

import scala.collection.SortedSet

package object performance {
  def performanceTests(path: Option[Path]): Map[String, LedgerTestSuite] = {
    val target =
      path
        .map(BenchmarkReporter.toFile)
        .getOrElse(BenchmarkReporter.toStream(System.out))

    Envelope.All.iterator
      .map(e => e.name -> PerformanceEnvelope(e, target.addReport))
      .toMap
  }

  private[testtool] val PerformanceTestsKeys: SortedSet[String] =
    SortedSet(Envelope.All.map(_.name): _*)
}
