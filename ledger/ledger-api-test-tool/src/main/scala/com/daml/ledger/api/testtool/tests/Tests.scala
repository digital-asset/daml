// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.nio.file.Path

import com.daml.ledger.api.testtool.infrastructure.{BenchmarkReporter, Envelope, LedgerTestSuite}
import com.daml.ledger.api.testtool.suites._

import scala.collection.SortedSet
import scala.concurrent.duration.FiniteDuration

object Tests {

  def default(
      timeoutScaleFactor: Double = Defaults.TimeoutScaleFactor,
      ledgerClockGranularity: FiniteDuration = Defaults.LedgerClockGranularity,
  ): Vector[LedgerTestSuite] =
    Vector(
      new ActiveContractsServiceIT,
      new ClosedWorldIT,
      new CommandServiceIT,
      new CommandSubmissionCompletionIT,
      new CommandDeduplicationIT(timeoutScaleFactor, ledgerClockGranularity),
      new ConfigManagementServiceIT,
      new ContractKeysIT,
      new DivulgenceIT,
      new HealthServiceIT,
      new IdentityIT,
      new LedgerConfigurationServiceIT,
      new PackageManagementServiceIT,
      new PackageServiceIT,
      new PartyManagementServiceIT,
      new RaceConditionIT,
      new SemanticTests,
      new TransactionServiceIT,
      new WitnessesIT,
      new WronglyTypedContractIdIT,
    )

  val optional: Vector[LedgerTestSuite] =
    Vector(
      new ParticipantPruningIT,
      new MultiPartySubmissionIT,
    )

  val retired: Vector[LedgerTestSuite] =
    Vector(
      new LotsOfPartiesIT,
      new TransactionScaleIT,
    )

  /** These are performance envelope tests that also provide benchmarks and are always run
    * sequentially; they also must be specified explicitly with --perf-tests and will exclude
    * all other tests.
    */
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
