// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{BenchmarkReporter, Envelope, LedgerTestSuite}
import com.daml.ledger.api.testtool.suites.AppendOnlyCompletionDeduplicationInfoIT.{
  CommandService,
  CommandSubmissionService,
}
import com.daml.ledger.api.testtool.suites._
import com.daml.ledger.test.TestDar
import com.daml.lf.language.LanguageVersion

import java.nio.file.Path
import scala.collection.SortedSet
import scala.concurrent.duration.FiniteDuration

object Tests {

  private val supportsExceptions: Boolean = {
    import scala.Ordering.Implicits.infixOrderingOps
    TestDar.lfVersion >= LanguageVersion.Features.exceptions
  }

  def default(
      timeoutScaleFactor: Double = Defaults.TimeoutScaleFactor,
      ledgerClockGranularity: FiniteDuration = Defaults.LedgerClockGranularity,
  ): Vector[LedgerTestSuite] =
    Vector(
      new ActiveContractsServiceIT,
      new ClosedWorldIT,
      new CommandDeduplicationIT(timeoutScaleFactor, ledgerClockGranularity),
      new CommandServiceIT,
      new CommandSubmissionCompletionIT,
      new ConfigManagementServiceIT,
      new ContractKeysIT,
      new DeeplyNestedValueIT,
      new DivulgenceIT,
      new HealthServiceIT,
      new IdentityIT,
      new LedgerConfigurationServiceIT,
      new PackageManagementServiceIT,
      new PackageServiceIT,
      new PartyManagementServiceIT,
      new RaceConditionIT,
      new SemanticTests,
      new TransactionServiceArgumentsIT,
      new TransactionServiceAuthorizationIT,
      new TransactionServiceCorrectnessIT,
      new TransactionServiceExerciseIT,
      new TransactionServiceOutputsIT,
      new TransactionServiceQueryIT,
      new TransactionServiceStakeholdersIT,
      new TransactionServiceStreamsIT,
      new TransactionServiceValidationIT,
      new TransactionServiceVisibilityIT,
      new ValueLimitsIT,
      new WitnessesIT,
      new WronglyTypedContractIdIT,
    ) ++ (if (supportsExceptions) Vector(new ExceptionsIT, new ExceptionRaceConditionIT)
          else Vector.empty)

  def optional(
      timeoutScaleFactor: Double = Defaults.TimeoutScaleFactor,
      ledgerClockGranularity: FiniteDuration = Defaults.LedgerClockGranularity,
  ): Vector[LedgerTestSuite] =
    Vector(
      new AppendOnlyCompletionDeduplicationInfoIT(CommandService),
      new AppendOnlyCompletionDeduplicationInfoIT(CommandSubmissionService),
      new KVCommandDeduplicationIT(timeoutScaleFactor, ledgerClockGranularity),
      new ContractIdIT,
      new MultiPartySubmissionIT,
      new ParticipantPruningIT,
      new MonotonicRecordTimeIT,
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
