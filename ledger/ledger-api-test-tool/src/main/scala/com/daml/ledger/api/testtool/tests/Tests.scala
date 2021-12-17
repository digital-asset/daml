// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{BenchmarkReporter, Envelope, LedgerTestSuite}
import com.daml.ledger.api.testtool.suites.CompletionDeduplicationInfoIT.{
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
      staticTime: Boolean = Defaults.StaticTime,
  ): Vector[LedgerTestSuite] =
    Vector(
      new ActiveContractsServiceIT,
      new ClosedWorldIT,
      new CommandDeduplicationIT(timeoutScaleFactor, ledgerClockGranularity, staticTime),
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
      new UserManagementServiceIT,
      new ValueLimitsIT,
      new WitnessesIT,
      new WronglyTypedContractIdIT,
    ) ++ (if (supportsExceptions) Vector(new ExceptionsIT, new ExceptionRaceConditionIT)
          else Vector.empty)

  def optional(
      timeoutScaleFactor: Double = Defaults.TimeoutScaleFactor,
      ledgerClockGranularity: FiniteDuration = Defaults.LedgerClockGranularity,
      staticTime: Boolean = Defaults.StaticTime,
  ): Vector[LedgerTestSuite] =
    Vector(
      new CompletionDeduplicationInfoIT(CommandService),
      new CompletionDeduplicationInfoIT(CommandSubmissionService),
      new CommandDeduplicationParallelIT,
      new CommandDeduplicationPeriodValidationIT,
      new ContractIdIT,
      new KVCommandDeduplicationIT(timeoutScaleFactor, ledgerClockGranularity, staticTime),
      new MultiPartySubmissionIT,
      new ParticipantPruningIT,
      new MonotonicRecordTimeIT,
      new TLSOnePointThreeIT,
      new TLSAtLeastOnePointTwoIT,
      // TODO sandbox-classic removal: Remove
      new DeprecatedSandboxClassicMemoryContractKeysIT,
      new DeprecatedSandboxClassicMemoryExceptionsIT,
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
