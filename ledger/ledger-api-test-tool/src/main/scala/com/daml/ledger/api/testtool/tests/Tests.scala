// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.nio.file.Path

import com.daml.ledger.api.testtool.infrastructure.{BenchmarkReporter, Envelope, LedgerTestSuite}
import com.daml.ledger.api.testtool.suites.CompletionDeduplicationInfoIT.{
  CommandService,
  CommandSubmissionService,
}
import com.daml.ledger.api.testtool.suites._
import com.daml.ledger.test.TestDar
import com.daml.lf.language.LanguageVersion

import scala.collection.SortedSet

object Tests {

  private val supportsExceptions: Boolean = {
    import scala.Ordering.Implicits.infixOrderingOps
    TestDar.lfVersion >= LanguageVersion.Features.exceptions
  }

  def default(
      timeoutScaleFactor: Double = Defaults.TimeoutScaleFactor
  ): Vector[LedgerTestSuite] =
    Vector(
      new ActiveContractsServiceIT,
      new ClosedWorldIT,
      new CommandDeduplicationIT(timeoutScaleFactor),
      new CommandDeduplicationParallelIT,
      new CommandDeduplicationPeriodValidationIT,
      new CommandServiceIT,
      new CommandSubmissionCompletionIT,
      new CompletionDeduplicationInfoIT(CommandService),
      new CompletionDeduplicationInfoIT(CommandSubmissionService),
      new ConfigManagementServiceIT,
      new ContractIdIT,
      new ContractKeysIT,
      new DeeplyNestedValueIT,
      new DivulgenceIT,
      new HealthServiceIT,
      new IdentityIT,
      new LedgerConfigurationServiceIT,
      new MultiPartySubmissionIT,
      new PackageManagementServiceIT,
      new PackageServiceIT,
      new ParticipantPruningIT,
      new PartyManagementServiceIT,
      new RaceConditionIT,
      new SemanticTests,
      new TimeServiceIT,
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

  def optional(): Vector[LedgerTestSuite] =
    Vector(
      new MonotonicRecordTimeIT,
      new TLSOnePointThreeIT,
      new TLSAtLeastOnePointTwoIT,
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
