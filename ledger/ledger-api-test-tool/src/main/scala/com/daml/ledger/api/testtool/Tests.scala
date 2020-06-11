// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.nio.file.Path

import com.daml.ledger.api.testtool
import com.daml.ledger.api.testtool.infrastructure.{BenchmarkReporter, LedgerTestSuite}
import com.daml.ledger.api.testtool.tests._
import org.slf4j.LoggerFactory

object Tests {
  type Tests = Seq[LedgerTestSuite]

  /**
    * These tests are safe to be run concurrently and are
    * always run by default, unless otherwise specified.
    */
  val default: Tests = Seq(
    new ActiveContractsServiceIT,
    new ClosedWorldIT,
    new CommandDeduplicationIT,
    new CommandServiceIT,
    new CommandSubmissionCompletionIT,
    new ContractKeysIT,
    new DivulgenceIT,
    new HealthServiceIT,
    new IdentityIT,
    new LedgerConfigurationServiceIT,
    new PackageManagementServiceIT,
    new PackageServiceIT,
    new PartyManagementServiceIT,
    new SemanticTests,
    new TransactionServiceIT,
    new WitnessesIT,
    new WronglyTypedContractIdIT,
  )

  /**
    * These tests can:
    * - change the global state of the ledger, or
    * - be especially resource intensive (by design)
    *
    * These are consequently not run unless otherwise specified.
    */
  def optional(config: Config): Tests = Seq(
    new ConfigManagementServiceIT,
    new LotsOfPartiesIT,
    new TransactionScaleIT(config.loadScaleFactor),
  )

  def all(config: Config): Tests = default ++ optional(config)

  /**
    * These are performance envelope tests that also provide benchmarks and are always run
    * sequentially; they also must be specified explicitly with --perf-tests and will exclude
    * all other tests.
    */
  def performanceTests(path: Option[Path]): Tests = {
    val reporter =
      (key: String, value: Double) =>
        path
          .map(BenchmarkReporter.toFile)
          .getOrElse(BenchmarkReporter.toStream(System.out))
          .addReport(key, value)

    Envelope.values.flatMap { envelope =>
      {
        val throughputKey: String = performanceEnvelopeThroughputTestKey(envelope)
        val latencyKey: String = performanceEnvelopeLatencyTestKey(envelope)
        val transactionSizeKey: String = performanceEnvelopeTransactionSizeTestKey(envelope)
        List(
          new testtool.tests.PerformanceEnvelope.ThroughputTest(
            throughputKey,
            logger = LoggerFactory.getLogger(throughputKey),
            envelope = envelope,
            reporter = reporter,
          ),
          new testtool.tests.PerformanceEnvelope.LatencyTest(
            latencyKey,
            logger = LoggerFactory.getLogger(latencyKey),
            envelope = envelope,
            reporter = reporter,
          ),
          new testtool.tests.PerformanceEnvelope.TransactionSizeScaleTest(
            transactionSizeKey,
            logger = LoggerFactory.getLogger(transactionSizeKey),
            envelope = envelope,
          ),
        )
      }
    }
  }.toSeq

  private[this] def performanceEnvelopeThroughputTestKey(envelope: Envelope): String =
    s"PerformanceEnvelope.${envelope.name}.Throughput"
  private[this] def performanceEnvelopeLatencyTestKey(envelope: Envelope): String =
    s"PerformanceEnvelope.${envelope.name}.Latency"
  private[this] def performanceEnvelopeTransactionSizeTestKey(envelope: Envelope): String =
    s"PerformanceEnvelope.${envelope.name}.TransactionSize"

  private[testtool] val PerformanceTestsKeys =
    Envelope.values.flatMap { envelope =>
      List(
        performanceEnvelopeThroughputTestKey(envelope),
        performanceEnvelopeLatencyTestKey(envelope),
        performanceEnvelopeTransactionSizeTestKey(envelope)),
    }
}
