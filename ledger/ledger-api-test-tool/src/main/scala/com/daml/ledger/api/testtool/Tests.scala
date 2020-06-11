// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.nio.file.Path

import com.daml.ledger.api.testtool
import com.daml.ledger.api.testtool.infrastructure.{BenchmarkReporter, LedgerTestSuite}
import com.daml.ledger.api.testtool.tests._
import org.slf4j.LoggerFactory

object Tests {
  type Tests = Map[String, LedgerTestSuite]

  /**
    * These tests are safe to be run concurrently and are
    * always run by default, unless otherwise specified.
    */
  val default: Tests = Map(
    "ActiveContractsServiceIT" -> (new ActiveContractsServiceIT),
    "CommandServiceIT" -> (new CommandServiceIT),
    "CommandSubmissionCompletionIT" -> (new CommandSubmissionCompletionIT),
    "CommandDeduplicationIT" -> (new CommandDeduplicationIT),
    "ContractKeysIT" -> (new ContractKeysIT),
    "DivulgenceIT" -> (new DivulgenceIT),
    "HealthServiceIT" -> (new HealthServiceIT),
    "IdentityIT" -> (new IdentityIT),
    "LedgerConfigurationServiceIT" -> (new LedgerConfigurationServiceIT),
    "PackageManagementServiceIT" -> (new PackageManagementServiceIT),
    "PackageServiceIT" -> (new PackageServiceIT),
    "PartyManagementServiceIT" -> (new PartyManagementServiceIT),
    "SemanticTests" -> (new SemanticTests),
    "TransactionServiceIT" -> (new TransactionServiceIT),
    "WitnessesIT" -> (new WitnessesIT),
    "WronglyTypedContractIdIT" -> (new WronglyTypedContractIdIT),
    "ClosedWorldIT" -> (new ClosedWorldIT),
  )

  /**
    * These tests can:
    * - change the global state of the ledger, or
    * - be especially resource intensive (by design)
    *
    * These are consequently not run unless otherwise specified.
    */
  def optional(config: Config): Tests = Map(
    "ConfigManagementServiceIT" -> (new ConfigManagementServiceIT),
    "LotsOfPartiesIT" -> (new LotsOfPartiesIT),
    "TransactionScaleIT" -> (new TransactionScaleIT(config.loadScaleFactor)),
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
          throughputKey -> (new testtool.tests.PerformanceEnvelope.ThroughputTest(
            logger = LoggerFactory.getLogger(throughputKey),
            envelope = envelope,
            reporter = reporter,
          )),
          latencyKey -> (new testtool.tests.PerformanceEnvelope.LatencyTest(
            logger = LoggerFactory.getLogger(latencyKey),
            envelope = envelope,
            reporter = reporter,
          )),
          transactionSizeKey -> (new testtool.tests.PerformanceEnvelope.TransactionSizeScaleTest(
            logger = LoggerFactory.getLogger(transactionSizeKey),
            envelope = envelope,
          )),
        )
      }
    }
  }.toMap

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
