// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.nio.file.Path

import com.daml.ledger.api.testtool
import com.daml.ledger.api.testtool.infrastructure.{
  BenchmarkReporter,
  LedgerSession,
  LedgerTestSuite
}
import com.daml.ledger.api.testtool.tests._
import org.slf4j.LoggerFactory

object Tests {
  type Tests = Map[String, LedgerSession => LedgerTestSuite]

  /**
    * These tests are safe to be run concurrently and are
    * always run by default, unless otherwise specified.
    */
  val default: Tests = Map(
    "ActiveContractsServiceIT" -> (new ActiveContractsService(_)),
    "CommandServiceIT" -> (new CommandService(_)),
    "CommandSubmissionCompletionIT" -> (new CommandSubmissionCompletion(_)),
    "CommandDeduplicationIT" -> (new CommandDeduplication(_)),
    "ContractKeysIT" -> (new ContractKeys(_)),
    "DivulgenceIT" -> (new Divulgence(_)),
    "HealthServiceIT" -> (new HealthService(_)),
    "IdentityIT" -> (new Identity(_)),
    "LedgerConfigurationServiceIT" -> (new LedgerConfigurationService(_)),
    "PackageManagementServiceIT" -> (new PackageManagement(_)),
    "PackageServiceIT" -> (new Packages(_)),
    "PartyManagementServiceIT" -> (new PartyManagement(_)),
    "SemanticTests" -> (new SemanticTests(_)),
    "TransactionServiceIT" -> (new TransactionService(_)),
    "WitnessesIT" -> (new Witnesses(_)),
    "WronglyTypedContractIdIT" -> (new WronglyTypedContractId(_)),
    "ClosedWorldIT" -> (new ClosedWorld(_)),
  )

  /**
    * These tests can:
    * - change the global state of the ledger, or
    * - be especially resource intensive (by design)
    *
    * These are consequently not run unless otherwise specified.
    */
  val optional: Tests = Map(
    "ConfigManagementServiceIT" -> (new ConfigManagement(_)),
    "LotsOfPartiesIT" -> (new LotsOfParties(_)),
    "TransactionScaleIT" -> (new TransactionScale(_)),
  )

  val all: Tests = default ++ optional

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
          )(_)),
          latencyKey -> (new testtool.tests.PerformanceEnvelope.LatencyTest(
            logger = LoggerFactory.getLogger(latencyKey),
            envelope = envelope,
            reporter = reporter,
          )(_)),
          transactionSizeKey -> (new testtool.tests.PerformanceEnvelope.TransactionSizeScaleTest(
            logger = LoggerFactory.getLogger(transactionSizeKey),
            envelope = envelope,
          )(_)),
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
