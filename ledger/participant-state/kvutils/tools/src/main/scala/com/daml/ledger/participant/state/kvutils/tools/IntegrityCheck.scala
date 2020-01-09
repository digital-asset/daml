// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools

import java.io.{DataInputStream, FileInputStream}
import java.util.concurrent.TimeUnit

import com.codahale.metrics
import com.daml.ledger.participant.state.kvutils.{DamlKvutils => Proto, _}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Engine

import scala.collection.JavaConverters._
import scala.util.Try

object Helpers {
  def time[T](act: () => T): (Long, T) = {
    val t0 = System.nanoTime()
    val result = act()
    val t1 = System.nanoTime()
    (t1 - t0) -> result
  }
}

object IntegrityCheck extends App {
  if (args.length != 1) {
    println("usage: integrity-check <ledger dump file>")
    println(
      "You can produce a ledger dump on a kvutils ledger by setting KVUTILS_LEDGER_DUMP=/path/to/file")
    sys.exit(1)
  }

  val filename = args(0)
  println(s"Verifying integrity of $filename...")

  val registry = metrics.SharedMetricRegistries.getOrCreate("kvutils")
  // Register JVM related metrics.
  (new metrics.jvm.GarbageCollectorMetricSet).getMetrics.forEach { (k, m) =>
    val _ = registry.register(s"jvm.gc.$k", m)
  }
  (new metrics.jvm.MemoryUsageGaugeSet).getMetrics.forEach { (k, m) =>
    val _ = registry.register(s"jvm.mem.$k", m)
  }

  val ledgerDumpStream: DataInputStream =
    new DataInputStream(new FileInputStream(filename))

  val engine = Engine()
  val defaultConfig = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault
  )
  var state = Map.empty[Proto.DamlStateKey, Proto.DamlStateValue]

  var total_t_commit = 0L
  var total_t_update = 0L
  var size = Try(ledgerDumpStream.readInt()).getOrElse(-1)
  var count = 0
  while (size > 0) {
    count += 1

    val buf = Array.ofDim[Byte](size)
    ledgerDumpStream.readFully(buf, 0, size)

    val entry = Proto.LedgerDumpEntry.parseFrom(buf)
    val logEntry = Envelope.openLogEntry(entry.getLogEntry).right.get
    val expectedOutputState =
      entry.getOutputStateList.asScala
        .map(sp => sp.getStateKey -> Envelope.openStateValue(sp.getStateValue).right.get)
        .toMap
    val submission = Envelope.openSubmission(entry.getSubmission).right.get

    val inputState: Map[Proto.DamlStateKey, Option[Proto.DamlStateValue]] =
      submission.getInputDamlStateList.asScala
        .map(k => k -> state.get(k))
        .toMap

    print(s"verifying ${Pretty.prettyEntryId(entry.getEntryId)}: commit... ")
    val (t_commit, (logEntry2, outputState)) = Helpers.time(
      () =>
        KeyValueCommitting.processSubmission(
          engine,
          entry.getEntryId,
          Conversions.parseTimestamp(logEntry.getRecordTime),
          defaultConfig,
          submission,
          Ref.LedgerString.assertFromString(entry.getParticipantId),
          inputState
      ))
    total_t_commit += t_commit

    // We assert that the resulting log entry is structurally equal to the original.
    // This assumes some degree of forward compatibility. We may need to weaken
    // this assumption at some point, but for now it is desirable to know when
    // forward compatibility is compromised.
    assert(logEntry.equals(logEntry2), "Log entry mismatch")

    // Likewise assertion for the output state.
    assert(outputState.equals(expectedOutputState), "Output state mismatch")

    // We verify that we can produce participant state updates, but only partially
    // verify the contents.
    print("update...")
    val (t_update, updates) =
      Helpers.time(() => KeyValueConsumption.logEntryToUpdate(entry.getEntryId, logEntry))

    logEntry.getPayloadCase match {
      case Proto.DamlLogEntry.PayloadCase.TRANSACTION_ENTRY =>
        assert(updates.head.isInstanceOf[Update.TransactionAccepted])
      case Proto.DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY =>
        assert(updates.head.isInstanceOf[Update.CommandRejected])
      case Proto.DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        assert(updates.head.isInstanceOf[Update.PublicPackageUpload])
      case Proto.DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY =>
        assert(updates.head.isInstanceOf[Update.ConfigurationChanged])
      case Proto.DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY =>
        assert(updates.head.isInstanceOf[Update.ConfigurationChangeRejected])
      case Proto.DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        assert(updates.head.isInstanceOf[Update.PartyAddedToParticipant])
      case Proto.DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY =>
        assert(updates.head.isInstanceOf[Update.PartyAllocationRejected])
      case _ =>
        ()
    }
    println(" ok.")
    total_t_update += t_update

    state = state ++ expectedOutputState
    size = Try(ledgerDumpStream.readInt()).getOrElse(-1)
  }

  // Dump detailed metrics.
  val reporter = metrics.ConsoleReporter
    .forRegistry(metrics.SharedMetricRegistries.getOrCreate("kvutils"))
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build
  reporter.report()

  println(s"Verified $count messages.")
  println(s"processSubmission: ${TimeUnit.NANOSECONDS.toMillis(total_t_commit)}ms total.")
  println(s"logEntryToUpdate: ${TimeUnit.NANOSECONDS.toMillis(total_t_update)}ms total.")

}
