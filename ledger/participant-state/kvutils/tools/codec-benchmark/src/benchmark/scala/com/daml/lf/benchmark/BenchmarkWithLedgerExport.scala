// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.benchmark

import java.nio.file.{Files, Paths}

import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.participant.state.kvutils.export.ProtobufBasedLedgerDataImporter
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.lf.archive.Decode
import com.daml.lf.transaction.TransactionOuterClass
import org.openjdk.jmh.annotations.{Param, Scope, Setup, State}

import scala.jdk.CollectionConverters._

@State(Scope.Benchmark)
abstract class BenchmarkWithLedgerExport {

  @Param(Array(""))
  var ledgerExport: String = _

  protected var submissions: Submissions = _

  @Setup
  def setup(): Unit = {

    if (ledgerExport.isEmpty) {
      ledgerExport = referenceLedgerExportPath
    }

    val source = Paths.get(ledgerExport)

    if (Files.notExists(source)) {
      throw new IllegalArgumentException(s"Ledger export file not found at $ledgerExport")
    }

    val builder = Submissions.newBuilder()

    def decodeEnvelope(envelope: Raw.Envelope): Unit =
      Envelope.open(envelope).fold(sys.error, identity) match {
        case Envelope.SubmissionMessage(submission)
            if submission.getPayloadCase == DamlSubmission.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
          for (archive <- submission.getPackageUploadEntry.getArchivesList.asScala) {
            builder += Decode.assertDecodeArchive(archive)
          }
        case Envelope.SubmissionMessage(submission)
            if submission.getPayloadCase == DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
          builder += TransactionOuterClass.Transaction.parseFrom(
            submission.getTransactionEntry.getRawTransaction
          )
        case Envelope.SubmissionBatchMessage(batch) =>
          for (submission <- batch.getSubmissionsList.asScala) {
            decodeEnvelope(Raw.Envelope(submission.getSubmission))
          }
        case _ =>
          ()
      }

    val importer = ProtobufBasedLedgerDataImporter(source)
    try {
      for ((submissionInfo, _) <- importer.read()) {
        decodeEnvelope(submissionInfo.submissionEnvelope)
      }
    } finally {
      importer.close()
    }

    submissions = builder.result()
  }

  private val relativeReferenceLedgerExportPath: String =
    "ledger/participant-state/kvutils/reference-ledger-export.out"

  private val referenceLedgerExportPath: String =
    BazelRunfiles.rlocation(relativeReferenceLedgerExportPath)

}
