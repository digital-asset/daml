// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.benchmark

import java.nio.file.Paths

import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.kvutils.`export`.ProtobufBasedLedgerDataImporter
import com.daml.lf.archive.Decode
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations.{Param, Scope, Setup, State}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

@State(Scope.Benchmark)
abstract class BenchmarkWithLedgerExport {

  @Param(Array(""))
  var ledgerExport: String = _

  protected var submissions: Submissions = _

  @Setup
  def setup(): Unit = {

    val source = Paths.get(if (ledgerExport.isEmpty) referenceLedgerExportPath else ledgerExport)

    val builder = Submissions.newBuilder()

    def decodeEnvelope(envelope: ByteString): Unit =
      Envelope.open(envelope).fold(sys.error, identity) match {
        case Envelope.SubmissionMessage(submission)
            if submission.getPayloadCase == DamlSubmission.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
          for (archive <- submission.getPackageUploadEntry.getArchivesList.asScala) {
            builder += Decode.decodeArchive(archive)
          }
        case Envelope.SubmissionMessage(submission)
            if submission.getPayloadCase == DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
          builder += submission.getTransactionEntry.getTransaction
        case Envelope.SubmissionBatchMessage(batch) =>
          for (submission <- batch.getSubmissionsList.asScala) {
            decodeEnvelope(submission.getSubmission)
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
