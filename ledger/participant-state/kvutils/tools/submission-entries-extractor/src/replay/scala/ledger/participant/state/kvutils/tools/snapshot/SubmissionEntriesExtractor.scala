// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.snapshot

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.export.{
  ProtobufBasedLedgerDataImporter,
  SubmissionInfo,
}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.lf.data._
import com.daml.lf.testing.snapshot.Snapshot

import java.io.BufferedOutputStream
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object SubmissionEntriesExtractor extends App {

  private[this] def decodeSubmissionInfo(submissionInfo: SubmissionInfo) =
    decodeEnvelope(submissionInfo.participantId, submissionInfo.submissionEnvelope)

  private[this] def decodeEnvelope(
      participantId: Ref.ParticipantId,
      envelope: Raw.Envelope,
  ): LazyList[Snapshot.SubmissionEntry] =
    assertRight(Envelope.open(envelope)) match {
      case Envelope.SubmissionMessage(submission) =>
        decodeSubmission(participantId, submission)
      case Envelope.SubmissionBatchMessage(batch) =>
        batch.getSubmissionsList.asScala
          .to(LazyList)
          .map(_.getSubmission)
          .flatMap(submissionEnvelope =>
            decodeEnvelope(participantId, Raw.Envelope(submissionEnvelope))
          )
      case Envelope.LogEntryMessage(_) | Envelope.StateValueMessage(_) =>
        LazyList.empty
    }

  private[this] def decodeSubmission(
      participantId: Ref.ParticipantId,
      submission: DamlSubmission,
  ): LazyList[Snapshot.SubmissionEntry] = {
    submission.getPayloadCase match {
      case DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
        val entry = submission.getTransactionEntry
        LazyList(
          Snapshot.SubmissionEntry
            .newBuilder()
            .setTransaction(
              Snapshot.TransactionEntry
                .newBuilder()
                .setRawTransaction(entry.getRawTransaction)
                .setParticipantId(participantId)
                .addAllSubmitters(entry.getSubmitterInfo.getSubmittersList)
                .setLedgerTime(parseTimestamp(entry.getLedgerEffectiveTime).micros)
                .setSubmissionTime(parseTimestamp(entry.getSubmissionTime).micros)
                .setSubmissionSeed(entry.getSubmissionSeed)
            )
            .build()
        )
      case DamlSubmission.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        val entry = submission.getPackageUploadEntry
        entry.getArchivesList.asScala.iterator
          .map(
            Snapshot.SubmissionEntry
              .newBuilder()
              .setArchives(_)
              .build()
          )
          .to(LazyList)
      case _ =>
        LazyList.empty
    }
  }

  case class Config(
      input: Option[Path] = None,
      output: Option[Path] = None,
  )

  import scopt.OParser

  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("submission-entries-extractor"),
      head("extractor", "1.0"),
      arg[String]("input")
        .action((x, c) => c.copy(input = Some(Paths.get(x))))
        .text("path of the ledger export"),
      opt[String]('o', "output ")
        .action((x, c) => c.copy(output = Some(Paths.get(x))))
        .text("path of the submssion entries file"),
    )
  }

  // OParser.parse returns Option[Config]
  OParser.parse(parser, args, Config()) match {
    case Some(Config(inputFile, outputFile)) =>
      val importer = ProtobufBasedLedgerDataImporter(inputFile.get)
      val output = outputFile match {
        case Some(path) => new BufferedOutputStream(Files.newOutputStream(path))
        case None => scala.sys.process.stdout
      }

      try {
        importer.read().map(_._1).flatMap(decodeSubmissionInfo).foreach {
          _.writeDelimitedTo(output)
        }
      } catch {
        case NonFatal(e) =>
          sys.error("Error:  " + e.getMessage)
      } finally {
        importer.close()
        output.close()
      }
    case None =>
      sys.exit(1)
  }

}
