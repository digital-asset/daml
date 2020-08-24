// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{DataOutputStream, FileOutputStream}
import java.time.Instant

import com.daml.ledger.participant.state.kvutils.CorrelationId
import com.daml.ledger.participant.state.v1.ParticipantId
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

trait LedgerDataExporter {

  /**
    * Adds given submission and its parameters to the list of in-progress submissions.
    */
  def addSubmission(
      submissionEnvelope: ByteString,
      correlationId: CorrelationId,
      recordTimeInstant: Instant,
      participantId: ParticipantId,
  ): SubmissionAggregator

  /**
    * Signals that entries for the given top-level (parent) correlation ID may be persisted.
    */
  def finishedProcessing(correlationId: CorrelationId): Unit
}

object LedgerDataExporter {
  val EnvironmentVariableName = "KVUTILS_LEDGER_EXPORT"

  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val outputStreamMaybe: Option[DataOutputStream] = {
    Option(System.getenv(EnvironmentVariableName))
      .map { filename =>
        logger.info(s"Enabled writing ledger entries to $filename")
        new DataOutputStream(new FileOutputStream(filename))
      }
  }

  private lazy val instance = outputStreamMaybe
    .map(new FileBasedLedgerDataExporter(_))
    .getOrElse(NoOpLedgerDataExporter)

  def apply(): LedgerDataExporter = instance
}
