// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.DataOutputStream
import java.nio.file.{Files, Paths}
import java.time.Instant

import com.daml.ledger.participant.state.kvutils.CorrelationId
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.resources.{Resource, ResourceOwner}
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

trait LedgerDataExporter {
  def addSubmission(
      participantId: ParticipantId,
      correlationId: CorrelationId,
      submissionEnvelope: ByteString,
      recordTimeInstant: Instant,
  ): SubmissionAggregator
}

object LedgerDataExporter {
  val EnvironmentVariableName = "KVUTILS_LEDGER_EXPORT"

  private val logger = LoggerFactory.getLogger(this.getClass)

  object Owner extends ResourceOwner[LedgerDataExporter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[LedgerDataExporter] =
      sys.env
        .get(EnvironmentVariableName)
        .map(Paths.get(_))
        .map { path =>
          logger.info(s"Enabled writing ledger entries to $path.")
          ResourceOwner
            .forCloseable(() => new DataOutputStream(Files.newOutputStream(path)))
            .acquire()
            .map(new FileBasedLedgerDataExporter(_))
        }
        .getOrElse(Resource.successful(NoOpLedgerDataExporter))
  }

}
