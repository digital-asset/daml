// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.DataOutputStream
import java.nio.file.{Files, Paths}

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/** Utilities for debugging kvutils. */
object Debug {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** The ledger dump stream is a gzip-compressed stream of `LedgerDumpEntry` messages prefixed
    * by their size.
    */
  private lazy val optLedgerDumpStream: Option[DataOutputStream] =
    sys.env
      .get("KVUTILS_LEDGER_DUMP")
      .map { filePath =>
        val path = Paths.get(filePath)
        logger.info(s"Enabled writing ledger entries to $path.")
        new DataOutputStream(Files.newOutputStream(path))
      }

  /** Dump ledger entry to disk if dumping is enabled.
    * Ledger dumps are mostly used to test for backwards compatibility of new releases.
    *
    * To enable dumping set the environment variable KVUTILS_LEDGER_DUMP, e.g:
    *   KVUTILS_LEDGER_DUMP=/tmp/my-ledger.dump
    */
  def dumpLedgerEntry(
      submission: DamlSubmission,
      participantId: String,
      entryId: DamlLogEntryId,
      logEntry: DamlLogEntry,
      outputState: Map[DamlStateKey, DamlStateValue]): Unit =
    optLedgerDumpStream.foreach { outs =>
      val dumpEntry = DamlKvutils.LedgerDumpEntry.newBuilder
        .setSubmission(Envelope.enclose(submission))
        .setEntryId(entryId)
        .setParticipantId(participantId)
        .setLogEntry(Envelope.enclose(logEntry))
        .addAllOutputState(
          outputState.map {
            case (k, v) =>
              DamlKvutils.LedgerDumpEntry.StatePair.newBuilder
                .setStateKey(k)
                .setStateValue(Envelope.enclose(v))
                .build
          }.asJava
        )
        .build

      // Messages are delimited by a header containing the message size as int32
      outs.writeInt(dumpEntry.getSerializedSize)
      dumpEntry.writeTo(outs)
      outs.flush()
    }

}
