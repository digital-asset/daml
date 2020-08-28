// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.InputStream

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils.LedgerExportEntry
import com.daml.ledger.participant.state.v1.ParticipantId

import scala.collection.JavaConverters._

final class ProtobufBasedLedgerDataImporter(input: InputStream) extends LedgerDataImporter {
  override def read(): Stream[(SubmissionInfo, WriteSet)] = {
    val builder = LedgerExportEntry.newBuilder
    if (input.synchronized(builder.mergeDelimitedFrom(input))) {
      val entry = builder.build()
      val submissionInfo: SubmissionInfo = parseSubmissionInfo(entry)
      val writeSet = parseWriteSet(entry)
      (submissionInfo -> writeSet) #:: read()
    } else {
      Stream.empty
    }
  }

  private def parseSubmissionInfo(entry: LedgerExportEntry): SubmissionInfo = {
    val entrySubmissionInfo = entry.getSubmissionInfo
    SubmissionInfo(
      ParticipantId.assertFromString(entrySubmissionInfo.getParticipantId),
      entrySubmissionInfo.getCorrelationId,
      entrySubmissionInfo.getSubmissionEnvelope,
      Conversions.parseInstant(entrySubmissionInfo.getRecordTime),
    )
  }

  private def parseWriteSet(entry: LedgerExportEntry): WriteSet =
    entry.getWriteSetList.asScala.view
      .map(writeEntry => writeEntry.getKey -> writeEntry.getValue)
      .toVector
}
