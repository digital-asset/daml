// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{BufferedInputStream, Closeable, InputStream}
import java.nio.file.{Files, Path}

import com.daml.ledger.participant.state.kvutils.export.LedgerExport.LedgerExportEntry
import com.daml.ledger.participant.state.kvutils.{Conversions, Raw}
import com.daml.lf.data.Ref

import scala.collection.compat.immutable.LazyList
import scala.jdk.CollectionConverters._

final class ProtobufBasedLedgerDataImporter(input: InputStream)
    extends LedgerDataImporter
    with Closeable {

  override def read(): LazyList[(SubmissionInfo, WriteSet)] = {
    header.consumeAndVerify(input)
    readEntries()
  }

  override def close(): Unit = input.close()

  private def readEntries(): LazyList[(SubmissionInfo, WriteSet)] = {
    val builder = LedgerExportEntry.newBuilder
    if (input.synchronized(builder.mergeDelimitedFrom(input))) {
      val entry = builder.build()
      val submissionInfo = parseSubmissionInfo(entry)
      val writeSet = parseWriteSet(entry)
      (submissionInfo -> writeSet) #:: readEntries()
    } else {
      close()
      LazyList.empty
    }
  }

  private def parseSubmissionInfo(entry: LedgerExportEntry): SubmissionInfo = {
    val entrySubmissionInfo = entry.getSubmissionInfo
    SubmissionInfo(
      Ref.ParticipantId.assertFromString(entrySubmissionInfo.getParticipantId),
      entrySubmissionInfo.getCorrelationId,
      Raw.Envelope(entrySubmissionInfo.getSubmissionEnvelope),
      Conversions.parseInstant(entrySubmissionInfo.getRecordTime),
    )
  }

  private def parseWriteSet(entry: LedgerExportEntry): WriteSet =
    entry.getWriteSetList.asScala.view
      .map(writeEntry => Raw.UnknownKey(writeEntry.getKey) -> Raw.Envelope(writeEntry.getValue))
      .toVector

}

object ProtobufBasedLedgerDataImporter {
  def apply(path: Path): ProtobufBasedLedgerDataImporter =
    new ProtobufBasedLedgerDataImporter(new BufferedInputStream(Files.newInputStream(path)))
}
