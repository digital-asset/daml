// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.DataInputStream
import java.time.Instant

import com.daml.ledger.participant.state
import com.google.protobuf.ByteString

/**
  * Enables importing ledger data from an input stream.
  * This class is thread-safe.
  */
final class SerializationBasedLedgerDataImporter(input: DataInputStream)
    extends LedgerDataImporter {

  override def read(): Stream[(SubmissionInfo, WriteSet)] =
    if (input.available() == 0) {
      input.close()
      Stream.empty
    } else {
      deserializeEntry() #:: read()
    }

  private def deserializeEntry(): (SubmissionInfo, WriteSet) = synchronized {
    val submissionInfo = deserializeSubmissionInfo()
    val writeSet = deserializeWriteSet()
    (submissionInfo, writeSet)
  }

  private def deserializeSubmissionInfo(): SubmissionInfo = {
    val correlationId = input.readUTF()
    val submissionEnvelope = readBytes()
    val recordTimeInstant: Instant = readInstant()
    val participantId = input.readUTF()
    SubmissionInfo(
      state.v1.ParticipantId.assertFromString(participantId),
      correlationId,
      submissionEnvelope,
      recordTimeInstant,
    )
  }

  private def deserializeWriteSet(): WriteSet = {
    val numKeyValuePairs = input.readInt()
    (1 to numKeyValuePairs).map { _ =>
      val key = readBytes()
      val value = readBytes()
      key -> value
    }
  }

  private def readBytes(): ByteString = {
    val size = input.readInt()
    val byteArray = new Array[Byte](size)
    input.readFully(byteArray)
    ByteString.copyFrom(byteArray)
  }

  private def readInstant(): Instant = {
    val epochSecond = input.readLong()
    val nano = input.readInt()
    Instant.ofEpochSecond(epochSecond, nano.toLong)
  }

}
