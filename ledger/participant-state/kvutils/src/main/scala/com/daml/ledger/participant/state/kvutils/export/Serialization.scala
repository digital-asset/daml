// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.DataOutputStream
import java.time.Instant

import com.google.protobuf.ByteString

object Serialization {
  def serializeEntry(
      submissionInfo: SubmissionInfo,
      writeSet: WriteSet,
      out: DataOutputStream,
  ): Unit = {
    serializeSubmissionInfo(submissionInfo, out)
    serializeWriteSet(writeSet, out)
  }

  private def serializeSubmissionInfo(
      submissionInfo: SubmissionInfo,
      out: DataOutputStream,
  ): Unit = {
    out.writeUTF(submissionInfo.correlationId)
    writeBytes(submissionInfo.submissionEnvelope, out)
    writeInstant(submissionInfo.recordTimeInstant, out)
    out.writeUTF(submissionInfo.participantId)
  }

  private def serializeWriteSet(writeSet: WriteSet, out: DataOutputStream): Unit = {
    out.writeInt(writeSet.size)
    for ((key, value) <- writeSet.sortBy(_._1.asReadOnlyByteBuffer())) {
      writeBytes(key, out)
      writeBytes(value, out)
    }
  }

  private def writeBytes(bytes: ByteString, out: DataOutputStream): Unit = {
    out.writeInt(bytes.size())
    bytes.writeTo(out)
  }

  private def writeInstant(instant: Instant, out: DataOutputStream): Unit = {
    out.writeLong(instant.getEpochSecond)
    out.writeInt(instant.getNano)
  }
}
