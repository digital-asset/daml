// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.DataOutputStream

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
    out.writeInt(submissionInfo.submissionEnvelope.size())
    submissionInfo.submissionEnvelope.writeTo(out)
    out.writeLong(submissionInfo.recordTimeInstant.getEpochSecond)
    out.writeInt(submissionInfo.recordTimeInstant.getNano)
    out.writeUTF(submissionInfo.participantId)
  }

  private def serializeWriteSet(writeSet: WriteSet, out: DataOutputStream): Unit = {
    out.writeInt(writeSet.size)
    for ((key, value) <- writeSet.sortBy(_._1.asReadOnlyByteBuffer())) {
      out.writeInt(key.size())
      key.writeTo(out)
      out.writeInt(value.size())
      value.writeTo(out)
    }
  }
}
