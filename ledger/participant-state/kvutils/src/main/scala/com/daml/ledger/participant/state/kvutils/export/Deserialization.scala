// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.DataInputStream
import java.time.Instant

import com.daml.ledger.participant.state
import com.google.protobuf.ByteString

object Deserialization {
  def deserializeEntry(input: DataInputStream): (SubmissionInfo, WriteSet) = {
    val submissionInfo = deserializeSubmissionInfo(input)
    val writeSet = deserializeWriteSet(input)
    (submissionInfo, writeSet)
  }

  private def deserializeSubmissionInfo(input: DataInputStream): SubmissionInfo = {
    val correlationId = input.readUTF()
    val submissionEnvelopeSize = input.readInt()
    val submissionEnvelope = new Array[Byte](submissionEnvelopeSize)
    input.readFully(submissionEnvelope)
    val recordTimeEpochSeconds = input.readLong()
    val recordTimeEpochNanos = input.readInt()
    val participantId = input.readUTF()
    SubmissionInfo(
      state.v1.ParticipantId.assertFromString(participantId),
      correlationId,
      ByteString.copyFrom(submissionEnvelope),
      Instant.ofEpochSecond(recordTimeEpochSeconds, recordTimeEpochNanos.toLong),
    )
  }

  private def deserializeWriteSet(input: DataInputStream): WriteSet = {
    val numKeyValuePairs = input.readInt()
    (1 to numKeyValuePairs).map { _ =>
      val keySize = input.readInt()
      val keyBytes = new Array[Byte](keySize)
      input.readFully(keyBytes)
      val valueSize = input.readInt()
      val valueBytes = new Array[Byte](valueSize)
      input.readFully(valueBytes)
      (ByteString.copyFrom(keyBytes), ByteString.copyFrom(valueBytes))
    }
  }
}
