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
    val submissionEnvelope = readBytes(input)
    val recordTimeInstant: Instant = readInstant(input)
    val participantId = input.readUTF()
    SubmissionInfo(
      state.v1.ParticipantId.assertFromString(participantId),
      correlationId,
      submissionEnvelope,
      recordTimeInstant,
    )
  }

  private def deserializeWriteSet(input: DataInputStream): WriteSet = {
    val numKeyValuePairs = input.readInt()
    (1 to numKeyValuePairs).map { _ =>
      val key = readBytes(input)
      val value = readBytes(input)
      key -> value
    }
  }

  private def readBytes(input: DataInputStream): ByteString = {
    val size = input.readInt()
    val byteArray = new Array[Byte](size)
    input.readFully(byteArray)
    ByteString.copyFrom(byteArray)
  }

  private def readInstant(input: DataInputStream) = {
    val epochSecond = input.readLong()
    val nano = input.readInt()
    Instant.ofEpochSecond(epochSecond, nano.toLong)
  }
}
