// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{DataInputStream, DataOutputStream}
import java.time.Instant

import com.daml.ledger.participant.state
import com.daml.ledger.participant.state.kvutils.export.FileBasedLedgerDataExporter.{
  SubmissionInfo,
  WriteSet
}
import com.google.protobuf.ByteString

object Serialization {
  def serializeEntry(
      submissionInfo: SubmissionInfo,
      writeSet: WriteSet,
      out: DataOutputStream): Unit = {
    serializeSubmissionInfo(submissionInfo, out)
    serializeWriteSet(writeSet, out)
  }

  def readEntry(input: DataInputStream): (SubmissionInfo, WriteSet) = {
    val submissionInfo = readSubmissionInfo(input)
    val writeSet = readWriteSet(input)
    (submissionInfo, writeSet)
  }

  private def serializeSubmissionInfo(
      submissionInfo: SubmissionInfo,
      out: DataOutputStream): Unit = {
    out.writeUTF(submissionInfo.correlationId)
    out.writeInt(submissionInfo.submissionEnvelope.size())
    submissionInfo.submissionEnvelope.writeTo(out)
    out.writeLong(submissionInfo.recordTimeInstant.getEpochSecond)
    out.writeInt(submissionInfo.recordTimeInstant.getNano)
    out.writeUTF(submissionInfo.participantId)
  }

  private def readSubmissionInfo(input: DataInputStream): SubmissionInfo = {
    val correlationId = input.readUTF()
    val submissionEnvelopeSize = input.readInt()
    val submissionEnvelope = new Array[Byte](submissionEnvelopeSize)
    input.readFully(submissionEnvelope)
    val recordTimeEpochSeconds = input.readLong()
    val recordTimeEpochNanos = input.readInt()
    val participantId = input.readUTF()
    SubmissionInfo(
      ByteString.copyFrom(submissionEnvelope),
      correlationId,
      Instant.ofEpochSecond(recordTimeEpochSeconds, recordTimeEpochNanos.toLong),
      state.v1.ParticipantId.assertFromString(participantId)
    )
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

  private def readWriteSet(input: DataInputStream): WriteSet = {
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
