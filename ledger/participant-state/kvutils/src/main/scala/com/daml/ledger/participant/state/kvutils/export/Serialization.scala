package com.daml.ledger.participant.state.kvutils.export

import java.io.{DataInputStream, DataOutputStream}
import java.time.Instant

import com.daml.ledger.participant.state
import com.daml.ledger.participant.state.kvutils.export.LedgerDataExport.{SubmissionInfo, WriteSet}
import com.google.protobuf.ByteString

object Serialization {
  def serializeSubmissionInfo(submissionInfo: SubmissionInfo, out: DataOutputStream): Unit = {
    out.writeUTF(submissionInfo.correlationId)
    out.writeInt(submissionInfo.submissionEnvelope.size())
    submissionInfo.submissionEnvelope.writeTo(out)
    out.writeLong(submissionInfo.recordTimeInstant.toEpochMilli)
    out.writeUTF(submissionInfo.participantId)
  }

  def readSubmissionInfo(input: DataInputStream): SubmissionInfo = {
    val correlationId = input.readUTF()
    val submissionEnvelopeSize = input.readInt()
    val submissionEnvelope = new Array[Byte](submissionEnvelopeSize)
    input.readFully(submissionEnvelope)
    val recordTimeEpochMillis = input.readLong()
    val participantId = input.readUTF()
    SubmissionInfo(
      ByteString.copyFrom(submissionEnvelope),
      correlationId,
      Instant.ofEpochMilli(recordTimeEpochMillis),
      state.v1.ParticipantId.assertFromString(participantId)
    )
  }

  def serializeWriteSet(writeSet: WriteSet, out: DataOutputStream): Unit = {
    out.writeInt(writeSet.size)
    for ((key, value) <- writeSet.sortBy(_._1.asReadOnlyByteBuffer())) {
      out.writeInt(key.size())
      key.writeTo(out)
      out.writeInt(value.size())
      value.writeTo(out)
    }
  }

  def readWriteSet(input: DataInputStream): WriteSet = {
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
