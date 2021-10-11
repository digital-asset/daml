// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.daml.ledger.participant.state.kvutils.store.{DamlLogEntry, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{DamlKvutils => Proto, envelope => proto}
import com.google.protobuf.ByteString

import scala.util.Try

/** Envelope is a wrapping for "top-level" kvutils messages that provides
  * versioning and compression and should be used when storing or transmitting
  * kvutils messages.
  */
object Envelope {

  sealed trait Message extends Product with Serializable

  final case class SubmissionMessage(submission: wire.DamlSubmission) extends Message

  final case class LogEntryMessage(logEntry: DamlLogEntry) extends Message

  final case class StateValueMessage(value: DamlStateValue) extends Message

  final case class SubmissionBatchMessage(value: wire.DamlSubmissionBatch) extends Message

  private val DefaultCompression = true

  private def enclose(
      kind: proto.Envelope.MessageKind,
      bytes: ByteString,
      compression: Boolean,
  ): Raw.Envelope =
    Raw.Envelope(
      proto.Envelope.newBuilder
        .setVersion(Version.version)
        .setKind(kind)
        .setMessage(if (compression) compress(bytes) else bytes)
        .setCompression(
          if (compression)
            proto.Envelope.CompressionSchema.GZIP
          else
            proto.Envelope.CompressionSchema.NONE
        )
        .build
    )

  def enclose(sub: wire.DamlSubmission): Raw.Envelope =
    enclose(sub, compression = DefaultCompression)

  def enclose(sub: wire.DamlSubmission, compression: Boolean): Raw.Envelope =
    enclose(proto.Envelope.MessageKind.SUBMISSION, sub.toByteString, compression)

  def enclose(logEntry: DamlLogEntry): Raw.Envelope =
    enclose(logEntry, compression = DefaultCompression)

  def enclose(logEntry: DamlLogEntry, compression: Boolean): Raw.Envelope =
    enclose(proto.Envelope.MessageKind.LOG_ENTRY, logEntry.toByteString, compression)

  def enclose(stateValue: DamlStateValue): Raw.Envelope =
    enclose(stateValue, compression = DefaultCompression)

  def enclose(stateValue: DamlStateValue, compression: Boolean): Raw.Envelope =
    enclose(proto.Envelope.MessageKind.STATE_VALUE, stateValue.toByteString, compression)

  def enclose(batch: wire.DamlSubmissionBatch): Raw.Envelope =
    enclose(proto.Envelope.MessageKind.SUBMISSION_BATCH, batch.toByteString, compression = false)

  def open(envelopeBytes: Raw.Envelope): Either[String, Message] =
    openWithParser(() => proto.Envelope.parseFrom(envelopeBytes.bytes))

  def open(envelopeBytes: Array[Byte]): Either[String, Message] =
    openWithParser(() => proto.Envelope.parseFrom(envelopeBytes))

  private def openWithParser(parseEnvelope: () => proto.Envelope): Either[String, Message] =
    for {
      parsedEnvelope <- Try(parseEnvelope()).toEither.left.map(_.getMessage)
      _ <- Either.cond(
        parsedEnvelope.getVersion == Version.version,
        (),
        s"Unsupported version ${parsedEnvelope.getVersion}",
      )
      uncompressedMessage <- parsedEnvelope.getCompression match {
        case proto.Envelope.CompressionSchema.GZIP =>
          parseMessageSafe(() => decompress(parsedEnvelope.getMessage))
        case proto.Envelope.CompressionSchema.NONE =>
          Right(parsedEnvelope.getMessage)
        case proto.Envelope.CompressionSchema.UNRECOGNIZED =>
          Left(s"Unrecognized compression schema: ${parsedEnvelope.getCompressionValue}")
      }
      message <- parsedEnvelope.getKind match {
        case proto.Envelope.MessageKind.LOG_ENTRY =>
          parseMessageSafe(() => DamlLogEntry.parseFrom(uncompressedMessage))
            .map(LogEntryMessage)
        case proto.Envelope.MessageKind.SUBMISSION =>
          parseMessageSafe(() => wire.DamlSubmission.parseFrom(uncompressedMessage))
            .map(SubmissionMessage)
        case proto.Envelope.MessageKind.STATE_VALUE =>
          parseMessageSafe(() => DamlStateValue.parseFrom(uncompressedMessage))
            .map(StateValueMessage)
        case proto.Envelope.MessageKind.SUBMISSION_BATCH =>
          parseMessageSafe(() => wire.DamlSubmissionBatch.parseFrom(uncompressedMessage))
            .map(SubmissionBatchMessage)
        case proto.Envelope.MessageKind.UNRECOGNIZED =>
          Left(s"Unrecognized message kind: ${parsedEnvelope.getKind}")
      }
    } yield message

  def openLogEntry(envelopeBytes: Raw.Envelope): Either[String, DamlLogEntry] =
    open(envelopeBytes).flatMap {
      case LogEntryMessage(entry) => Right(entry)
      case msg => Left(s"Expected log entry, got ${msg.getClass}")
    }

  def openSubmission(envelopeBytes: Raw.Envelope): Either[String, wire.DamlSubmission] =
    open(envelopeBytes).flatMap {
      case SubmissionMessage(entry) => Right(entry)
      case msg => Left(s"Expected submission, got ${msg.getClass}")
    }

  def openSubmission(envelopeBytes: Array[Byte]): Either[String, wire.DamlSubmission] =
    open(envelopeBytes).flatMap {
      case SubmissionMessage(entry) => Right(entry)
      case msg => Left(s"Expected submission, got ${msg.getClass}")
    }

  def openStateValue(envelopeBytes: Raw.Envelope): Either[String, DamlStateValue] =
    open(envelopeBytes).flatMap {
      case StateValueMessage(entry) => Right(entry)
      case msg => Left(s"Expected state value, got ${msg.getClass}")
    }

  def openStateValue(envelopeBytes: Array[Byte]): Either[String, DamlStateValue] =
    open(envelopeBytes).flatMap {
      case StateValueMessage(entry) => Right(entry)
      case msg => Left(s"Expected state value, got ${msg.getClass}")
    }

  private def compress(payload: ByteString): ByteString = {
    val out = ByteString.newOutput
    val gzipOut = new GZIPOutputStream(out)
    try {
      gzipOut.write(payload.toByteArray)
    } finally {
      gzipOut.close()
    }
    out.toByteString
  }

  private def decompress(payload: ByteString): ByteString = {
    val gzipIn = new GZIPInputStream(payload.newInput)
    try {
      ByteString.readFrom(gzipIn)
    } finally {
      gzipIn.close()
    }
  }

  private def parseMessageSafe[T](callParser: () => T): Either[String, T] =
    Try(callParser()).toEither.left
      .map(_.getMessage)

}
