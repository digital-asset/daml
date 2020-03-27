// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.google.protobuf.ByteString

import scala.util.Try
import com.daml.ledger.participant.state.kvutils.{DamlKvutils => Proto}

/** Envelope is a wrapping for "top-level" kvutils messages that provides
  * versioning and compression and should be used when storing or transmitting
  * kvutils messages.
  */
object Envelope {

  sealed trait Message extends Product with Serializable

  final case class SubmissionMessage(submission: Proto.DamlSubmission) extends Message

  final case class LogEntryMessage(logEntry: Proto.DamlLogEntry) extends Message

  final case class StateValueMessage(value: Proto.DamlStateValue) extends Message

  final case class SubmissionBatchMessage(value: Proto.DamlSubmissionBatch) extends Message

  private def enclose(
      kind: Proto.Envelope.MessageKind,
      bytes: ByteString,
      compression: Boolean = true): ByteString =
    Proto.Envelope.newBuilder
      .setVersion(Version.version)
      .setKind(kind)
      .setMessage(if (compression) compress(bytes) else bytes)
      .setCompression(
        if (compression)
          Proto.Envelope.CompressionSchema.GZIP
        else
          Proto.Envelope.CompressionSchema.NONE
      )
      .build
      .toByteString

  def enclose(sub: Proto.DamlSubmission): ByteString = enclose(sub, compression = true)
  def enclose(sub: Proto.DamlSubmission, compression: Boolean): ByteString =
    enclose(Proto.Envelope.MessageKind.SUBMISSION, sub.toByteString, compression)

  def enclose(logEntry: Proto.DamlLogEntry): ByteString = enclose(logEntry, compression = true)
  def enclose(logEntry: Proto.DamlLogEntry, compression: Boolean): ByteString =
    enclose(Proto.Envelope.MessageKind.LOG_ENTRY, logEntry.toByteString, compression)

  def enclose(stateValue: Proto.DamlStateValue): ByteString =
    enclose(stateValue, compression = true)
  def enclose(stateValue: Proto.DamlStateValue, compression: Boolean): ByteString =
    enclose(Proto.Envelope.MessageKind.STATE_VALUE, stateValue.toByteString, compression)

  def enclose(batch: Proto.DamlSubmissionBatch): ByteString =
    enclose(Proto.Envelope.MessageKind.SUBMISSION_BATCH, batch.toByteString, compression = false)

  def open(envelopeBytes: ByteString): Either[String, Message] =
    openWithParser(() => Proto.Envelope.parseFrom(envelopeBytes))

  def open(envelopeBytes: Array[Byte]): Either[String, Message] =
    openWithParser(() => Proto.Envelope.parseFrom(envelopeBytes))

  private def openWithParser(parseEnvelope: () => Proto.Envelope): Either[String, Message] =
    for {
      envelope <- Try(parseEnvelope()).toEither.left.map(_.getMessage)
      _ <- Either.cond(
        envelope.getVersion == Version.version,
        (),
        s"Unsupported version ${envelope.getVersion}")
      uncompressedMessage <- envelope.getCompression match {
        case Proto.Envelope.CompressionSchema.GZIP =>
          parseMessageSafe(() => decompress(envelope.getMessage))
        case Proto.Envelope.CompressionSchema.NONE =>
          Right(envelope.getMessage)
        case Proto.Envelope.CompressionSchema.UNRECOGNIZED =>
          Left(s"Unrecognized compression schema: ${envelope.getCompressionValue}")
      }
      message <- envelope.getKind match {
        case Proto.Envelope.MessageKind.LOG_ENTRY =>
          parseMessageSafe(() => Proto.DamlLogEntry.parseFrom(uncompressedMessage)).right
            .map(LogEntryMessage)
        case Proto.Envelope.MessageKind.SUBMISSION =>
          parseMessageSafe(() => Proto.DamlSubmission.parseFrom(uncompressedMessage)).right
            .map(SubmissionMessage)
        case Proto.Envelope.MessageKind.STATE_VALUE =>
          parseMessageSafe(() => Proto.DamlStateValue.parseFrom(uncompressedMessage)).right
            .map(StateValueMessage)
        case Proto.Envelope.MessageKind.SUBMISSION_BATCH =>
          parseMessageSafe(() => Proto.DamlSubmissionBatch.parseFrom(uncompressedMessage)).right
            .map(SubmissionBatchMessage)
        case Proto.Envelope.MessageKind.UNRECOGNIZED =>
          Left(s"Unrecognized message kind: ${envelope.getKind}")
      }
    } yield message

  def openLogEntry(envelopeBytes: ByteString): Either[String, Proto.DamlLogEntry] =
    open(envelopeBytes).flatMap {
      case LogEntryMessage(entry) => Right(entry)
      case msg => Left(s"Expected log entry, got ${msg.getClass}")
    }

  def openSubmission(envelopeBytes: ByteString): Either[String, Proto.DamlSubmission] =
    open(envelopeBytes).flatMap {
      case SubmissionMessage(entry) => Right(entry)
      case msg => Left(s"Expected submission, got ${msg.getClass}")
    }

  def openSubmission(envelopeBytes: Array[Byte]): Either[String, Proto.DamlSubmission] =
    open(envelopeBytes).flatMap {
      case SubmissionMessage(entry) => Right(entry)
      case msg => Left(s"Expected submission, got ${msg.getClass}")
    }

  def openStateValue(envelopeBytes: ByteString): Either[String, Proto.DamlStateValue] =
    open(envelopeBytes).flatMap {
      case StateValueMessage(entry) => Right(entry)
      case msg => Left(s"Expected state value, got ${msg.getClass}")
    }

  def openStateValue(envelopeBytes: Array[Byte]): Either[String, Proto.DamlStateValue] =
    open(envelopeBytes).flatMap {
      case StateValueMessage(entry) => Right(entry)
      case msg => Left(s"Expected state value, got ${msg.getClass}")
    }

  private def compress(payload: ByteString): ByteString = {
    val out = ByteString.newOutput
    val gzipOut = new GZIPOutputStream(out)
    gzipOut.write(payload.toByteArray)
    gzipOut.close()
    out.toByteString
  }

  private def decompress(payload: ByteString): ByteString = {
    val gzipIn = new GZIPInputStream(payload.newInput)
    ByteString.readFrom(gzipIn)
  }

  private def parseMessageSafe[T](callParser: () => T): Either[String, T] =
    Try(callParser()).toEither.left
      .map(_.getMessage)

}
