// Copyright (c) 2019 The DAML Authors. All rights reserved.
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

  def enclose(sub: Proto.DamlSubmission): ByteString = enclose(sub, true)
  def enclose(sub: Proto.DamlSubmission, compression: Boolean): ByteString =
    enclose(Proto.Envelope.MessageKind.SUBMISSION, sub.toByteString, compression)

  def enclose(logEntry: Proto.DamlLogEntry): ByteString = enclose(logEntry, true)
  def enclose(logEntry: Proto.DamlLogEntry, compression: Boolean): ByteString =
    enclose(Proto.Envelope.MessageKind.LOG_ENTRY, logEntry.toByteString, compression)

  def enclose(stateValue: Proto.DamlStateValue): ByteString = enclose(stateValue, true)
  def enclose(stateValue: Proto.DamlStateValue, compression: Boolean): ByteString =
    enclose(Proto.Envelope.MessageKind.STATE_VALUE, stateValue.toByteString, compression)

  def open(envelopeBytes: ByteString): Either[String, Message] =
    for {
      envelope <- Try(Proto.Envelope.parseFrom(envelopeBytes)).toEither.left.map(_.getMessage)
      _ <- Either.cond(
        envelope.getVersion == Version.version,
        (),
        s"Unsupported version ${envelope.getVersion}")
      uncompressedMessage <- envelope.getCompression match {
        case Proto.Envelope.CompressionSchema.GZIP =>
          Try(decompress(envelope.getMessage)).toEither.left.map(_.getMessage)
        case Proto.Envelope.CompressionSchema.NONE =>
          Right(envelope.getMessage)
        case Proto.Envelope.CompressionSchema.UNRECOGNIZED =>
          Left(s"Unrecognized compression schema: ${envelope.getCompressionValue}")
      }
      message <- envelope.getKind match {
        case Proto.Envelope.MessageKind.LOG_ENTRY =>
          Try(Proto.DamlLogEntry.parseFrom(uncompressedMessage)).toEither.left
            .map(_.getMessage)
            .right
            .map(LogEntryMessage)
        case Proto.Envelope.MessageKind.SUBMISSION =>
          Try(Proto.DamlSubmission.parseFrom(uncompressedMessage)).toEither.left
            .map(_.getMessage)
            .right
            .map(SubmissionMessage)
        case Proto.Envelope.MessageKind.STATE_VALUE =>
          Try(Proto.DamlStateValue.parseFrom(uncompressedMessage)).toEither.left
            .map(_.getMessage)
            .right
            .map(StateValueMessage)
        case Proto.Envelope.MessageKind.UNRECOGNIZED =>
          Left(s"Unrecognized message kind: ${envelope.getKind} ")
      }
    } yield message

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

}
