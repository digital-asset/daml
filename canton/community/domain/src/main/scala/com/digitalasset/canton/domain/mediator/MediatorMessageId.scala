// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.{LfTimestamp, checked}
import com.google.common.annotations.VisibleForTesting

import scala.util.Try

/** Structure serialized into a sequencer message id to convey what the sequencer event means for a mediator
  * between a send and a receipt (witnessing the sequenced event).
  */
private[mediator] sealed trait MediatorMessageId {
  def toMessageId: MessageId
}
// TODO(#15231) remove me with 3.0
private[mediator] object MediatorMessageId {
  @Deprecated(since = "3.0, remove me once we can break compatibility with old proto versions")
  final case class VerdictMessageId(requestId: RequestId) extends MediatorMessageId {
    val toMessageId: MessageId =
      mkMessageId(verdictPrefix)(requestId.unwrap.toLf.micros)
  }

  /** Message ID for mediator heartbeats.
    * @param id value to ensure the message id is unique for the sender (but isn't significant elsewhere)
    */
  final case class MediatorHeartbeatMessageId(id: Long) extends MediatorMessageId {
    val toMessageId: MessageId = mkMessageId(mediatorHeartbeatPrefix)(id)
  }

  /** Message ID for mediator leadership requests.
    * @param id value to ensure the message id is unique for the sender (but isn't significant elsewhere)
    */
  final case class LeadershipEventMessageId(id: Long) extends MediatorMessageId {
    val toMessageId: MessageId = mkMessageId(leadershipRequestPrefix)(id)
  }

  /** all message ids encoded by the mediator message id should start with this prefix */
  private[mediator] val mediatorMessageIdPrefix = "mmid"
  private[mediator] val verdictPrefix = "verdict"
  private[mediator] val mediatorHeartbeatPrefix = "heartbeat"
  private[mediator] val leadershipRequestPrefix = "leadership"
  private[mediator] val separator = ':'

  def apply(messageIdO: Option[MessageId]): Option[Either[String, MediatorMessageId]] =
    messageIdO.flatMap(apply)

  def apply(messageId: MessageId): Option[Either[String, MediatorMessageId]] =
    messageId.unwrap.split(separator).toList match {
      case `mediatorMessageIdPrefix` :: tail =>
        // this message id was almost certainly created by MediatorMessageId so from this point parse or return error
        Some(parseMessageId(tail))
      case _other =>
        // this wasn't created by us so don't attempt to parse it
        None
    }

  @VisibleForTesting
  private[mediator] def mkMessageId(prefix: String)(id: Long): MessageId =
    checked {
      // Total character count: 37
      // - mediatorMessageIdPrefix: 4
      // - prefix: 10
      // - id: 20 (19 digita and a sign)
      // - separators: 3
      MessageId.tryCreate(
        List(mediatorMessageIdPrefix, prefix, id.toString).mkString(separator.toString)
      )
    }

  private def parseMessageId(items: List[String]): Either[String, MediatorMessageId] =
    items match {
      case `verdictPrefix` :: tail => parseVerdictMessageId(tail)
      case `mediatorHeartbeatPrefix` :: tail => parseMediatorHeartbeatMessageId(tail)
      case `leadershipRequestPrefix` :: tail => parseLeadershipRequestMessageId(tail)
      case unknownPrefix :: _ => Left(s"Unknown prefix: $unknownPrefix")
      case Nil => Left("Missing components for mediator message id")
    }

  private def parseVerdictMessageId(items: List[String]): Either[String, VerdictMessageId] =
    items match {
      case Seq(_mediatorUidRaw, requestId) =>
        for {
          microseconds <- Try(requestId.toLong).toEither
            .leftMap(ex => s"Failed to parse request id: ${ex.getMessage}")
          lfTs <- LfTimestamp
            .fromLong(microseconds)
            .leftMap(err => s"Failed to create timestamp from request id: $err")
          requestId = RequestId(CantonTimestamp(lfTs))
        } yield VerdictMessageId(requestId)
      case other =>
        Left(s"Incorrect components to create verdict-message-id: [${other.mkString(",")}]")
    }

  private def parseMediatorHeartbeatMessageId(
      items: List[String]
  ): Either[String, MediatorHeartbeatMessageId] =
    items match {
      case List(mediatorUidRaw, idStr) =>
        for {
          id <- Try(idStr.toLong).toEither.leftMap(ex =>
            s"Failed to parse request id: ${ex.getMessage}"
          )
        } yield MediatorHeartbeatMessageId(id)

      case incorrectFormat =>
        Left(
          s"Unable to parse mediator heartbeat message id: expected two components, founds ${incorrectFormat.size}"
        )
    }

  private def parseLeadershipRequestMessageId(
      items: List[String]
  ): Either[String, LeadershipEventMessageId] =
    items match {
      case List(mediatorUidRaw, idStr) =>
        for {
          id <- Try(idStr.toLong).toEither.leftMap(ex =>
            s"Failed to parse request id: ${ex.getMessage}"
          )
        } yield LeadershipEventMessageId(id)

      case incorrectFormat =>
        Left(
          s"Unable to parse mediator leadership message id: expected two components, founds ${incorrectFormat.size}"
        )
    }
}
