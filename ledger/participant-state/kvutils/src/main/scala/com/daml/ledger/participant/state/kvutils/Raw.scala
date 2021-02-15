// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlLogEntryId, DamlStateKey}
import com.google.protobuf.ByteString

import scala.language.implicitConversions

object Raw {

  trait Bytes {
    def bytes: ByteString

    final def size: Long = bytes.size.toLong
  }

  object Bytes {

    /** This implicit conversion exists to aid in migration.
      * It will be deprecated and subsequently removed in the future.
      */
    implicit def `ByteString to Raw.Bytes`(bytes: ByteString): Bytes = Unknown(bytes)

    /** This implicit conversion exists to aid in migration.
      * It will be deprecated and subsequently removed in the future.
      */
    implicit def `Raw.Bytes to ByteString`(raw: Bytes): ByteString = raw.bytes
  }

  trait Companion[Self] {

    val empty: Self = apply(ByteString.EMPTY)

    def apply(bytes: ByteString): Self

    /** This implicit conversion exists to aid in migration.
      * It will be deprecated and subsequently removed in the future.
      */
    implicit def `ByteString to Self`(bytes: ByteString): Self = apply(bytes)

    /** This implicit conversion exists to aid in migration.
      * It will be deprecated and subsequently removed in the future.
      */
    implicit def `Raw.Bytes to Self`(rawBytes: Bytes): Self = apply(rawBytes.bytes)

  }

  /** This case class only exists to preserve some functionality of
    * [[com.daml.ledger.participant.state.kvutils.Bytes]].
    *
    * It will be removed in the future.
    */
  private final case class Unknown(override val bytes: ByteString) extends Bytes

  trait Key extends Bytes

  object Key extends Companion[Key] {
    def apply(bytes: ByteString): Key =
      UnknownKey(bytes)

    implicit val `Key Ordering`: Ordering[Key] =
      Ordering.by(_.bytes.asReadOnlyByteBuffer)
  }

  final case class UnknownKey(override val bytes: ByteString) extends Key

  object UnknownKey extends Companion[UnknownKey]

  final case class LogEntryId(override val bytes: ByteString) extends Key

  object LogEntryId extends Companion[LogEntryId] {
    def apply(logEntryId: DamlLogEntryId): LogEntryId =
      apply(logEntryId.toByteString)
  }

  final case class StateKey(override val bytes: ByteString) extends Key

  object StateKey extends Companion[StateKey] {
    def apply(stateKey: DamlStateKey): StateKey =
      apply(stateKey.toByteString)

    implicit val `StateKey Ordering`: Ordering[StateKey] =
      Key.`Key Ordering`.on(identity)
  }

  trait Value extends Bytes

  object Value extends Companion[Value] {
    def apply(bytes: ByteString): Value =
      Envelope(bytes)
  }

  final case class Envelope(override val bytes: ByteString) extends Value

  object Envelope extends Companion[Envelope] {
    def apply(envelope: DamlKvutils.Envelope): Envelope =
      apply(envelope.toByteString)
  }

  type LogEntry = (LogEntryId, Envelope)

  type StateEntry = (StateKey, Envelope)

}
