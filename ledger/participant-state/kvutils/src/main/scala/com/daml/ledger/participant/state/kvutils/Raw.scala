// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.store.{DamlLogEntryId, DamlStateKey}
import com.google.protobuf.ByteString

object Raw {

  trait Bytes {
    def bytes: ByteString

    final def size: Long = bytes.size.toLong
  }

  trait Companion[Self] {
    val empty: Self = apply(ByteString.EMPTY)

    def apply(bytes: ByteString): Self
  }

  trait Key extends Bytes

  object Key {
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
    def apply(protoEnvelope: envelope.Envelope): Envelope =
      apply(protoEnvelope.toByteString)
  }

  type LogEntry = (LogEntryId, Envelope)

  type StateEntry = (StateKey, Envelope)

  final case class Transaction(override val bytes: ByteString) extends Bytes

  object Transaction extends Companion[Transaction]

  final case class ContractInstance(override val bytes: ByteString) extends Bytes

  object ContractInstance extends Companion[ContractInstance]
}
