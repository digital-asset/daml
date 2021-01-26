// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

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

  final case class Key(override val bytes: ByteString) extends Bytes

  object Key extends Companion[Key] {
    implicit val `Key Ordering`: Ordering[Key] = Ordering.by(_.bytes.asReadOnlyByteBuffer)
  }

  final case class Value(override val bytes: ByteString) extends Bytes

  object Value extends Companion[Value]

  type KeyValuePair = (Key, Value)

}
