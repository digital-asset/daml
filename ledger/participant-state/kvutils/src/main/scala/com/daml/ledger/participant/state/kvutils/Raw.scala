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

  /** This case class only exists to preserve some functionality of
    * [[com.daml.ledger.participant.state.kvutils.Bytes]].
    *
    * It will be removed in the future.
    */
  final case class Unknown(override val bytes: ByteString) extends Bytes

  final case class Key(override val bytes: ByteString) extends Bytes

  object Key {

    implicit val `Key Ordering`: Ordering[Key] = Ordering.by(_.bytes.asReadOnlyByteBuffer)

    /** This implicit conversion exists to aid in migration.
      * It will be deprecated and subsequently removed in the future.
      */
    implicit def `ByteString to Raw.Key`(bytes: ByteString): Key = Key(bytes)

    /** This implicit conversion exists to aid in migration.
      * It will be deprecated and subsequently removed in the future.
      */
    implicit def `Raw.Bytes to Raw.Key`(rawBytes: Bytes): Key = Key(rawBytes.bytes)
  }

  final case class Value(override val bytes: ByteString) extends Bytes

  object Value {

    /** This implicit conversion exists to aid in migration.
      * It will be deprecated and subsequently removed in the future.
      */
    implicit def `ByteString to Raw.Value`(bytes: ByteString): Value = Value(bytes)

    /** This implicit conversion exists to aid in migration.
      * It will be deprecated and subsequently removed in the future.
      */
    implicit def `Raw.Bytes to Raw.Value`(rawBytes: Bytes): Value = Value(rawBytes.bytes)
  }

  type KeyValuePair = (Key, Value)

}
