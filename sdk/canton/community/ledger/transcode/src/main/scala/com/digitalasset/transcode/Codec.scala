// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

import com.digitalasset.transcode.schema.DynamicValue

/** Codec encodes and decodes target protocol to and from an intermediary
  * [[com.digitalasset.transcode.schema.DynamicValue]] representation. Arbitrary codecs can be composed together to
  * create direct interoperability [[Converter]]s.
  */
trait Codec[A] extends Decoder[A] with Encoder[A]

/** Decodes target protocol representation into intermediary DynamicValue representation. */
trait Decoder[A] {
  def toDynamicValue(v: A): DynamicValue
  def isOptional(): Boolean = false
}

/** Encodes intermediary DynamicValue representation into target protocol representation. */
trait Encoder[A] { def fromDynamicValue(dv: DynamicValue): A }

class UnexpectedFieldsException(val unexpectedFields: Set[String])
    extends Exception(s"Unexpected fields: $unexpectedFields") {}

class MissingFieldException(
    val missingField: String
) extends Exception(s"Missing fields: $missingField") {}
