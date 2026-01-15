// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

import com.digitalasset.transcode.schema.DynamicValue

import scala.annotation.static

/** Codec encodes and decodes target protocol to and from an intermediary
  * [[com.digitalasset.transcode.schema.DynamicValue]] representation. Arbitrary codecs can be
  * composed together to create direct interoperability [[Converter]]s.
  */
trait Codec[A] extends Encoder[A] with Decoder[A] { self =>
  def dimap[B](f: A => B, g: B => A): Codec[B] = new Codec[B] {
    override def fromDynamicValue(dv: DynamicValue): B = f(self.fromDynamicValue(dv))
    override def toDynamicValue(v: B): DynamicValue = self.toDynamicValue(g(v))
  }
}

object Codec {
  @static
  def from[A](encoder: Encoder[A], decoder: Decoder[A]): Codec[A] = new Codec[A] {
    def fromDynamicValue(dv: DynamicValue): A = encoder.fromDynamicValue(dv)
    def toDynamicValue(v: A): DynamicValue = decoder.toDynamicValue(v)
  }
}
