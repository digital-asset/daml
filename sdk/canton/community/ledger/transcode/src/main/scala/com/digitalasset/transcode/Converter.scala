// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

trait Converter[A, B] { def convert(a: A): B }
object Converter {
  def apply[A, B](decoder: Decoder[A], encoder: Encoder[B]): Converter[A, B] =
    (a: A) => encoder.fromDynamicValue(decoder.toDynamicValue(a))
}
