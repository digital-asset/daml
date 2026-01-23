// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

trait Converter[A, B] { def convert(a: A): B }
object Converter:
  def apply[A, B](aDecoder: Decoder[A], bEncoder: Encoder[B]): Converter[A, B] =
    a => bEncoder.fromDynamicValue(aDecoder.toDynamicValue(a))
