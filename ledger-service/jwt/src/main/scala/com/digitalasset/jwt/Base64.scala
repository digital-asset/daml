// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import scalaz.{Show, \/}

private object Base64 {

  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"Base64.Error: ${e.what}, ${e.message}")
  }

  private val defaultEncoder = java.util.Base64.getEncoder

  private val encoderWithoutPadding = java.util.Base64.getEncoder.withoutPadding

  def encode(bs: Array[Byte]): Error \/ Array[Byte] =
    encode(defaultEncoder, bs)

  def encodeWithoutPadding(bs: Array[Byte]): Error \/ Array[Byte] =
    encode(encoderWithoutPadding, bs)

  private def encode(encoder: java.util.Base64.Encoder, bs: Array[Byte]): Error \/ Array[Byte] =
    \/.fromTryCatchNonFatal(encoder.encode(bs))
      .leftMap(e => Error('encode, "Cannot base64 encode a string. Cause: " + e.getMessage))

  def decode(base64str: String): Error \/ String =
    \/.fromTryCatchNonFatal(new String(java.util.Base64.getDecoder.decode(base64str)))
      .leftMap(e => Error('decode, "Cannot base64 decode a string. Cause: " + e.getMessage))
}
