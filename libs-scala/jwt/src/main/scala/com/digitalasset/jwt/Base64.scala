// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import scalaz.{Show, \/}

private object Base64 {

  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"Base64.Error: ${e.what}, ${e.message}")
  }

  private val defaultEncoder = java.util.Base64.getUrlEncoder

  private val encoderWithoutPadding = java.util.Base64.getUrlEncoder.withoutPadding

  private val defaultDecoder = java.util.Base64.getUrlDecoder

  def encode(bs: Array[Byte]): Error \/ Array[Byte] =
    encode(defaultEncoder, bs)

  def encodeWithoutPadding(bs: Array[Byte]): Error \/ Array[Byte] =
    encode(encoderWithoutPadding, bs)

  private def encode(encoder: java.util.Base64.Encoder, bs: Array[Byte]): Error \/ Array[Byte] =
    \/.attempt(encoder.encode(bs))(e =>
      Error(Symbol("encode"), "Cannot base64 encode a string. Cause: " + e.getMessage)
    )

  def decode(base64str: String): Error \/ String =
    \/.attempt(new String(defaultDecoder.decode(base64str)))(e =>
      Error(Symbol("decode"), "Cannot base64 decode a string. Cause: " + e.getMessage)
    )
}
