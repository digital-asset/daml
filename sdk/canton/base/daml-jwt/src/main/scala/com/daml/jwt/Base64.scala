// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

private object Base64 extends WithExecuteUnsafe {

  private val defaultEncoder = java.util.Base64.getUrlEncoder

  private val encoderWithoutPadding = java.util.Base64.getUrlEncoder.withoutPadding

  private val defaultDecoder = java.util.Base64.getUrlDecoder

  def encode(bs: Array[Byte]): Either[Error, Array[Byte]] =
    encode(defaultEncoder, bs)

  def encodeWithoutPadding(bs: Array[Byte]): Either[Error, Array[Byte]] =
    encode(encoderWithoutPadding, bs)

  private def encode(
      encoder: java.util.Base64.Encoder,
      bs: Array[Byte],
  ): Either[Error, Array[Byte]] =
    executeUnsafe(encoder.encode(bs), Symbol("Base64.encode"))

  def decode(base64str: String): Either[Error, String] =
    executeUnsafe(new String(defaultDecoder.decode(base64str)), Symbol("Base64.decode"))
}
