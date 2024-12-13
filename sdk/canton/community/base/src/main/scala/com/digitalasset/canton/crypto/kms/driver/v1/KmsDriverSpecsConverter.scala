// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.v1

import cats.syntax.either.*
import com.digitalasset.canton.crypto
import com.digitalasset.canton.crypto.kms.driver.api.v1.{
  EncryptionAlgoSpec,
  EncryptionKeySpec,
  SigningAlgoSpec,
  SigningKeySpec,
}

object KmsDriverSpecsConverter {

  def convertToCryptoSigningAlgoSpec(
      algoSpec: SigningAlgoSpec
  ): crypto.SigningAlgorithmSpec = algoSpec match {
    case SigningAlgoSpec.EcDsaSha256 => crypto.SigningAlgorithmSpec.EcDsaSha256
    case SigningAlgoSpec.EcDsaSha384 => crypto.SigningAlgorithmSpec.EcDsaSha384
  }

  def convertToCryptoSigningKeySpec(
      keySpec: SigningKeySpec
  ): crypto.SigningKeySpec = keySpec match {
    case SigningKeySpec.EcP256 => crypto.SigningKeySpec.EcP256
    case SigningKeySpec.EcP384 => crypto.SigningKeySpec.EcP384
  }

  def convertToCryptoEncryptionAlgoSpec(
      algoSpec: EncryptionAlgoSpec
  ): crypto.EncryptionAlgorithmSpec = algoSpec match {
    case EncryptionAlgoSpec.RsaEsOaepSha256 => crypto.EncryptionAlgorithmSpec.RsaOaepSha256
  }

  def convertToCryptoEncryptionKeySpec(
      keySpec: EncryptionKeySpec
  ): crypto.EncryptionKeySpec = keySpec match {
    case EncryptionKeySpec.Rsa2048 => crypto.EncryptionKeySpec.Rsa2048
  }

  def convertToDriverSigningAlgoSpec(
      scheme: crypto.SigningAlgorithmSpec
  ): Either[String, SigningAlgoSpec] =
    scheme match {
      case crypto.SigningAlgorithmSpec.Ed25519 => Left(s"$scheme unsupported by KMS drivers")
      case crypto.SigningAlgorithmSpec.EcDsaSha256 => SigningAlgoSpec.EcDsaSha256.asRight
      case crypto.SigningAlgorithmSpec.EcDsaSha384 => SigningAlgoSpec.EcDsaSha384.asRight
    }

  def convertToDriverSigningKeySpec(
      keySpec: crypto.SigningKeySpec
  ): Either[String, SigningKeySpec] =
    keySpec match {
      case crypto.SigningKeySpec.EcCurve25519 => Left(s"$keySpec unsupported by KMS drivers")
      case crypto.SigningKeySpec.EcP256 => SigningKeySpec.EcP256.asRight
      case crypto.SigningKeySpec.EcP384 => SigningKeySpec.EcP384.asRight
    }

  def convertToDriverEncryptionAlgoSpec(
      spec: crypto.EncryptionAlgorithmSpec
  ): Either[String, EncryptionAlgoSpec] = spec match {
    case crypto.EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm |
        crypto.EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
      Left(s"$spec unsupported by KMS drivers")
    case crypto.EncryptionAlgorithmSpec.RsaOaepSha256 =>
      EncryptionAlgoSpec.RsaEsOaepSha256.asRight
  }

  def convertToDriverEncryptionKeySpec(
      spec: crypto.EncryptionKeySpec
  ): Either[String, EncryptionKeySpec] = spec match {
    case crypto.EncryptionKeySpec.EcP256 =>
      Left(s"$spec unsupported by KMS drivers")
    case crypto.EncryptionKeySpec.Rsa2048 =>
      EncryptionKeySpec.Rsa2048.asRight
  }

}
