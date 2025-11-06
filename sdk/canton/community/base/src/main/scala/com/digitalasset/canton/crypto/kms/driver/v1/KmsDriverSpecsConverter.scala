// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.v1

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
    case SigningAlgoSpec.Ed25519 => crypto.SigningAlgorithmSpec.Ed25519
    case SigningAlgoSpec.EcDsaSha256 => crypto.SigningAlgorithmSpec.EcDsaSha256
    case SigningAlgoSpec.EcDsaSha384 => crypto.SigningAlgorithmSpec.EcDsaSha384
  }

  def convertToCryptoSigningKeySpec(
      keySpec: SigningKeySpec
  ): crypto.SigningKeySpec = keySpec match {
    case SigningKeySpec.EcCurve25519 => crypto.SigningKeySpec.EcCurve25519
    case SigningKeySpec.EcP256 => crypto.SigningKeySpec.EcP256
    case SigningKeySpec.EcP384 => crypto.SigningKeySpec.EcP384
    case SigningKeySpec.EcSecp256k1 => crypto.SigningKeySpec.EcSecp256k1
  }

  def convertToCryptoEncryptionAlgoSpec(
      algoSpec: EncryptionAlgoSpec
  ): crypto.EncryptionAlgorithmSpec =
    algoSpec match {
      case EncryptionAlgoSpec.EciesHkdfHmacSha256Aes128Cbc =>
        crypto.EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc
      case EncryptionAlgoSpec.RsaEsOaepSha256 => crypto.EncryptionAlgorithmSpec.RsaOaepSha256
    }

  def convertToCryptoEncryptionKeySpec(
      keySpec: EncryptionKeySpec
  ): crypto.EncryptionKeySpec =
    keySpec match {
      case EncryptionKeySpec.EcP256 => crypto.EncryptionKeySpec.EcP256
      case EncryptionKeySpec.Rsa2048 => crypto.EncryptionKeySpec.Rsa2048
    }

  def convertToDriverSigningAlgoSpec(
      scheme: crypto.SigningAlgorithmSpec
  ): SigningAlgoSpec =
    scheme match {
      case crypto.SigningAlgorithmSpec.Ed25519 => SigningAlgoSpec.Ed25519
      case crypto.SigningAlgorithmSpec.EcDsaSha256 => SigningAlgoSpec.EcDsaSha256
      case crypto.SigningAlgorithmSpec.EcDsaSha384 => SigningAlgoSpec.EcDsaSha384
    }

  def convertToDriverSigningKeySpec(
      keySpec: crypto.SigningKeySpec
  ): SigningKeySpec =
    keySpec match {
      case crypto.SigningKeySpec.EcCurve25519 => SigningKeySpec.EcCurve25519
      case crypto.SigningKeySpec.EcP256 => SigningKeySpec.EcP256
      case crypto.SigningKeySpec.EcP384 => SigningKeySpec.EcP384
      case crypto.SigningKeySpec.EcSecp256k1 => SigningKeySpec.EcSecp256k1
    }

  def convertToDriverEncryptionAlgoSpec(
      spec: crypto.EncryptionAlgorithmSpec
  ): EncryptionAlgoSpec = spec match {
    case crypto.EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
      EncryptionAlgoSpec.EciesHkdfHmacSha256Aes128Cbc
    case crypto.EncryptionAlgorithmSpec.RsaOaepSha256 =>
      EncryptionAlgoSpec.RsaEsOaepSha256
  }

  def convertToDriverEncryptionKeySpec(
      spec: crypto.EncryptionKeySpec
  ): EncryptionKeySpec = spec match {
    case crypto.EncryptionKeySpec.EcP256 => EncryptionKeySpec.EcP256
    case crypto.EncryptionKeySpec.Rsa2048 => EncryptionKeySpec.Rsa2048
  }

}
