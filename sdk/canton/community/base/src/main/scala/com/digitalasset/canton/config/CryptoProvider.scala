// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.crypto.{
  CryptoKeyFormat,
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  HashAlgorithm,
  PbkdfScheme,
  SigningAlgorithmSpec,
  SigningKeySpec,
  SymmetricKeyScheme,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.version.ProtocolVersion

sealed trait CryptoProvider extends PrettyPrinting {

  def name: String

  def signingAlgorithms: CryptoProviderScheme[SigningAlgorithmSpec]
  def signingKeys: CryptoProviderScheme[SigningKeySpec]
  def encryptionAlgorithms: CryptoProviderScheme[EncryptionAlgorithmSpec]
  def encryptionKeys: CryptoProviderScheme[EncryptionKeySpec]
  def symmetric: CryptoProviderScheme[SymmetricKeyScheme]
  def hash: CryptoProviderScheme[HashAlgorithm]
  def pbkdf: Option[CryptoProviderScheme[PbkdfScheme]]

  def supportedCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]]

  def supportedCryptoKeyFormatsForProtocol(
      protocolVersion: ProtocolVersion
  ): NonEmpty[Set[CryptoKeyFormat]]

  override protected def pretty: Pretty[CryptoProvider.this.type] = prettyOfString(_.name)
}

object CryptoProvider {

  implicit val cryptoProviderCantonConfigValidator: CantonConfigValidator[CryptoProvider] =
    CantonConfigValidatorDerivation[CryptoProvider]

  case object Jce extends CryptoProvider with UniformCantonConfigValidation {
    override def name: String = "JCE"

    override def signingAlgorithms: CryptoProviderScheme[SigningAlgorithmSpec] =
      CryptoProviderScheme(
        SigningAlgorithmSpec.Ed25519,
        NonEmpty(
          Set,
          SigningAlgorithmSpec.Ed25519,
          SigningAlgorithmSpec.EcDsaSha256,
          SigningAlgorithmSpec.EcDsaSha384,
        ),
      )

    override def signingKeys: CryptoProviderScheme[SigningKeySpec] =
      CryptoProviderScheme(
        SigningKeySpec.EcCurve25519,
        NonEmpty(
          Set,
          SigningKeySpec.EcCurve25519,
          SigningKeySpec.EcP256,
          SigningKeySpec.EcP384,
          SigningKeySpec.EcSecp256k1,
        ),
      )

    override def encryptionAlgorithms: CryptoProviderScheme[EncryptionAlgorithmSpec] =
      CryptoProviderScheme(
        EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm,
        NonEmpty.mk(
          Set,
          EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm,
          EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
          EncryptionAlgorithmSpec.RsaOaepSha256,
        ),
      )

    override def encryptionKeys: CryptoProviderScheme[EncryptionKeySpec] =
      CryptoProviderScheme(
        EncryptionKeySpec.EcP256,
        NonEmpty.mk(
          Set,
          EncryptionKeySpec.EcP256,
          EncryptionKeySpec.Rsa2048,
        ),
      )

    override def symmetric: CryptoProviderScheme[SymmetricKeyScheme] =
      CryptoProviderScheme(
        SymmetricKeyScheme.Aes128Gcm,
        NonEmpty.mk(Set, SymmetricKeyScheme.Aes128Gcm),
      )

    override def hash: CryptoProviderScheme[HashAlgorithm] =
      CryptoProviderScheme(HashAlgorithm.Sha256, NonEmpty.mk(Set, HashAlgorithm.Sha256))

    override def pbkdf: Option[CryptoProviderScheme[PbkdfScheme]] =
      Some(
        CryptoProviderScheme(PbkdfScheme.Argon2idMode1, NonEmpty.mk(Set, PbkdfScheme.Argon2idMode1))
      )

    override def supportedCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]] =
      NonEmpty(
        Set,
        CryptoKeyFormat.Raw,
        CryptoKeyFormat.DerX509Spki,
        CryptoKeyFormat.DerPkcs8Pki,
      )

    override def supportedCryptoKeyFormatsForProtocol(
        protocolVersion: ProtocolVersion
    ): NonEmpty[Set[CryptoKeyFormat]] =
      NonEmpty(
        Set,
        CryptoKeyFormat.Raw,
        CryptoKeyFormat.DerX509Spki,
        CryptoKeyFormat.DerPkcs8Pki,
      )
  }

  /** The KMS crypto provider is based on the JCE crypto provider because the non-signing/encryption part, as well as
    * the public crypto operations (i.e., encrypting, or verifying a signature), are implemented in
    * software using the JCE.
    */
  case object Kms extends CryptoProvider with UniformCantonConfigValidation {
    override def name: String = "KMS"

    override def signingAlgorithms: CryptoProviderScheme[SigningAlgorithmSpec] =
      CryptoProviderScheme(
        SigningAlgorithmSpec.EcDsaSha256,
        NonEmpty.mk(
          Set,
          SigningAlgorithmSpec.EcDsaSha256,
          SigningAlgorithmSpec.EcDsaSha384,
          SigningAlgorithmSpec.Ed25519,
        ),
      )

    override def signingKeys: CryptoProviderScheme[SigningKeySpec] =
      CryptoProviderScheme(
        SigningKeySpec.EcP256,
        NonEmpty.mk(
          Set,
          SigningKeySpec.EcP256,
          SigningKeySpec.EcP384,
          SigningKeySpec.EcSecp256k1,
          // EcCurve25519 is only supported to verify signatures, it does not allow generation of such keys for signing
          SigningKeySpec.EcCurve25519,
        ),
      )

    override def encryptionAlgorithms: CryptoProviderScheme[EncryptionAlgorithmSpec] =
      CryptoProviderScheme(
        EncryptionAlgorithmSpec.RsaOaepSha256,
        NonEmpty.mk(
          Set,
          EncryptionAlgorithmSpec.RsaOaepSha256,
          EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm,
          EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
        ),
      )

    override def encryptionKeys: CryptoProviderScheme[EncryptionKeySpec] =
      CryptoProviderScheme(
        EncryptionKeySpec.Rsa2048,
        NonEmpty.mk(
          Set,
          EncryptionKeySpec.Rsa2048,
          // ECIES schemes only supported for encryption, it does not allow generation of such keys for decryption
          EncryptionKeySpec.EcP256,
        ),
      )

    override def supportedCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]] =
      NonEmpty(
        Set,
        CryptoKeyFormat.Raw,
        CryptoKeyFormat.DerX509Spki,
        CryptoKeyFormat.DerPkcs8Pki,
      )

    override def supportedCryptoKeyFormatsForProtocol(
        protocolVersion: ProtocolVersion
    ): NonEmpty[Set[CryptoKeyFormat]] =
      NonEmpty(
        Set,
        CryptoKeyFormat.Raw,
        CryptoKeyFormat.DerX509Spki,
        CryptoKeyFormat.DerPkcs8Pki,
      )

    override def symmetric: CryptoProviderScheme[SymmetricKeyScheme] = Jce.symmetric

    override def hash: CryptoProviderScheme[HashAlgorithm] = Jce.hash

    override def pbkdf: Option[CryptoProviderScheme[PbkdfScheme]] = Jce.pbkdf
  }
}
