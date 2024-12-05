// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.nonempty.NonEmpty
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

trait CryptoProvider extends PrettyPrinting {

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
  trait JceCryptoProvider extends CryptoProvider {
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

}

sealed trait CommunityCryptoProvider extends CryptoProvider

object CommunityCryptoProvider {
  case object Jce extends CommunityCryptoProvider with CryptoProvider.JceCryptoProvider
}
