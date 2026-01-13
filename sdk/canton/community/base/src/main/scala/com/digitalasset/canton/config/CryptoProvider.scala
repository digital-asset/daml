// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  CryptoKeyFormat,
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  HashAlgorithm,
  PbkdfScheme,
  SignatureFormat,
  SigningAlgorithmSpec,
  SigningKeySpec,
  SymmetricKeyScheme,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.version.ProtocolVersion

sealed trait CryptoProvider extends PrettyPrinting {

  def name: String

  // TODO(i23777): make all the crypto parameters PV-dependent
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

  def supportedSignatureFormats: NonEmpty[Set[SignatureFormat]]

  def supportedSignatureFormatsForProtocol(
      protocolVersion: ProtocolVersion
  ): NonEmpty[Set[SignatureFormat]]

  override protected def pretty: Pretty[CryptoProvider.this.type] = prettyOfString(_.name)
}

object CryptoProvider {

  case object Jce extends CryptoProvider {
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
        NonEmpty.mk(
          Set,
          SigningKeySpec.EcCurve25519,
          SigningKeySpec.EcP256,
          SigningKeySpec.EcP384,
          SigningKeySpec.EcSecp256k1,
        ),
      )

    override def encryptionAlgorithms: CryptoProviderScheme[EncryptionAlgorithmSpec] =
      CryptoProviderScheme(
        EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
        NonEmpty.mk(
          Set,
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

    override def supportedSignatureFormats: NonEmpty[Set[SignatureFormat]] =
      NonEmpty(
        Set,
        SignatureFormat.Der,
        SignatureFormat.Concat,
      )

    override def supportedSignatureFormatsForProtocol(
        protocolVersion: ProtocolVersion
    ): NonEmpty[Set[SignatureFormat]] =
      NonEmpty(
        Set,
        SignatureFormat.Der,
        SignatureFormat.Concat,
      )
  }

  /** The KMS crypto provider is based on the JCE crypto provider because the non-signing/encryption
    * part, as well as the public crypto operations (i.e., encrypting, or verifying a signature),
    * are implemented in software using the JCE.
    *
    * We select [[com.digitalasset.canton.crypto.SigningAlgorithmSpec.EcDsaSha256]] and
    * [[com.digitalasset.canton.crypto.EncryptionAlgorithmSpec.RsaOaepSha256]] as the default
    * signing/encryption algorithm specifications for a KMS provider, because some proprietary KMS
    * instances, such as AWS, do not support `Ed25519` or `Ecies` crypto algorithms. The same
    * applies to the key specifications. However, if the chosen KMS supports any of these
    * algorithms, the default scheme can be configured accordingly.
    */
  case object Kms extends CryptoProvider {
    override def name: String = "KMS"

    override def signingAlgorithms: CryptoProviderScheme[SigningAlgorithmSpec] =
      CryptoProviderScheme(
        SigningAlgorithmSpec.EcDsaSha256,
        NonEmpty(
          Set,
          SigningAlgorithmSpec.Ed25519,
          SigningAlgorithmSpec.EcDsaSha256,
          SigningAlgorithmSpec.EcDsaSha384,
        ),
      )

    override def signingKeys: CryptoProviderScheme[SigningKeySpec] =
      CryptoProviderScheme(
        SigningKeySpec.EcP256,
        NonEmpty.mk(
          Set,
          SigningKeySpec.EcCurve25519,
          SigningKeySpec.EcP256,
          SigningKeySpec.EcP384,
          SigningKeySpec.EcSecp256k1,
        ),
      )

    override def encryptionAlgorithms: CryptoProviderScheme[EncryptionAlgorithmSpec] =
      CryptoProviderScheme(
        EncryptionAlgorithmSpec.RsaOaepSha256,
        NonEmpty.mk(
          Set,
          EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
          EncryptionAlgorithmSpec.RsaOaepSha256,
        ),
      )

    override def encryptionKeys: CryptoProviderScheme[EncryptionKeySpec] =
      CryptoProviderScheme(
        EncryptionKeySpec.Rsa2048,
        NonEmpty.mk(
          Set,
          EncryptionKeySpec.EcP256,
          EncryptionKeySpec.Rsa2048,
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

    override def supportedSignatureFormats: NonEmpty[Set[SignatureFormat]] =
      NonEmpty(
        Set,
        SignatureFormat.Der,
        SignatureFormat.Concat,
      )

    override def supportedSignatureFormatsForProtocol(
        protocolVersion: ProtocolVersion
    ): NonEmpty[Set[SignatureFormat]] =
      NonEmpty(
        Set,
        SignatureFormat.Der,
        SignatureFormat.Concat,
      )

    override def symmetric: CryptoProviderScheme[SymmetricKeyScheme] = Jce.symmetric

    override def hash: CryptoProviderScheme[HashAlgorithm] = Jce.hash

    override def pbkdf: Option[CryptoProviderScheme[PbkdfScheme]] = Jce.pbkdf
  }
}
