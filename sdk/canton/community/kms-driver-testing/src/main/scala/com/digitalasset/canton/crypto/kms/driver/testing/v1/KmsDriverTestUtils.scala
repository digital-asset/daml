// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.testing.v1

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto
import com.digitalasset.canton.crypto.kms.driver.api.v1.{
  EncryptionAlgoSpec,
  EncryptionKeySpec,
  PublicKey,
  SigningAlgoSpec,
  SigningKeySpec,
}
import com.digitalasset.canton.crypto.provider.jce.JcePureCrypto
import com.digitalasset.canton.crypto.{
  CryptoKeyFormat,
  EncryptionPublicKey,
  HashAlgorithm,
  PbkdfScheme,
  SigningKeyUsage,
  SigningPublicKey,
  SymmetricKeyScheme,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.google.protobuf.ByteString
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.Security

object KmsDriverTestUtils {

  implicit val transformerEncryptionAlgoSpec
      : Transformer[EncryptionAlgoSpec, crypto.EncryptionAlgorithmSpec] = {
    case EncryptionAlgoSpec.RsaEsOaepSha256 => crypto.EncryptionAlgorithmSpec.RsaOaepSha256
  }

  val supportedSigningKeySpecsByAlgoSpec: Map[SigningAlgoSpec, SigningKeySpec] = Map(
    SigningAlgoSpec.EcDsaSha256 -> SigningKeySpec.EcP256,
    SigningAlgoSpec.EcDsaSha384 -> SigningKeySpec.EcP384,
  )

  val supportedEncryptionKeySpecsByAlgoSpec: Map[EncryptionAlgoSpec, EncryptionKeySpec] = Map(
    EncryptionAlgoSpec.RsaEsOaepSha256 -> EncryptionKeySpec.Rsa2048
  )

  def newPureCrypto(
      supportedDriverSigningAlgoSpecs: Set[SigningAlgoSpec],
      supportedDriverEncryptionAlgoSpecs: Set[EncryptionAlgoSpec],
  ): JcePureCrypto = {

    // Register BC as security provider, typically done by the crypto factory
    Security.addProvider(new BouncyCastleProvider)

    val supportedCryptoSigningAlgoSpecs =
      NonEmpty
        .from(supportedDriverSigningAlgoSpecs.map(_.transformInto[crypto.SigningAlgorithmSpec]))
        .getOrElse(sys.error("no supported signing algo specs"))

    val supportedCryptoEncryptionAlgoSpecs =
      NonEmpty
        .from(
          supportedDriverEncryptionAlgoSpecs.map(_.transformInto[crypto.EncryptionAlgorithmSpec])
        )
        .getOrElse(sys.error("no supported encryption algo specs"))

    new JcePureCrypto(
      defaultSymmetricKeyScheme = SymmetricKeyScheme.Aes128Gcm,
      defaultSigningAlgorithmSpec = supportedCryptoSigningAlgoSpecs.head1,
      supportedSigningAlgorithmSpecs = supportedCryptoSigningAlgoSpecs,
      defaultEncryptionAlgorithmSpec = supportedCryptoEncryptionAlgoSpecs.head1,
      supportedEncryptionAlgorithmSpecs = supportedCryptoEncryptionAlgoSpecs,
      defaultHashAlgorithm = HashAlgorithm.Sha256,
      defaultPbkdfScheme = PbkdfScheme.Argon2idMode1,
      loggerFactory = NamedLoggerFactory.root,
    )
  }

  def signingPublicKey(
      publicKey: PublicKey,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): SigningPublicKey = {
    val key = ByteString.copyFrom(publicKey.key)
    val spec = publicKey.spec match {
      case spec: SigningKeySpec => spec.transformInto[crypto.SigningKeySpec]
      case _: EncryptionKeySpec => sys.error("public key is not a signing public key")
    }
    SigningPublicKey(CryptoKeyFormat.DerX509Spki, key, spec, usage)()
  }

  def encryptionPublicKey(publicKey: PublicKey): EncryptionPublicKey = {
    val key = ByteString.copyFrom(publicKey.key)
    val spec = publicKey.spec match {
      case spec: EncryptionKeySpec => spec.transformInto[crypto.EncryptionKeySpec]
      case _: SigningKeySpec => sys.error("public key is not an encryption public key")
    }
    EncryptionPublicKey(CryptoKeyFormat.DerX509Spki, key, spec)()
  }

}
