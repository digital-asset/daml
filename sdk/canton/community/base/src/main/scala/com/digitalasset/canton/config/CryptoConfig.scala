// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.crypto.{
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  HashAlgorithm,
  PbkdfScheme,
  SigningAlgorithmSpec,
  SigningKeySpec,
  SymmetricKeyScheme,
}

final case class CryptoProviderScheme[S](default: S, supported: NonEmpty[Set[S]]) {
  require(supported.contains(default))
}

/** Configures the optional default and allowed schemes of kind S.
  *
  * @param default The optional scheme to use. If none is specified, use the provider's default scheme of kind S.
  * @param allowed The optional allowed schemes to use. If none is specified, all the provider's supported schemes of kind S are allowed.
  */
final case class CryptoSchemeConfig[S](
    default: Option[S] = None,
    allowed: Option[NonEmpty[Set[S]]] = None,
) extends UniformCantonConfigValidation

object CryptoSchemeConfig {
  implicit def cryptoSchemeConfigCantonConfigValidator[S](implicit
      ev: CantonConfigValidator[S]
  ): CantonConfigValidator[CryptoSchemeConfig[S]] =
    CantonConfigValidatorDerivation[CryptoSchemeConfig[S]]
}

/** Stores the configuration of the signing scheme.
  *
  * @param algorithms the algorithm specifications
  * @param keys the key specifications
  */
final case class SigningSchemeConfig(
    algorithms: CryptoSchemeConfig[SigningAlgorithmSpec] = CryptoSchemeConfig(),
    keys: CryptoSchemeConfig[SigningKeySpec] = CryptoSchemeConfig(),
) extends UniformCantonConfigValidation

object SigningSchemeConfig {
  implicit val signingSchemeConfigCantonConfigValidator
      : CantonConfigValidator[SigningSchemeConfig] =
    CantonConfigValidatorDerivation[SigningSchemeConfig]
}

/** Stores the configuration of the encryption scheme.
  *
  * @param algorithms the algorithm specifications
  * @param keys the key specifications
  */
final case class EncryptionSchemeConfig(
    algorithms: CryptoSchemeConfig[EncryptionAlgorithmSpec] = CryptoSchemeConfig(),
    keys: CryptoSchemeConfig[EncryptionKeySpec] = CryptoSchemeConfig(),
) extends UniformCantonConfigValidation

object EncryptionSchemeConfig {
  implicit val encryptionSchemeConfigCantonConfigValidator
      : CantonConfigValidator[EncryptionSchemeConfig] =
    CantonConfigValidatorDerivation[EncryptionSchemeConfig]
}

/** Cryptography configuration.
  * @param provider the crypto provider implementation to use
  * @param signing the signing key scheme configuration
  * @param encryption the encryption scheme configuration
  * @param symmetric the symmetric key scheme configuration
  * @param hash the hash algorithm configuration
  * @param pbkdf the password-based key derivation function configuration
  * @param kms optional support for a KMS
  * @param privateKeyStore private key store configuration to allow for encrypted key storage
  */
final case class CryptoConfig(
    provider: CryptoProvider = CryptoProvider.Jce,
    signing: SigningSchemeConfig = SigningSchemeConfig(),
    encryption: EncryptionSchemeConfig = EncryptionSchemeConfig(),
    symmetric: CryptoSchemeConfig[SymmetricKeyScheme] = CryptoSchemeConfig(),
    hash: CryptoSchemeConfig[HashAlgorithm] = CryptoSchemeConfig(),
    pbkdf: CryptoSchemeConfig[PbkdfScheme] = CryptoSchemeConfig(),
    kms: Option[KmsConfig] = None,
    privateKeyStore: PrivateKeyStoreConfig = PrivateKeyStoreConfig(),
) extends UniformCantonConfigValidation

object CryptoConfig {
  implicit val cryptoConfigValidator: CantonConfigValidator[CryptoConfig] =
    CantonConfigValidatorDerivation[CryptoConfig]
}
