// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.show.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  CryptoProviderScheme,
  CryptoSchemeConfig,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.kms.KmsFactory
import com.digitalasset.canton.crypto.provider.jce.{JceCrypto, JcePureCrypto}
import com.digitalasset.canton.crypto.provider.kms.{KmsCrypto, KmsPrivateCrypto}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStore,
  CryptoPublicStore,
  KmsCryptoPrivateStore,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

object CryptoFactory {

  final case class CryptoScheme[S](default: S, allowed: NonEmpty[Set[S]])

  final case class CryptoStoresAndSchemes(
      cryptoPublicStore: CryptoPublicStore,
      cryptoPrivateStore: CryptoPrivateStore,
      symmetricKeyScheme: SymmetricKeyScheme,
      // TODO(#18934): Ensure required/allowed schemes are enforced by private/pure crypto classes
      supportedSigningAlgorithmSpecs: NonEmpty[Set[SigningAlgorithmSpec]],
      signingAlgorithmSpec: SigningAlgorithmSpec,
      signingKeySpec: SigningKeySpec,
      supportedEncryptionAlgorithmSpecs: NonEmpty[Set[EncryptionAlgorithmSpec]],
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
      encryptionKeySpec: EncryptionKeySpec,
      hashAlgorithm: HashAlgorithm,
  )

  def selectSchemes[S](
      configured: CryptoSchemeConfig[S],
      provider: CryptoProviderScheme[S],
  ): Either[String, CryptoScheme[S]] = {
    val supported = provider.supported

    // If no allowed schemes are configured, all supported schemes are allowed.
    val allowed = configured.allowed.getOrElse(supported)

    // If no scheme is configured, use the default scheme of the provider
    val default = configured.default.getOrElse(provider.default)

    // The allowed schemes that are not in the supported set
    val unsupported = allowed.diff(supported)

    for {
      _ <- Either.cond(unsupported.isEmpty, (), s"Allowed schemes $unsupported are not supported")
      _ <- Either.cond(allowed.contains(default), (), s"Scheme $default is not allowed: $allowed")
    } yield CryptoScheme(default, allowed)
  }

  def selectAllowedSymmetricKeySchemes(
      config: CryptoConfig
  ): Either[String, NonEmpty[Set[SymmetricKeyScheme]]] =
    selectSchemes(config.symmetric, config.provider.symmetric).map(_.allowed)

  def selectAllowedHashAlgorithms(
      config: CryptoConfig
  ): Either[String, NonEmpty[Set[HashAlgorithm]]] =
    selectSchemes(config.hash, config.provider.hash).map(_.allowed)

  def selectAllowedSigningAlgorithmSpecs(
      config: CryptoConfig
  ): Either[String, NonEmpty[Set[SigningAlgorithmSpec]]] =
    selectSchemes(config.signing.algorithms, config.provider.signingAlgorithms).map(_.allowed)

  def selectAllowedSigningKeySpecs(
      config: CryptoConfig
  ): Either[String, NonEmpty[Set[SigningKeySpec]]] =
    selectSchemes(config.signing.keys, config.provider.signingKeys).map(_.allowed)

  def selectAllowedEncryptionAlgorithmSpecs(
      config: CryptoConfig
  ): Either[String, NonEmpty[Set[EncryptionAlgorithmSpec]]] =
    selectSchemes(config.encryption.algorithms, config.provider.encryptionAlgorithms).map(_.allowed)

  def selectAllowedEncryptionKeySpecs(
      config: CryptoConfig
  ): Either[String, NonEmpty[Set[EncryptionKeySpec]]] =
    selectSchemes(config.encryption.keys, config.provider.encryptionKeys).map(_.allowed)

  private def initStoresAndSelectSchemes(
      config: CryptoConfig,
      storage: Storage,
      cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      tracerProvider: TracerProvider,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, CryptoStoresAndSchemes] =
    for {
      cryptoPublicStore <- CryptoPublicStore
        .create(storage, releaseProtocolVersion, timeouts, loggerFactory)
        .leftMap(err => show"Failed to create crypto public store: $err")
      cryptoPrivateStore <- cryptoPrivateStoreFactory
        .create(storage, releaseProtocolVersion, timeouts, loggerFactory, tracerProvider)
        .leftMap(err => show"Failed to create crypto private store: $err")
      symmetricKeyScheme <- selectSchemes(config.symmetric, config.provider.symmetric)
        .map(_.default)
        .toEitherT[FutureUnlessShutdown]
      hashAlgorithm <- selectSchemes(config.hash, config.provider.hash)
        .map(_.default)
        .toEitherT[FutureUnlessShutdown]
      signingCryptoAlgorithmSpec <- selectSchemes(
        config.signing.algorithms,
        config.provider.signingAlgorithms,
      )
        .map(_.default)
        .toEitherT[FutureUnlessShutdown]
      signingKeySpec <- selectSchemes(config.signing.keys, config.provider.signingKeys)
        .map(_.default)
        .toEitherT[FutureUnlessShutdown]
      supportedSigningAlgorithmSpecs <- selectAllowedSigningAlgorithmSpecs(config)
        .toEitherT[FutureUnlessShutdown]
      encryptionCryptoAlgorithmSpec <- selectSchemes(
        config.encryption.algorithms,
        config.provider.encryptionAlgorithms,
      )
        .map(_.default)
        .toEitherT[FutureUnlessShutdown]
      encryptionKeySpec <- selectSchemes(config.encryption.keys, config.provider.encryptionKeys)
        .map(_.default)
        .toEitherT[FutureUnlessShutdown]
      supportedEncryptionAlgorithmSpecs <- selectAllowedEncryptionAlgorithmSpecs(config)
        .toEitherT[FutureUnlessShutdown]
    } yield CryptoStoresAndSchemes(
      cryptoPublicStore,
      cryptoPrivateStore,
      symmetricKeyScheme,
      supportedSigningAlgorithmSpecs,
      signingCryptoAlgorithmSpec,
      signingKeySpec,
      supportedEncryptionAlgorithmSpecs,
      encryptionCryptoAlgorithmSpec,
      encryptionKeySpec,
      hashAlgorithm,
    )

  @VisibleForTesting
  def createPureCrypto(
      config: CryptoConfig,
      loggerFactory: NamedLoggerFactory,
  ): Either[String, CryptoPureApi] =
    for {
      symmetricKeyScheme <- selectSchemes(config.symmetric, config.provider.symmetric)
        .map(_.default)
      signingAlgorithmSpec <- selectSchemes(
        config.signing.algorithms,
        config.provider.signingAlgorithms,
      )
      supportedSigningAlgorithmSpecs <- selectAllowedSigningAlgorithmSpecs(config)
      encryptionAlgorithmSpec <- selectSchemes(
        config.encryption.algorithms,
        config.provider.encryptionAlgorithms,
      )
      supportedEncryptionAlgorithmSpecs <- selectAllowedEncryptionAlgorithmSpecs(config)
      hashAlgorithm <- selectSchemes(config.hash, config.provider.hash).map(_.default)
      crypto <- config.provider match {
        case CryptoProvider.Jce | CryptoProvider.Kms =>
          for {
            pbkdfSchemes <- config.provider.pbkdf.toRight(
              "PBKDF schemes must be set for JCE provider"
            )
            pbkdfScheme <- selectSchemes(config.pbkdf, pbkdfSchemes).map(_.default)
          } yield new JcePureCrypto(
            symmetricKeyScheme,
            signingAlgorithmSpec.default,
            supportedSigningAlgorithmSpecs,
            encryptionAlgorithmSpec.default,
            supportedEncryptionAlgorithmSpecs,
            hashAlgorithm,
            pbkdfScheme,
            loggerFactory,
          )
      }
    } yield crypto

  def create(
      config: CryptoConfig,
      storage: Storage,
      cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
      kmsFactory: KmsFactory,
      releaseProtocolVersion: ReleaseProtocolVersion,
      nonStandardConfig: Boolean,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
      executionContext: ExecutionContext,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Crypto] =
    for {
      storesAndSchemes <- initStoresAndSelectSchemes(
        config,
        storage,
        cryptoPrivateStoreFactory,
        releaseProtocolVersion,
        timeouts,
        tracerProvider,
        loggerFactory,
      )
      crypto <- config.provider match {
        case CryptoProvider.Jce =>
          JceCrypto
            .create(config, storesAndSchemes, timeouts, loggerFactory)
            .mapK(FutureUnlessShutdown.outcomeK)
        case CryptoProvider.Kms =>
          EitherT.fromEither[FutureUnlessShutdown] {
            for {
              kmsConfig <- config.kms.toRight("Missing KMS configuration for KMS crypto provider")
              kms <- kmsFactory
                .create(
                  kmsConfig,
                  nonStandardConfig,
                  timeouts,
                  futureSupervisor,
                  tracerProvider,
                  clock,
                  loggerFactory,
                  executionContext,
                )
                .leftMap(err => s"Failed to create the KMS client: $err")
              kmsCryptoPrivateStore <- KmsCryptoPrivateStore.fromCryptoPrivateStore(
                storesAndSchemes.cryptoPrivateStore
              )
              kmsPrivateCrypto <- KmsPrivateCrypto.create(
                kms,
                storesAndSchemes,
                kmsCryptoPrivateStore,
                timeouts,
                loggerFactory,
              )
              kmsCrypto <- KmsCrypto.create(
                config,
                storesAndSchemes,
                kmsPrivateCrypto,
                timeouts,
                loggerFactory,
              )
            } yield kmsCrypto
          }
      }
    } yield crypto

}
