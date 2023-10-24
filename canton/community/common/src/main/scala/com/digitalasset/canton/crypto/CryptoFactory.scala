// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.implicits.showInterpolator
import cats.instances.future.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{
  CommunityCryptoProvider,
  CryptoConfig,
  CryptoProvider,
  CryptoProviderScheme,
  CryptoSchemeConfig,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.CryptoFactory.{
  CryptoStoresAndSchemes,
  selectAllowedEncryptionKeyScheme,
  selectAllowedSigningKeyScheme,
  selectSchemes,
}
import com.digitalasset.canton.crypto.provider.CryptoKeyConverter
import com.digitalasset.canton.crypto.provider.jce.{
  JceJavaConverter,
  JcePrivateCrypto,
  JcePureCrypto,
}
import com.digitalasset.canton.crypto.provider.tink.{
  TinkJavaConverter,
  TinkPrivateCrypto,
  TinkPureCrypto,
}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.store.{CryptoPrivateStore, CryptoPublicStore}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.Security
import scala.concurrent.{ExecutionContext, Future}

trait CryptoFactory {

  def create(
      config: CryptoConfig,
      storage: Storage,
      cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Crypto]

  def createPureCrypto(
      config: CryptoConfig,
      loggerFactory: NamedLoggerFactory,
  ): Either[String, CryptoPureApi] =
    for {
      symmetricKeyScheme <- selectSchemes(config.symmetric, config.provider.symmetric)
        .map(_.default)
      hashAlgorithm <- selectSchemes(config.hash, config.provider.hash).map(_.default)
      requiredSigningKeySchemes <- selectAllowedSigningKeyScheme(config)
      requiredEncryptionKeySchemes <- selectAllowedEncryptionKeyScheme(config)
      jceJavaConverter = new JceJavaConverter(
        requiredSigningKeySchemes,
        requiredEncryptionKeySchemes,
      )
      crypto <- config.provider match {
        case _: CryptoProvider.TinkCryptoProvider =>
          val cryptoKeyConverter = new CryptoKeyConverter(new TinkJavaConverter, jceJavaConverter)
          TinkPureCrypto.create(cryptoKeyConverter, symmetricKeyScheme, hashAlgorithm)
        case _: CryptoProvider.JceCryptoProvider =>
          Right(
            new JcePureCrypto(jceJavaConverter, symmetricKeyScheme, hashAlgorithm, loggerFactory)
          )
        case prov =>
          Left(s"Unsupported crypto provider: $prov")
      }
    } yield crypto

  protected def initStoresAndSelectSchemes(
      config: CryptoConfig,
      storage: Storage,
      cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, CryptoStoresAndSchemes] =
    for {
      cryptoPublicStore <- EitherT.rightT[Future, String](
        CryptoPublicStore.create(storage, releaseProtocolVersion, timeouts, loggerFactory)
      )
      cryptoPrivateStore <- cryptoPrivateStoreFactory
        .create(storage, releaseProtocolVersion, timeouts, loggerFactory, tracerProvider)
        .leftMap(err => show"Failed to create crypto private store: $err")
      symmetricKeyScheme <- selectSchemes(config.symmetric, config.provider.symmetric)
        .map(_.default)
        .toEitherT
      hashAlgorithm <- selectSchemes(config.hash, config.provider.hash).map(_.default).toEitherT
      signingKeyScheme <- selectSchemes(config.signing, config.provider.signing)
        .map(_.default)
        .toEitherT
      encryptionKeyScheme <- selectSchemes(config.encryption, config.provider.encryption)
        .map(_.default)
        .toEitherT
    } yield CryptoStoresAndSchemes(
      cryptoPublicStore,
      cryptoPrivateStore,
      symmetricKeyScheme,
      signingKeyScheme,
      encryptionKeyScheme,
      hashAlgorithm,
    )

}

object CryptoFactory {

  final case class CryptoScheme[S](default: S, allowed: NonEmpty[Set[S]])

  final case class CryptoStoresAndSchemes(
      cryptoPublicStore: CryptoPublicStore,
      cryptoPrivateStore: CryptoPrivateStore,
      symmetricKeyScheme: SymmetricKeyScheme,
      signingKeyScheme: SigningKeyScheme,
      encryptionKeyScheme: EncryptionKeyScheme,
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

  def selectAllowedSigningKeyScheme(
      config: CryptoConfig
  ): Either[String, NonEmpty[Set[SigningKeyScheme]]] =
    selectSchemes(config.signing, config.provider.signing).map(_.allowed)

  def selectAllowedEncryptionKeyScheme(
      config: CryptoConfig
  ): Either[String, NonEmpty[Set[EncryptionKeyScheme]]] =
    selectSchemes(config.encryption, config.provider.encryption).map(_.allowed)

}

class CommunityCryptoFactory extends CryptoFactory {

  def create(
      config: CryptoConfig,
      storage: Storage,
      cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Crypto] =
    for {
      storesAndSchemes <- initStoresAndSelectSchemes(
        config,
        storage,
        cryptoPrivateStoreFactory,
        releaseProtocolVersion,
        timeouts,
        loggerFactory,
        tracerProvider,
      )
      crypto <- config.provider match {
        case CommunityCryptoProvider.Tink =>
          TinkCrypto.create(
            config,
            storesAndSchemes,
            timeouts,
            loggerFactory,
          )
        case CommunityCryptoProvider.Jce =>
          JceCrypto.create(config, storesAndSchemes, timeouts, loggerFactory)
        case prov =>
          EitherT.leftT[Future, Crypto](s"Unsupported crypto provider: $prov")
      }
    } yield crypto
}

object TinkCrypto {

  def create(
      config: CryptoConfig,
      storesAndSchemes: CryptoStoresAndSchemes,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, String, Crypto] =
    for {
      cryptoPrivateStoreExtended <- storesAndSchemes.cryptoPrivateStore.toExtended
        .toRight(
          s"The crypto private store does not implement all the functions necessary " +
            s"for the chosen provider ${config.provider}"
        )
        .toEitherT[Future]
      requiredSigningKeySchemes <- selectAllowedSigningKeyScheme(config).toEitherT[Future]
      requiredEncryptionKeySchemes <- selectAllowedEncryptionKeyScheme(config).toEitherT[Future]
      jceKeyConverter = new JceJavaConverter(
        requiredSigningKeySchemes,
        requiredEncryptionKeySchemes,
      )
      cryptoKeyConverter = new CryptoKeyConverter(new TinkJavaConverter, jceKeyConverter)
      pureCrypto <- TinkPureCrypto
        .create(
          cryptoKeyConverter,
          storesAndSchemes.symmetricKeyScheme,
          storesAndSchemes.hashAlgorithm,
        )
        .toEitherT
      privateCrypto = TinkPrivateCrypto.create(
        pureCrypto,
        storesAndSchemes.signingKeyScheme,
        storesAndSchemes.encryptionKeyScheme,
        cryptoPrivateStoreExtended,
      )
      crypto = new Crypto(
        pureCrypto,
        privateCrypto,
        storesAndSchemes.cryptoPrivateStore,
        storesAndSchemes.cryptoPublicStore,
        new TinkJavaConverter,
        timeouts,
        loggerFactory,
      )
    } yield crypto

}

object JceCrypto {
  def create(
      config: CryptoConfig,
      storesAndSchemes: CryptoStoresAndSchemes,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, String, Crypto] =
    for {
      cryptoPrivateStoreExtended <- storesAndSchemes.cryptoPrivateStore.toExtended
        .toRight(
          s"The crypto private store does not implement all the functions necessary " +
            s"for the chosen provider ${config.provider}"
        )
        .toEitherT[Future]
      _ = Security.addProvider(new BouncyCastleProvider)
      requiredSigningKeySchemes <- selectAllowedSigningKeyScheme(config).toEitherT[Future]
      requiredEncryptionKeySchemes <- selectAllowedEncryptionKeyScheme(config).toEitherT[Future]
      javaKeyConverter = new JceJavaConverter(
        requiredSigningKeySchemes,
        requiredEncryptionKeySchemes,
      )
      pureCrypto =
        new JcePureCrypto(
          javaKeyConverter,
          storesAndSchemes.symmetricKeyScheme,
          storesAndSchemes.hashAlgorithm,
          loggerFactory,
        )
      privateCrypto =
        new JcePrivateCrypto(
          pureCrypto,
          storesAndSchemes.signingKeyScheme,
          storesAndSchemes.encryptionKeyScheme,
          cryptoPrivateStoreExtended,
        )
      crypto = new Crypto(
        pureCrypto,
        privateCrypto,
        storesAndSchemes.cryptoPrivateStore,
        storesAndSchemes.cryptoPublicStore,
        javaKeyConverter,
        timeouts,
        loggerFactory,
      )
    } yield crypto

}
