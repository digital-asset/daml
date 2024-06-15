// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import better.files.File
import cats.data.EitherT
import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{LocalNodeConfig, TestingConfigInternal}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.GrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStoreError}
import com.digitalasset.canton.environment.CantonNodeBootstrap.HealthDumpFunction
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.metrics.MetricHandle.MetricsFactory
import com.digitalasset.canton.resource.StorageFactory
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

/** When a canton node is created it first has to obtain an identity before most of its services can be started.
  * This process will begin when `start` is called and will try to perform as much as permitted by configuration automatically.
  * If external action is required before this process can complete `start` will return successfully but `isInitialized` will still be false.
  * When the node is successfully initialized the underlying node will be available through `getNode`.
  */
trait CantonNodeBootstrap[+T <: CantonNode] extends FlagCloseable with NamedLogging {

  def name: InstanceName
  def clock: Clock
  def getId: Option[NodeId]
  def isInitialized: Boolean

  def start(): EitherT[Future, String, Unit]

  def getNode: Option[T]

  /** Access to the private and public store to support local key inspection commands */
  def crypto: Crypto

  def isActive: Boolean
}

object CantonNodeBootstrap {
  type HealthDumpFunction = () => Future[File]
}

trait BaseMetrics {
  def prefix: MetricName
  @nowarn("cat=deprecation")
  def metricsFactory: MetricsFactory
  def grpcMetrics: GrpcServerMetrics
  def healthMetrics: HealthMetrics
  def storageMetrics: DbStorageMetrics
}

final case class CantonNodeBootstrapCommonArguments[
    +NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    M <: BaseMetrics,
](
    name: InstanceName,
    config: NodeConfig,
    parameterConfig: ParameterConfig,
    testingConfig: TestingConfigInternal,
    clock: Clock,
    metrics: M,
    storageFactory: StorageFactory,
    cryptoFactory: CryptoFactory,
    cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
    grpcVaultServiceFactory: GrpcVaultServiceFactory,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
    writeHealthDumpToFile: HealthDumpFunction,
    configuredOpenTelemetry: ConfiguredOpenTelemetry,
    tracerProvider: TracerProvider,
)

object CantonNodeBootstrapCommon {

  def getOrCreateSigningKey(crypto: Crypto)(
      name: String
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, String, SigningPublicKey] =
    getOrCreateKey(
      "signing",
      crypto.cryptoPublicStore.findSigningKeyIdByName,
      name => crypto.generateSigningKey(name = name).leftMap(_.toString),
      crypto.cryptoPrivateStore.existsSigningKey,
      name,
    )

  def getOrCreateSigningKeyByFingerprint(crypto: Crypto)(
      fingerprint: Fingerprint
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, String, SigningPublicKey] =
    getKeyByFingerprint(
      "signing",
      crypto.cryptoPublicStore.findSigningKeyIdByFingerprint,
      crypto.cryptoPrivateStore.existsSigningKey,
      fingerprint,
    )

  def getOrCreateEncryptionKey(crypto: Crypto)(
      name: String
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, String, EncryptionPublicKey] =
    getOrCreateKey(
      "encryption",
      crypto.cryptoPublicStore.findEncryptionKeyIdByName,
      name => crypto.generateEncryptionKey(name = name).leftMap(_.toString),
      crypto.cryptoPrivateStore.existsDecryptionKey,
      name,
    )

  private def getKeyByFingerprint[P <: PublicKey](
      typ: String,
      findPubKeyIdByFingerprint: Fingerprint => EitherT[Future, CryptoPublicStoreError, Option[P]],
      existPrivateKeyByFp: Fingerprint => EitherT[Future, CryptoPrivateStoreError, Boolean],
      fingerprint: Fingerprint,
  )(implicit ec: ExecutionContext): EitherT[Future, String, P] = for {
    keyIdO <- findPubKeyIdByFingerprint(fingerprint)
      .leftMap(err =>
        s"Failure while looking for $typ fingerprint $fingerprint in public store: ${err}"
      )
    pubKey <- keyIdO.fold(
      EitherT.leftT[Future, P](s"$typ key with fingerprint $fingerprint does not exist")
    ) { keyWithFingerprint =>
      val fingerprint = keyWithFingerprint.fingerprint
      existPrivateKeyByFp(fingerprint)
        .leftMap(err =>
          s"Failure while looking for $typ key $fingerprint in private key store: $err"
        )
        .transform {
          case Right(true) => Right(keyWithFingerprint)
          case Right(false) =>
            Left(s"Broken private key store: Could not find $typ key $fingerprint")
          case Left(err) => Left(err)
        }
    }
  } yield pubKey

  private def getOrCreateKey[P <: PublicKey](
      typ: String,
      findPubKeyIdByName: KeyName => EitherT[Future, CryptoPublicStoreError, Option[P]],
      generateKey: Option[KeyName] => EitherT[Future, String, P],
      existPrivateKeyByFp: Fingerprint => EitherT[Future, CryptoPrivateStoreError, Boolean],
      name: String,
  )(implicit ec: ExecutionContext): EitherT[Future, String, P] = for {
    keyName <- EitherT.fromEither[Future](KeyName.create(name))
    keyIdO <- findPubKeyIdByName(keyName)
      .leftMap(err => s"Failure while looking for $typ key $name in public store: ${err}")
    pubKey <- keyIdO.fold(
      generateKey(Some(keyName))
        .leftMap(err => s"Failure while generating $typ key for $name: $err")
    ) { keyWithName =>
      val fingerprint = keyWithName.fingerprint
      existPrivateKeyByFp(fingerprint)
        .leftMap(err =>
          s"Failure while looking for $typ key $fingerprint of $name in private key store: $err"
        )
        .transform {
          case Right(true) => Right(keyWithName)
          case Right(false) =>
            Left(s"Broken private key store: Could not find $typ key $fingerprint of $name")
          case Left(err) => Left(err)
        }
    }
  } yield pubKey

}
