// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.v1

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.{ExecutorServiceExtensions, FutureSupervisor, Threading}
import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.kms.driver.api
import com.digitalasset.canton.crypto.kms.driver.api.v1.KmsDriverHealth
import com.digitalasset.canton.crypto.kms.{Kms, KmsError, KmsKeyId}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.driver.v1.{DriverFactoryLoader, DriverLoader}
import com.digitalasset.canton.health.{CloseableAtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.{Clock, PositiveFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.google.protobuf.ByteString
import io.opentelemetry.context.Context

import java.util.concurrent.ExecutorService
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** An internal KMS implementation based on an external KMS driver */
class DriverKms(
    val config: KmsConfig.Driver,
    driver: api.v1.KmsDriver,
    futureSupervisor: FutureSupervisor,
    driverExecutionContext: ExecutorService,
    checkPeriod: PositiveFiniteDuration,
    clock: Clock,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    executionContext: ExecutionContext,
) extends Kms
    with CloseableAtomicHealthComponent
    with HasCloseContext
    with NamedLogging {

  override type Config = KmsConfig.Driver

  lazy val supportedSigningKeySpecs: Set[crypto.SigningKeySpec] =
    driver.supportedSigningKeySpecs.map(KmsDriverSpecsConverter.convertToCryptoSigningKeySpec)

  lazy val supportedSigningAlgoSpecs: Set[crypto.SigningAlgorithmSpec] =
    driver.supportedSigningAlgoSpecs.map(
      KmsDriverSpecsConverter.convertToCryptoSigningAlgoSpec
    )

  lazy val supportedEncryptionKeySpecs: Set[crypto.EncryptionKeySpec] =
    driver.supportedEncryptionKeySpecs.map(KmsDriverSpecsConverter.convertToCryptoEncryptionKeySpec)

  lazy val supportedEncryptionAlgoSpecs: Set[crypto.EncryptionAlgorithmSpec] =
    driver.supportedEncryptionAlgoSpecs.map(
      KmsDriverSpecsConverter.convertToCryptoEncryptionAlgoSpec
    )

  private def monitor[A](
      operation: String,
      errFn: (String, Boolean) => KmsError,
  )(
      driverMethod: => Context => Future[A]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, A] = {

    val mapErr: Throwable => KmsError = {
      case api.v1.KmsDriverException(cause, retryable) =>
        errFn(ErrorUtil.messageWithStacktrace(cause), retryable)
      case ex =>
        throw ex
    }

    logger.trace(s"Calling KMS driver for $operation")

    val requestF =
      try {
        futureSupervisor.supervised(s"KMS Driver: $operation") {
          driverMethod(traceContext.context)
        }
      } catch {
        case NonFatal(e) =>
          // Catch any exception thrown by the driver outside of a failed future and return a failed future
          Future.failed(e)
      }

    EitherTUtil
      .fromFuture(FutureUnlessShutdown.recoverFromAbortException(requestF), mapErr)
      .thereafter {
        case Success(Outcome(Right(_))) =>
          logger.trace(s"KMS driver operation $operation succeeded")
        // We log errors only on info level here because they may be retried by the Kms layer.
        // Only after failing the retries they are logged on warning in the Kms layer.
        case Success(Outcome(Left(err))) =>
          logger.info(s"KMS driver operation $operation failed: $err")
        case Success(AbortedDueToShutdown) =>
          logger.info(s"KMS driver operation $operation aborted due to shutdown")
        case Failure(err) =>
          logger.info(s"KMS driver operation $operation failed", err)
      }
  }

  override protected def generateSigningKeyPairInternal(
      signingKeySpec: crypto.SigningKeySpec,
      name: Option[crypto.KeyName],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      keySpec <- KmsDriverSpecsConverter
        .convertToDriverSigningKeySpec(signingKeySpec)
        .leftMap[KmsError](err => KmsError.KmsCreateKeyError(err, retryable = false))
        .toEitherT[FutureUnlessShutdown]

      keyId <- monitor("generate-signing-key", KmsError.KmsCreateKeyError.apply) {
        driver.generateSigningKeyPair(keySpec, name.map(_.unwrap))
      }

      kmsKeyId <- KmsKeyId
        .create(keyId)
        .leftMap[KmsError](err => KmsError.KmsCreateKeyError(err, retryable = false))
        .toEitherT[FutureUnlessShutdown]
    } yield kmsKeyId

  override protected def generateSymmetricEncryptionKeyInternal(
      name: Option[crypto.KeyName]
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      keyId <- monitor("generate-symmetric-key", KmsError.KmsCreateKeyError.apply) {
        driver.generateSymmetricKey(name.map(_.unwrap))
      }
      kmsKeyId <- KmsKeyId
        .create(keyId)
        .leftMap[KmsError](err => KmsError.KmsCreateKeyError(err, retryable = false))
        .toEitherT[FutureUnlessShutdown]
    } yield kmsKeyId

  override protected def generateAsymmetricEncryptionKeyPairInternal(
      encryptionKeySpec: crypto.EncryptionKeySpec,
      name: Option[crypto.KeyName],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      keySpec <- KmsDriverSpecsConverter
        .convertToDriverEncryptionKeySpec(encryptionKeySpec)
        .leftMap[KmsError](err => KmsError.KmsCreateKeyError(err, retryable = false))
        .toEitherT[FutureUnlessShutdown]
      keyId <- monitor("generate-encryption-key", KmsError.KmsCreateKeyError.apply) {
        driver.generateEncryptionKeyPair(keySpec, name.map(_.unwrap))
      }
      kmsKeyId <- KmsKeyId
        .create(keyId)
        .leftMap[KmsError](err => KmsError.KmsCreateKeyError(err, retryable = false))
        .toEitherT[FutureUnlessShutdown]
    } yield kmsKeyId

  override protected def getPublicSigningKeyInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, crypto.SigningPublicKey] =
    for {
      publicKey <- monitor("get-public-key", KmsError.KmsGetPublicKeyError(keyId, _, _)) {
        driver.getPublicKey(keyId.unwrap)
      }
      spec <- publicKey.spec match {
        case spec: api.v1.SigningKeySpec =>
          KmsDriverSpecsConverter
            .convertToCryptoSigningKeySpec(spec)
            .asRight[KmsError]
            .toEitherT[FutureUnlessShutdown]
        case spec: api.v1.EncryptionKeySpec =>
          EitherT
            .leftT[FutureUnlessShutdown, crypto.SigningKeySpec](
              KmsError.KmsGetPublicKeyError(
                keyId,
                s"Public key $keyId is not a signing public key, returned key spec: $spec",
                retryable = false,
              )
            )
            .leftWiden[KmsError]
      }
      pubKeyRaw = ByteString.copyFrom(publicKey.key)
      key <- crypto.SigningPublicKey
        .create(crypto.CryptoKeyFormat.DerX509Spki, pubKeyRaw, spec)
        .toEitherT[FutureUnlessShutdown]
        .leftMap[KmsError](err => KmsError.KmsGetPublicKeyError(keyId, err.toString))
    } yield key

  override protected def getPublicEncryptionKeyInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, crypto.EncryptionPublicKey] =
    for {
      publicKey <- monitor("get-public-key", KmsError.KmsGetPublicKeyError(keyId, _, _)) {
        driver.getPublicKey(keyId.unwrap)
      }
      spec <- publicKey.spec match {
        case spec: api.v1.EncryptionKeySpec =>
          KmsDriverSpecsConverter
            .convertToCryptoEncryptionKeySpec(spec)
            .asRight[KmsError]
            .toEitherT[FutureUnlessShutdown]
        case spec: api.v1.SigningKeySpec =>
          EitherT
            .leftT[FutureUnlessShutdown, crypto.EncryptionKeySpec](
              KmsError.KmsGetPublicKeyError(
                keyId,
                s"Public key $keyId is not an encryption public key, returned key spec: $spec",
                retryable = false,
              )
            )
            .leftWiden[KmsError]
      }
      key = ByteString.copyFrom(publicKey.key)
    } yield crypto.EncryptionPublicKey(crypto.CryptoKeyFormat.DerX509Spki, key, spec)()

  override protected def keyExistsAndIsActiveInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    monitor("check key exists and active", KmsError.KmsKeyDisabledError(keyId, _, _)) {
      driver.keyExistsAndIsActive(keyId.unwrap)
    }

  override protected def encryptSymmetricInternal(keyId: KmsKeyId, data: ByteString4096)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString6144] =
    for {
      encrypted <- monitor("encrypt-symmetric", KmsError.KmsEncryptError(keyId, _, _)) {
        driver.encryptSymmetric(data.unwrap.toByteArray, keyId.unwrap)
      }
      result <- ByteString6144
        .create(ByteString.copyFrom(encrypted))
        .leftMap[KmsError](err => KmsError.KmsEncryptError(keyId, err, retryable = false))
        .toEitherT[FutureUnlessShutdown]
    } yield result

  override protected def decryptSymmetricInternal(keyId: KmsKeyId, data: ByteString6144)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString4096] =
    for {
      decrypted <- monitor("decrypt-symmetric", KmsError.KmsDecryptError(keyId, _, _)) {
        driver.decryptSymmetric(data.unwrap.toByteArray, keyId.unwrap)
      }
      result <- ByteString4096
        .create(ByteString.copyFrom(decrypted))
        .leftMap[KmsError](err => KmsError.KmsDecryptError(keyId, err, retryable = false))
        .toEitherT[FutureUnlessShutdown]
    } yield result

  override protected def decryptAsymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString256,
      encryptionAlgorithmSpec: crypto.EncryptionAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString190] =
    for {
      algoSpec <-
        KmsDriverSpecsConverter
          .convertToDriverEncryptionAlgoSpec(encryptionAlgorithmSpec)
          .leftMap[KmsError](err => KmsError.KmsDecryptError(keyId, err, retryable = false))
          .toEitherT[FutureUnlessShutdown]
      decrypted <- monitor("decrypt-asymmetric", KmsError.KmsDecryptError(keyId, _, _)) {
        driver.decryptAsymmetric(
          data.unwrap.toByteArray,
          keyId.unwrap,
          algoSpec,
        )
      }
      result <- ByteString190
        .create(ByteString.copyFrom(decrypted))
        .leftMap[KmsError](err => KmsError.KmsDecryptError(keyId, err, retryable = false))
        .toEitherT[FutureUnlessShutdown]
    } yield result

  override protected def signInternal(
      keyId: KmsKeyId,
      data: ByteString4096,
      signingAlgorithmSpec: SigningAlgorithmSpec,
      signingKeySpec: SigningKeySpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString] =
    for {
      algoSpec <- KmsDriverSpecsConverter
        .convertToDriverSigningAlgoSpec(signingAlgorithmSpec)
        .leftMap[KmsError](err => KmsError.KmsSignError(keyId, err, retryable = false))
        .toEitherT[FutureUnlessShutdown]
      signature <- monitor("sign", KmsError.KmsSignError(keyId, _, _)) {
        driver.sign(data.unwrap.toByteArray, keyId.unwrap, algoSpec)
      }
    } yield ByteString.copyFrom(signature)

  override protected def deleteKeyInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    monitor("delete-key", KmsError.KmsDeleteKeyError(keyId, _, _)) {
      driver.deleteKey(keyId.unwrap)
    }

  override def onClosed(): Unit = {
    val driverEc = ExecutorServiceExtensions(driverExecutionContext)(logger, timeouts)
    LifeCycle.close(driver, driverEc)(logger)
  }

  override def name: String = "driver-kms"

  private def checkHealth(now: CantonTimestamp): Unit = {
    implicit val ec: ExecutionContext = executionContext

    TraceContext
      .withNewTraceContext { implicit traceContext =>
        performUnlessClosing(functionFullName) {
          logger.debug(s"Checking health at $now")

          val driverHealthF = driver.health.thereafter {
            case Success(value) =>
              value match {
                case KmsDriverHealth.Ok =>
                  reportHealthState(ComponentHealthState.Ok())
                case KmsDriverHealth.Degraded(reason) =>
                  reportHealthState(ComponentHealthState.degraded(reason))
                case KmsDriverHealth.Failed(reason) =>
                  reportHealthState(ComponentHealthState.failed(reason))
                case KmsDriverHealth.Fatal(reason) =>
                  reportHealthState(ComponentHealthState.fatal(reason))
              }
            case Failure(exception) =>
              reportHealthState(
                ComponentHealthState.failed(ErrorUtil.messageWithStacktrace(exception))
              )
          }

          FutureUtil.doNotAwait(
            // Schedule next health check after check is complete
            driverHealthF.thereafter(_ => scheduleHealthCheck(clock.now)),
            failureMessage = s"Failed to check KMS driver health",
          )
        }
          .onShutdown {
            logger.debug(s"Shutting down, stop storage health check")
          }
      }
  }

  private def scheduleHealthCheck(now: CantonTimestamp)(implicit tc: TraceContext): Unit = {
    val nextCheckTime = now.add(checkPeriod.unwrap)
    logger.debug(s"Scheduling the next health check at $nextCheckTime")
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      clock.scheduleAt(
        checkHealth,
        nextCheckTime,
      ),
      "failed to schedule next health check",
      closeContext = Some(closeContext),
    )
  }

  // Run the initial health check
  checkHealth(clock.now)

  override protected def initialHealthState: ComponentHealthState =
    ComponentHealthState.NotInitializedState

}

object DriverKms {

  def factory(driverName: String): Either[String, api.v1.KmsDriverFactory] =
    DriverFactoryLoader.load[api.v1.KmsDriverFactory, api.KmsDriverFactory](driverName)

  def create(
      config: KmsConfig.Driver,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      executionContext: ExecutionContext,
  ): Either[KmsError, DriverKms] = {

    val driverLogger = loggerFactory.append("kms-driver", config.name).getLogger(classOf[DriverKms])
    val driverExecutionContext =
      Threading.newExecutionContext(s"kms-driver-${config.name}", driverLogger)

    DriverLoader
      .load[api.v1.KmsDriverFactory, api.KmsDriverFactory](
        config.name,
        config.config,
        loggerFactory,
        driverExecutionContext,
      )
      .leftMap(KmsError.KmsCreateClientError.apply)
      .map { driver =>
        new DriverKms(
          config,
          driver,
          futureSupervisor,
          driverExecutionContext,
          config.healthCheckPeriod.toInternal,
          clock,
          timeouts,
          loggerFactory,
          executionContext,
        )
      }
  }
}
