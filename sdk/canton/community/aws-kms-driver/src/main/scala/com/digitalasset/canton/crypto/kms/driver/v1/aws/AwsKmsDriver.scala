// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.v1.aws

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto
import com.digitalasset.canton.crypto.KeyName
import com.digitalasset.canton.crypto.kms.aws.AwsKms
import com.digitalasset.canton.crypto.kms.driver.api.v1.*
import com.digitalasset.canton.crypto.kms.driver.v1.KmsDriverSpecsConverter
import com.digitalasset.canton.crypto.kms.{KmsEncryptionPublicKey, KmsKeyId, KmsSigningPublicKey}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import com.digitalasset.canton.util.{ByteString256, ByteString4096, ByteString6144, EitherTUtil}
import com.google.protobuf.ByteString
import io.opentelemetry.context.Context
import org.slf4j.Logger
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.{ExecutionContext, Future}

final case class AwsKmsDriverConfig(
    region: String,
    multiRegionKey: Boolean = false,
    auditLogging: Boolean = false,
)

class AwsKmsDriverFactory extends KmsDriverFactory {

  override def name: String = "aws-kms"

  override def buildInfo: Option[String] = Some(BuildInfo.version)

  override type ConfigType = AwsKmsDriverConfig

  override type Driver = AwsKmsDriver

  override def configReader: ConfigReader[AwsKmsDriverConfig] = {
    import pureconfig.generic.semiauto.*
    deriveReader[AwsKmsDriverConfig]
  }

  override def configWriter(confidential: Boolean): ConfigWriter[AwsKmsDriverConfig] = {
    import pureconfig.generic.semiauto.*
    deriveWriter[AwsKmsDriverConfig]
  }

  override def create(
      config: AwsKmsDriverConfig,
      loggerFactory: Class[?] => Logger,
      executionContext: ExecutionContext,
  ): AwsKmsDriver = {

    val awsConfig = KmsConfig.Aws(
      region = config.region,
      multiRegionKey = config.multiRegionKey,
      auditLogging = config.auditLogging,
    )

    // Use default timeouts and logger factory for the wrapped KMS
    val awsKms =
      AwsKms
        .create(
          awsConfig,
          ProcessingTimeout(),
          NamedLoggerFactory.root,
          NoReportingTracerProvider,
        )
        .valueOr { err =>
          throw new RuntimeException(s"Failed to create AWS KMS: $err")
        }

    new AwsKmsDriver(awsKms)(executionContext)
  }
}

/** A reference implementation of the KMS Driver API based on the existing internal AWS KMS
  * integration.
  */
class AwsKmsDriver(kms: AwsKms)(implicit ec: ExecutionContext) extends KmsDriver {

  private def mapErr[A](operation: String)(
      result: => EitherT[Future, String, A]
  ): Future[A] =
    EitherTUtil.toFuture(
      result.leftMap(err =>
        KmsDriverException(
          new RuntimeException(s"KMS operation `$operation` failed: $err"),
          // Internally the AwsKms already retries when possible, so only non-retryable errors bubble up here.
          retryable = false,
        )
      )
    )

  // TODO(i18206): Check that connection and permissions are properly configured
  override def health: Future[KmsDriverHealth] = Future.successful(KmsDriverHealth.Ok)

  override def supportedSigningKeySpecs: Set[SigningKeySpec] =
    AwsKms.supportedSigningKeySpecs.forgetNE.map(
      KmsDriverSpecsConverter.convertToDriverSigningKeySpec
    )

  override def supportedSigningAlgoSpecs: Set[SigningAlgoSpec] =
    AwsKms.supportedSigningAlgoSpecs.forgetNE.map(
      KmsDriverSpecsConverter.convertToDriverSigningAlgoSpec
    )

  override def supportedEncryptionKeySpecs: Set[EncryptionKeySpec] =
    AwsKms.supportedEncryptionKeySpecs.forgetNE.map(
      KmsDriverSpecsConverter.convertToDriverEncryptionKeySpec
    )

  override def supportedEncryptionAlgoSpecs: Set[EncryptionAlgoSpec] =
    AwsKms.supportedEncryptionAlgoSpecs.forgetNE.map(
      KmsDriverSpecsConverter.convertToDriverEncryptionAlgoSpec
    )

  override def generateSigningKeyPair(
      signingKeySpec: SigningKeySpec,
      keyName: Option[String],
  )(traceContext: Context): Future[String] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("generate-signing-key") {
        for {
          _ <- EitherTUtil.condUnitET[Future](
            supportedSigningKeySpecs.contains(signingKeySpec),
            s"Unsupported signing key spec: $signingKeySpec",
          )
          keySpec = KmsDriverSpecsConverter.convertToCryptoSigningKeySpec(signingKeySpec)
          name <- keyName.traverse(KeyName.create).toEitherT[Future]
          keyId <- kms
            .generateSigningKeyPair(keySpec, name)
            .map(_.unwrap)
            .leftMap(_.toString)
            .failOnShutdownToAbortException(functionFullName)

        } yield keyId
      }
    }

  override def generateEncryptionKeyPair(
      encryptionKeySpec: EncryptionKeySpec,
      keyName: Option[String],
  )(traceContext: Context): Future[String] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("generate-encryption-key") {
        for {
          _ <- EitherTUtil.condUnitET[Future](
            supportedEncryptionKeySpecs.contains(encryptionKeySpec),
            s"Unsupported encryption key spec: $encryptionKeySpec",
          )
          name <- keyName.traverse(KeyName.create).toEitherT[Future]
          keySpec = KmsDriverSpecsConverter.convertToCryptoEncryptionKeySpec(encryptionKeySpec)
          keyId <- kms
            .generateAsymmetricEncryptionKeyPair(keySpec, name)
            .map(_.unwrap)
            .leftMap(_.toString)
            .failOnShutdownToAbortException(functionFullName)
        } yield keyId
      }
    }

  override def generateSymmetricKey(
      keyName: Option[String]
  )(traceContext: Context): Future[String] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("generate symmetric key") {
        for {
          name <- keyName.traverse(KeyName.create).toEitherT[Future]
          keyId <- kms
            .generateSymmetricEncryptionKey(name)
            .map(_.unwrap)
            .leftMap(_.toString)
            .failOnShutdownToAbortException(functionFullName)
        } yield keyId
      }
    }

  override def sign(
      data: Array[Byte],
      keyId: String,
      algoSpec: SigningAlgoSpec,
  )(traceContext: Context): Future[Array[Byte]] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("sign") {
        for {
          kmsKeyId <- KmsKeyId.create(keyId).toEitherT[Future]
          data <- ByteString4096.create(ByteString.copyFrom(data)).toEitherT[Future]
          signingScheme = KmsDriverSpecsConverter.convertToCryptoSigningAlgoSpec(algoSpec)
          signature <- kms
            .sign(
              kmsKeyId,
              data,
              signingScheme,
              // We can hard-code the key spec here as AWS KMS does not use the provided key spec for signing
              crypto.SigningKeySpec.EcP256,
            )
            .leftMap(_.toString)
            .failOnShutdownToAbortException(functionFullName)
        } yield signature.toByteArray
      }
    }

  override def decryptAsymmetric(
      ciphertext: Array[Byte],
      keyId: String,
      algoSpec: EncryptionAlgoSpec,
  )(traceContext: Context): Future[Array[Byte]] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("decrypt-asymmetric") {
        for {
          cipher <- ByteString256.create(ByteString.copyFrom(ciphertext)).toEitherT[Future]
          kmsKeyId <- KmsKeyId.create(keyId).toEitherT[Future]
          encryptSpec = KmsDriverSpecsConverter.convertToCryptoEncryptionAlgoSpec(algoSpec)
          data <- kms
            .decryptAsymmetric(kmsKeyId, cipher, encryptSpec)
            .leftMap(_.toString)
            .failOnShutdownToAbortException(functionFullName)
        } yield data.unwrap.toByteArray
      }
    }

  override def decryptSymmetric(ciphertext: Array[Byte], keyId: String)(
      traceContext: Context
  ): Future[Array[Byte]] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("decrypt-symmetric") {
        for {
          cipher <- ByteString6144.create(ByteString.copyFrom(ciphertext)).toEitherT[Future]
          kmsKeyId <- KmsKeyId.create(keyId).toEitherT[Future]
          decrypted <- kms
            .decryptSymmetric(kmsKeyId, cipher)
            .leftMap(_.toString)
            .failOnShutdownToAbortException(functionFullName)
        } yield decrypted.unwrap.toByteArray
      }
    }

  override def encryptSymmetric(data: Array[Byte], keyId: String)(
      traceContext: Context
  ): Future[Array[Byte]] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("encrypt-symmetric") {
        for {
          plaintext <- ByteString4096.create(ByteString.copyFrom(data)).toEitherT[Future]
          kmsKeyId <- KmsKeyId.create(keyId).toEitherT[Future]
          encrypted <- kms
            .encryptSymmetric(kmsKeyId, plaintext)
            .leftMap(_.toString)
            .failOnShutdownToAbortException(functionFullName)
        } yield encrypted.unwrap.toByteArray
      }
    }

  override def getPublicKey(keyId: String)(traceContext: Context): Future[PublicKey] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("get-public-key") {
        for {
          kmsKeyId <- KmsKeyId.create(keyId).toEitherT[Future]
          publicKey <- kms
            .getPublicKey(kmsKeyId)
            .leftMap(_.toString)
            .subflatMap {
              case KmsEncryptionPublicKey(key, spec) =>
                val convertedSpec = KmsDriverSpecsConverter
                  .convertToDriverEncryptionKeySpec(spec)
                Right(PublicKey(key.toByteArray, convertedSpec))
              case KmsSigningPublicKey(key, spec) =>
                val convertedSpec = KmsDriverSpecsConverter
                  .convertToDriverSigningKeySpec(spec)
                Right(PublicKey(key.toByteArray, convertedSpec))
              case unknownKey => Left(s"Invalid public key: $unknownKey")
            }
            .failOnShutdownToAbortException(functionFullName)
        } yield publicKey
      }
    }

  override def keyExistsAndIsActive(keyId: String)(traceContext: Context): Future[Unit] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("key-exists-and-active") {
        for {
          kmsKeyId <- KmsKeyId.create(keyId).toEitherT[Future]
          _ <- kms
            .keyExistsAndIsActive(kmsKeyId)
            .leftMap(_.toString)
            .failOnShutdownToAbortException(functionFullName)

        } yield ()
      }
    }

  override def deleteKey(keyId: String)(traceContext: Context): Future[Unit] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("delete-key") {
        for {
          kmsKeyId <- KmsKeyId.create(keyId).toEitherT[Future]
          _ <- kms
            .deleteKey(kmsKeyId)
            .leftMap(_.toString)
            .failOnShutdownToAbortException(functionFullName)

        } yield ()
      }
    }

  override def close(): Unit = kms.close()

}
