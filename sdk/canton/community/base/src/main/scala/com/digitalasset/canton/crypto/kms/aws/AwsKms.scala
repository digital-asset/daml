// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.aws

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.kms.KmsError.*
import com.digitalasset.canton.crypto.kms.aws.AwsKms.ExtendedAwsRequestBuilder
import com.digitalasset.canton.crypto.kms.aws.audit.AwsRequestResponseLogger
import com.digitalasset.canton.crypto.kms.aws.audit.AwsRequestResponseLogger.traceContextExecutionAttribute
import com.digitalasset.canton.crypto.kms.aws.tracing.AwsTraceContextInterceptor
import com.digitalasset.canton.crypto.kms.{
  Kms,
  KmsEncryptionPublicKey,
  KmsError,
  KmsKeyId,
  KmsSigningPublicKey,
}
import com.digitalasset.canton.crypto.{
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  KeyName,
  SigningAlgorithmSpec,
  SigningKeySpec,
}
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, TracerProvider}
import com.digitalasset.canton.util.*
import com.google.api.gax.rpc.ResourceExhaustedException
import com.google.protobuf.ByteString
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.awscore.{AwsRequest, AwsRequestOverrideConfiguration}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kms.model.*
import software.amazon.awssdk.services.kms.{KmsAsyncClient, model as aws}
import software.amazon.awssdk.utils.AttributeMap

import java.net.URI
import java.util.concurrent.CompletionException
import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters.*

/** Stands for Amazon Web Services - Key Management Service and is an internal KMS implementation
  * that wraps the necessary cryptographic functions from the AWS SDK.
  */
class AwsKms(
    override val config: KmsConfig.Aws,
    private val kmsClient: KmsAsyncClient,
    httpClientO: Option[SdkAsyncHttpClient],
    override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
) extends Kms
    with NamedLogging {

  override type Config = KmsConfig.Aws

  override def name: String = "aws-kms"

  override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()

  private val errorMessagesToRetry =
    Set(
      "Unable to execute HTTP request: The connection was closed during the request.",
      "Unable to execute HTTP request: connection timed out",
    )

  private def errorHandler(
      err: Throwable,
      kmsErrorGen: (String, Boolean) => KmsError,
  ): KmsError =
    err match {
      case err: CompletionException =>
        Option(err.getCause) match {
          case Some(kmsErr: KmsException) if kmsErr.retryable() =>
            kmsErrorGen(ErrorUtil.messageWithStacktrace(err), true)
          // we look for network failure errors to retry on
          case Some(sdkErr: SdkClientException)
              if errorMessagesToRetry.exists(sdkErr.getMessage.contains(_)) || sdkErr.retryable() =>
            kmsErrorGen(ErrorUtil.messageWithStacktrace(err), true)
          // we retry on resource exceptions as well
          case Some(resourceException: ResourceExhaustedException) =>
            logger.debug(
              s"ResourceExhaustedException with retry: ${resourceException.isRetryable}"
            )(TraceContext.empty)
            kmsErrorGen(ErrorUtil.messageWithStacktrace(err), true)
          case _ =>
            kmsErrorGen(ErrorUtil.messageWithStacktrace(err), false)
        }
      case err => kmsErrorGen(ErrorUtil.messageWithStacktrace(err), true)
    }

  /** Creates an AWS KMS key based on a series of specifications and returns its key identifier.
    *
    * @param keySpec
    *   specifies the type of KMS key to create (e.g. SYMMETRIC_DEFAULT (AES-256-CBC) or RSA_2048).
    * @param keyUsage
    *   the cryptographic operations for which you can use the KMS key (e.g. signing or encryption).
    * @param name
    *   optional name for the KMS key that is currently mapped to the description field.
    * @return
    *   a key id or an error if it fails to create a key
    */
  private def createKey(
      keySpec: aws.KeySpec,
      keyUsage: aws.KeyUsageType,
      name: Option[KeyName],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      keyRequest <-
        Either
          .catchOnly[aws.KmsException] {
            aws.CreateKeyRequest.builder
              .multiRegion(config.multiRegionKey)
              .keySpec(keySpec)
              .keyUsage(keyUsage)
              .tags(Tag.builder().tagKey("CreatedBy").tagValue("Canton").build())
              .description(name.map(_.unwrap).getOrElse(""))
              .withTraceContext(_.overrideConfiguration)
              .build
          }
          .toEitherT[FutureUnlessShutdown]
          .leftMap(err => KmsCreateKeyRequestError(ErrorUtil.messageWithStacktrace(err)))
      response <- EitherTUtil.fromFuture[KmsError, aws.CreateKeyResponse](
        synchronizeWithClosingF(functionFullName)(kmsClient.createKey(keyRequest).asScala),
        err => errorHandler(err, (errStr, retryable) => KmsCreateKeyError(errStr, retryable)),
      )
      kmsKeyId <- EitherT
        .fromEither[FutureUnlessShutdown](String300.create(response.keyMetadata().keyId()))
        .map(KmsKeyId.apply)
        .leftMap[KmsError](err => KmsCreateKeyError(err))
    } yield kmsKeyId

  override protected def generateSigningKeyPairInternal(
      signingKeySpec: SigningKeySpec,
      name: Option[KeyName],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      awsKeySpec <- convertToAwsKeySpec(signingKeySpec)
        .leftMap(err => KmsCreateKeyError(err))
        .toEitherT[FutureUnlessShutdown]
      kmsKeyId <- createKey(
        awsKeySpec,
        aws.KeyUsageType.SIGN_VERIFY,
        name,
      )
    } yield kmsKeyId

  override protected def generateSymmetricEncryptionKeyInternal(
      name: Option[KeyName]
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    createKey(
      aws.KeySpec.SYMMETRIC_DEFAULT,
      aws.KeyUsageType.ENCRYPT_DECRYPT,
      name,
    )

  override protected def generateAsymmetricEncryptionKeyPairInternal(
      encryptionKeySpec: EncryptionKeySpec,
      name: Option[KeyName],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      keySpec <- convertToAwsAsymmetricKeyEncryptionSpec(encryptionKeySpec)
        .leftMap(err => KmsCreateKeyError(err))
        .toEitherT[FutureUnlessShutdown]
      kmsKeyId <- createKey(
        keySpec,
        aws.KeyUsageType.ENCRYPT_DECRYPT,
        name,
      )
    } yield kmsKeyId

  private def getPublicKeyInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, aws.GetPublicKeyResponse] =
    for {
      getPublicKeyRequest <-
        Either
          .catchOnly[aws.KmsException] {
            aws.GetPublicKeyRequest.builder
              .keyId(keyId.unwrap)
              .withTraceContext(_.overrideConfiguration)
              .build
          }
          .toEitherT[FutureUnlessShutdown]
          .leftMap(err => KmsGetPublicKeyRequestError(keyId, ErrorUtil.messageWithStacktrace(err)))
      pkResponse <- EitherTUtil.fromFuture[KmsError, aws.GetPublicKeyResponse](
        synchronizeWithClosingF(functionFullName)(
          kmsClient.getPublicKey(getPublicKeyRequest).asScala
        ),
        err =>
          errorHandler(err, (errStr, retryable) => KmsGetPublicKeyError(keyId, errStr, retryable)),
      )
    } yield pkResponse

  override protected def getPublicSigningKeyInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsSigningPublicKey] =
    getPublicKeyInternal(keyId).flatMap { pkResponse =>
      pkResponse.keyUsage() match {
        case aws.KeyUsageType.SIGN_VERIFY =>
          val pubKeyRaw = ByteString.copyFrom(pkResponse.publicKey().asByteBuffer())
          for {
            keySpec <- convertFromAwsSigningKeySpec(pkResponse.keySpec())
              .leftMap[KmsError](KmsGetPublicKeyError(keyId, _))
              .toEitherT[FutureUnlessShutdown]
            pubKey <- KmsSigningPublicKey
              .create(pubKeyRaw, keySpec)
              .leftMap[KmsError](err => KmsGetPublicKeyError(keyId, err))
              .toEitherT[FutureUnlessShutdown]
          } yield pubKey
        case _ =>
          EitherT.leftT[FutureUnlessShutdown, KmsSigningPublicKey](
            KmsGetPublicKeyError(
              keyId,
              s"The selected key is defined for ${pkResponse.keyUsage()} and not for signing",
            )
          )
      }
    }

  override protected def getPublicEncryptionKeyInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsEncryptionPublicKey] =
    getPublicKeyInternal(keyId).flatMap { pkResponse =>
      pkResponse.keyUsage() match {
        case aws.KeyUsageType.ENCRYPT_DECRYPT =>
          val pubKeyRaw = ByteString.copyFrom(pkResponse.publicKey().asByteBuffer())
          for {
            keySpec <- convertFromAwsAsymmetricKeyEncryptionSpec(pkResponse.keySpec())
              .leftMap[KmsError](KmsGetPublicKeyError(keyId, _))
              .toEitherT[FutureUnlessShutdown]
            pubKey <- KmsEncryptionPublicKey
              .create(pubKeyRaw, keySpec)
              .leftMap[KmsError](err => KmsGetPublicKeyError(keyId, err))
              .toEitherT[FutureUnlessShutdown]
          } yield pubKey
        case _ =>
          EitherT.leftT[FutureUnlessShutdown, KmsEncryptionPublicKey](
            KmsGetPublicKeyError(
              keyId,
              s"The selected key is defined for ${pkResponse.keyUsage()} and not for encryption",
            )
          )
      }
    }

  private def convertToAwsKeySpec(
      signingKeySpec: SigningKeySpec
  ): Either[String, aws.KeySpec] =
    signingKeySpec match {
      case SigningKeySpec.EcCurve25519 =>
        Left(s"Unsupported signing key type: ${signingKeySpec.name}")
      case SigningKeySpec.EcP256 =>
        Right(aws.KeySpec.ECC_NIST_P256)
      case SigningKeySpec.EcP384 =>
        Right(aws.KeySpec.ECC_NIST_P384)
      case SigningKeySpec.EcSecp256k1 =>
        Right(aws.KeySpec.ECC_SECG_P256_K1)
    }

  private def convertToAwsAlgoSpec(
      signingAlgorithmSpec: SigningAlgorithmSpec
  ): Either[String, aws.SigningAlgorithmSpec] =
    signingAlgorithmSpec match {
      case SigningAlgorithmSpec.Ed25519 =>
        Left(s"Unsupported signing algorithm type: ${signingAlgorithmSpec.name}")
      case SigningAlgorithmSpec.EcDsaSha256 =>
        Right(aws.SigningAlgorithmSpec.ECDSA_SHA_256)
      case SigningAlgorithmSpec.EcDsaSha384 =>
        Right(aws.SigningAlgorithmSpec.ECDSA_SHA_384)
    }

  private def convertToAwsAsymmetricKeyEncryptionSpec(
      encryptionKeySpec: EncryptionKeySpec
  ): Either[String, aws.KeySpec] =
    encryptionKeySpec match {
      case EncryptionKeySpec.EcP256 =>
        Left(s"Unsupported encryption key type: ${encryptionKeySpec.name}")
      case EncryptionKeySpec.Rsa2048 =>
        Right(aws.KeySpec.RSA_2048)
    }

  private def convertToAwsAsymmetricEncryptionAlgorithmSpec(
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec
  ): Either[String, aws.EncryptionAlgorithmSpec] =
    encryptionAlgorithmSpec match {
      case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
        Left(s"Unsupported encryption key type: ${encryptionAlgorithmSpec.name}")
      case EncryptionAlgorithmSpec.RsaOaepSha256 =>
        Right(aws.EncryptionAlgorithmSpec.RSAES_OAEP_SHA_256)
    }

  private def convertFromAwsSigningKeySpec(keySpec: aws.KeySpec): Either[String, SigningKeySpec] =
    keySpec match {
      case aws.KeySpec.ECC_NIST_P256 => Right(SigningKeySpec.EcP256)
      case aws.KeySpec.ECC_NIST_P384 => Right(SigningKeySpec.EcP384)
      case aws.KeySpec.ECC_SECG_P256_K1 => Right(SigningKeySpec.EcSecp256k1)
      case _ => Left(s"Unsupported signing key type: ${keySpec.toString}")
    }

  private def convertFromAwsAsymmetricKeyEncryptionSpec(
      keySpec: aws.KeySpec
  ): Either[String, EncryptionKeySpec] =
    keySpec match {
      case aws.KeySpec.RSA_2048 => Right(EncryptionKeySpec.Rsa2048)
      case _ => Left(s"Unsupported encryption key type: ${keySpec.toString}")
    }

  private def getMetadataForActiveKeys(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KeyMetadata] =
    retrieveKeyMetadata(keyId)
      .leftMap[KmsError] {
        case err: KmsRetrieveKeyMetadataError if !err.retryable =>
          KmsCannotFindKeyError(keyId, err.show)
        case err => err
      }
      .flatMap { keyMetadata =>
        EitherTUtil
          .condUnitET[FutureUnlessShutdown](
            keyMetadata.enabled() == true,
            KmsKeyDisabledError(
              keyId,
              s"key is disabled",
            ),
          )
          .map(_ => keyMetadata)
          .leftWiden[KmsError]
      }

  override protected def keyExistsAndIsActiveInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    getMetadataForActiveKeys(keyId).map(_ => ())

  private def encrypt(
      keyId: KmsKeyId,
      data: ByteString,
      encryptionAlgorithm: aws.EncryptionAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString] =
    for {
      encryptRequest <-
        Either
          .catchOnly[aws.KmsException] {
            aws.EncryptRequest.builder
              .keyId(keyId.unwrap)
              .encryptionAlgorithm(encryptionAlgorithm)
              .plaintext(SdkBytes.fromByteArray(data.toByteArray))
              .withTraceContext(_.overrideConfiguration)
              .build
          }
          .toEitherT[FutureUnlessShutdown]
          .leftMap(err => KmsEncryptRequestError(keyId, ErrorUtil.messageWithStacktrace(err)))
      response <- EitherTUtil.fromFuture[KmsError, aws.EncryptResponse](
        synchronizeWithClosingF(functionFullName)(kmsClient.encrypt(encryptRequest).asScala),
        err => errorHandler(err, (errStr, retryable) => KmsEncryptError(keyId, errStr, retryable)),
      )
      encryptedData = response.ciphertextBlob
    } yield ByteString.copyFrom(encryptedData.asByteBuffer())

  override protected def encryptSymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString4096,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString6144] =
    encrypt(keyId, data.unwrap, aws.EncryptionAlgorithmSpec.SYMMETRIC_DEFAULT).flatMap(dataEnc =>
      ByteString6144
        .create(dataEnc)
        .toEitherT[FutureUnlessShutdown]
        .leftMap(err =>
          KmsError
            .KmsEncryptError(keyId, s"generated ciphertext does not adhere to bound: $err)")
        )
    )

  private def decrypt(
      keyId: KmsKeyId,
      data: ByteString,
      encryptionAlgorithm: aws.EncryptionAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString] =
    for {
      decryptRequest <-
        Either
          .catchOnly[aws.KmsException] {
            aws.DecryptRequest.builder
              .ciphertextBlob(SdkBytes.fromByteArray(data.toByteArray))
              .keyId(keyId.unwrap)
              .encryptionAlgorithm(encryptionAlgorithm)
              .withTraceContext(_.overrideConfiguration)
              .build
          }
          .toEitherT[FutureUnlessShutdown]
          .leftMap(err => KmsDecryptRequestError(keyId, ErrorUtil.messageWithStacktrace(err)))
      response <- EitherTUtil.fromFuture[KmsError, aws.DecryptResponse](
        synchronizeWithClosingF(functionFullName)(kmsClient.decrypt(decryptRequest).asScala),
        err => errorHandler(err, (errStr, retryable) => KmsDecryptError(keyId, errStr, retryable)),
      )
      decryptedData = response.plaintext
    } yield ByteString.copyFrom(decryptedData.asByteBuffer())

  override protected def decryptSymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString6144,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString4096] =
    decrypt(keyId, data.unwrap, aws.EncryptionAlgorithmSpec.SYMMETRIC_DEFAULT).flatMap(dataPlain =>
      ByteString4096
        .create(dataPlain)
        .toEitherT[FutureUnlessShutdown]
        .leftMap(err =>
          KmsError.KmsDecryptError(keyId, s"plaintext does not adhere to bound: $err)")
        )
    )

  override protected def decryptAsymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString256,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString190] =
    for {
      encryptionAlgorithm <- convertToAwsAsymmetricEncryptionAlgorithmSpec(encryptionAlgorithmSpec)
        .leftMap(KmsDecryptError(keyId, _))
        .toEitherT[FutureUnlessShutdown]
      decryptedData <- decrypt(keyId, data.unwrap, encryptionAlgorithm)
        .flatMap(
          ByteString190
            .create(_)
            .toEitherT[FutureUnlessShutdown]
            .leftMap[KmsError](err =>
              KmsError.KmsDecryptError(keyId, s"plaintext does not adhere to bound: $err)")
            )
        )
    } yield decryptedData

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
      signingAlgorithm <- convertToAwsAlgoSpec(signingAlgorithmSpec)
        .leftMap(KmsSignRequestError(keyId, _))
        .toEitherT[FutureUnlessShutdown]
      signRequest <-
        Either
          .catchOnly[aws.KmsException] {
            aws.SignRequest.builder
              .messageType(MessageType.RAW)
              .message(SdkBytes.fromByteArray(data.unwrap.toByteArray))
              .signingAlgorithm(signingAlgorithm)
              .keyId(keyId.unwrap)
              .withTraceContext(_.overrideConfiguration)
              .build
          }
          .toEitherT[FutureUnlessShutdown]
          .leftMap(err => KmsSignRequestError(keyId, ErrorUtil.messageWithStacktrace(err)))
      response <- EitherTUtil.fromFuture[KmsError, aws.SignResponse](
        synchronizeWithClosingF(functionFullName)(kmsClient.sign(signRequest).asScala),
        err => errorHandler(err, (errStr, retryable) => KmsSignError(keyId, errStr, retryable)),
      )
    } yield ByteString.copyFrom(response.signature().asByteBuffer())

  override protected def deleteKeyInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    for {
      scheduleKeyDeletionRequest <-
        Either
          .catchOnly[aws.KmsException] {
            aws.ScheduleKeyDeletionRequest
              .builder()
              .keyId(keyId.unwrap)
              // 7 days is the minimum waiting time for key deletion
              .pendingWindowInDays(7)
              .withTraceContext(_.overrideConfiguration)
              .build
          }
          .toEitherT[FutureUnlessShutdown]
          .leftMap(err => KmsDeleteKeyRequestError(keyId, ErrorUtil.messageWithStacktrace(err)))
      _ <- EitherTUtil.fromFuture[KmsError, aws.ScheduleKeyDeletionResponse](
        synchronizeWithClosingF(functionFullName)(
          kmsClient.scheduleKeyDeletion(scheduleKeyDeletionRequest).asScala
        ),
        err => errorHandler(err, (errStr, retryable) => KmsDeleteKeyError(keyId, errStr, retryable)),
      )
    } yield ()

  private def retrieveKeyMetadata(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, aws.KeyMetadata] =
    for {
      describeRequest <-
        Either
          .catchOnly[aws.KmsException] {
            aws.DescribeKeyRequest
              .builder()
              .keyId(keyId.unwrap)
              .withTraceContext(_.overrideConfiguration)
              .build
          }
          .toEitherT[FutureUnlessShutdown]
          .leftMap(err =>
            KmsRetrieveKeyMetadataRequestError(keyId, ErrorUtil.messageWithStacktrace(err))
          )
      response <- EitherTUtil.fromFuture[KmsError, aws.DescribeKeyResponse](
        synchronizeWithClosingF(functionFullName)(kmsClient.describeKey(describeRequest).asScala),
        err =>
          errorHandler(
            err,
            (errStr, retryable) => KmsRetrieveKeyMetadataError(keyId, errStr, retryable),
          ),
      )
    } yield response.keyMetadata()

  override def onClosed(): Unit =
    LifeCycle.close(kmsClient, LifeCycle.toCloseableOption(httpClientO))(logger)

}

object AwsKms extends Kms.SupportedSchemes {

  val supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]] =
    NonEmpty.mk(Set, SigningKeySpec.EcP256, SigningKeySpec.EcP384, SigningKeySpec.EcSecp256k1)

  val supportedSigningAlgoSpecs: NonEmpty[Set[SigningAlgorithmSpec]] =
    NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256, SigningAlgorithmSpec.EcDsaSha384)

  val supportedEncryptionKeySpecs: NonEmpty[Set[EncryptionKeySpec]] =
    NonEmpty.mk(Set, EncryptionKeySpec.Rsa2048)

  val supportedEncryptionAlgoSpecs: NonEmpty[Set[EncryptionAlgorithmSpec]] =
    NonEmpty.mk(Set, EncryptionAlgorithmSpec.RsaOaepSha256)

  /** Extension class on AWS request builders with a method to be used to set the Canton trace
    * context as an attribute that will be accessible by the request/response logger. See
    * [[audit.AwsRequestResponseLogger]] for more details.
    */
  implicit class ExtendedAwsRequestBuilder[A <: AwsRequest.Builder](val requestBuilder: A)
      extends AnyVal {

    /** Should be called on request builders to inject the canton trace context to the request
      * logger. Because of typing constraints of the SDK, the `overrideConfiguration` method must be
      * used on the specific request builder type and cannot be abstracted. This method usage looks
      * like:
      *
      * aws.DescribeKeyRequest .builder() .keyId(keyId.unwrap)
      * .withTraceContext(_.overrideConfiguration) .build
      */
    def withTraceContext(
        overrideMethod: A => AwsRequestOverrideConfiguration => A
    )(implicit tc: TraceContext): A = overrideMethod(requestBuilder)(
      AwsRequestOverrideConfiguration
        .builder()
        .putExecutionAttribute(traceContextExecutionAttribute, tc)
        .build()
    )
  }

  def create(
      config: KmsConfig.Aws,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider = NoReportingTracerProvider,
  ): Either[KmsError, AwsKms] = {
    val kmsAsyncClientBuilder = KmsAsyncClient
      .builder()
      .region(Region.of(config.region))
      /* We can access AWS in multiple ways, for example: (1) using the AWS security token service (sts)
         profile (2) setting up the following environment variables: AWS_ACCESS_KEY_ID,
         AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN */
      .credentialsProvider(DefaultCredentialsProvider.builder().build())

    config.endpointOverride.map(URI.create).foreach(kmsAsyncClientBuilder.endpointOverride)

    val httpClientO = Option.when(config.disableSslVerification) {
      loggerFactory
        .getLogger(getClass)
        .info("Disabling SSL verification")
      NettyNioAsyncHttpClient
        .builder()
        .buildWithDefaults(
          AttributeMap
            .builder()
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.box(true))
            .build()
        )
    }
    // this disables SSL certificate checks in the underlying http client.
    // setting the http client explicitly also means we need to close it ourselves.
    httpClientO.foreach(kmsAsyncClientBuilder.httpClient)

    if (config.auditLogging)
      kmsAsyncClientBuilder
        .overrideConfiguration(
          ClientOverrideConfiguration
            .builder()
            .addExecutionInterceptor(
              new AwsTraceContextInterceptor(loggerFactory, tracerProvider)
            )
            .addExecutionInterceptor(new AwsRequestResponseLogger(loggerFactory))
            .build()
        )

    Either
      .catchOnly[aws.KmsException] {
        new AwsKms(
          config,
          kmsAsyncClientBuilder
            .region(Region.of(config.region))
            /* We can access AWS in multiple ways, for example: (1) using the AWS security token service (sts)
             profile (2) setting up the following environment variables: AWS_ACCESS_KEY_ID,
             AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN */
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .build(),
          httpClientO,
          timeouts,
          loggerFactory,
        )
      }
      .leftMap[KmsError](err => KmsCreateClientError(ErrorUtil.messageWithStacktrace(err)))

  }
}
