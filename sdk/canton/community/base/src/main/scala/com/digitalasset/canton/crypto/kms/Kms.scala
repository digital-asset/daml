// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms

import cats.data.EitherT
import cats.implicits.showInterpolator
import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.KmsConfig
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.retry.{Jitter, NoExceptionRetryPolicy, Success}
import com.google.protobuf.ByteString
import pureconfig.ConfigReader
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.ExecutionContext

object KmsKeyId {
  implicit val setParameterKmsKeyId: SetParameter[KmsKeyId] = (f, pp) => pp >> f.str
  implicit val getResultKmsKeyId: GetResult[KmsKeyId] = GetResult { r =>
    String300
      .fromProtoPrimitive(r.nextString(), "KmsKeyId")
      .map(KmsKeyId(_))
      .valueOr(err =>
        throw new DbDeserializationException(s"Failed to deserialize KMS key id: $err")
      )
  }

  implicit val kmsKeyIdReader: ConfigReader[KmsKeyId] =
    String300.lengthLimitedStringReader.map(KmsKeyId.apply)

  def create(str: String): Either[String, KmsKeyId] = String300.create(str).map(KmsKeyId.apply)

  def tryCreate(str: String): KmsKeyId = KmsKeyId(String300.tryCreate(str))
}

// a wrapper type for a KMS key id
final case class KmsKeyId(str: String300) extends PrettyPrinting {
  def unwrap: String = str.unwrap
  override protected def pretty: Pretty[KmsKeyId] = prettyOfClass(
    unnamedParam(_.str.unwrap.unquoted)
  )
}

/** Represents a KMS interface and allows symmetric encryption/decryption with keys stored in the KMS. */
trait Kms extends FlagCloseable {
  type Config <: KmsConfig

  def config: Config

  /** Creates a new signing key pair in the KMS and returns its key identifier.
    * @param signingKeySpec defines the signing key specification to which the key is going to be used for.
    * @param name an optional name to identify the key.
    * @return a key id or an error if it fails to create a key
    */
  def generateSigningKeyPair(
      signingKeySpec: SigningKeySpec,
      name: Option[KeyName] = None,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    withRetries(s"generate signing key pair with name ${name.map(_.show).getOrElse("N/A")}")(
      generateSigningKeyPairInternal(signingKeySpec, name)
    )

  protected def generateSigningKeyPairInternal(
      signingKeySpec: SigningKeySpec,
      name: Option[KeyName] = None,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId]

  /** Creates a new symmetric encryption key in the KMS and returns its key identifier.
    * The specific encryption scheme is not necessary (default is taken)
    * because this is intended to be used to generate a KMS wrapper key.
    *
    * @param name an optional name to identify the key.
    * @return a key id or an error if it fails to create a key
    */
  def generateSymmetricEncryptionKey(
      name: Option[KeyName] = None
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    withRetries(
      s"generate symmetric encryption key with name ${name.map(_.show).getOrElse("N/A")}"
    )(generateSymmetricEncryptionKeyInternal(name))

  protected def generateSymmetricEncryptionKeyInternal(
      name: Option[KeyName] = None
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId]

  /** Creates a new (asymmetric) encryption key pair in the KMS and returns a key identifier.
    *
    * @param encryptionKeySpec defines the encryption key specification to which the key is going to be used for.
    * @param name an optional name to identify the key.
    * @return a key id or an error if it fails to create a key
    */
  def generateAsymmetricEncryptionKeyPair(
      encryptionKeySpec: EncryptionKeySpec,
      name: Option[KeyName] = None,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    withRetries(
      s"generate asymmetric encryption key pair with name ${name.map(_.show).getOrElse("N/A")}"
    )(generateAsymmetricEncryptionKeyPairInternal(encryptionKeySpec, name))

  protected def generateAsymmetricEncryptionKeyPairInternal(
      encryptionKeySpec: EncryptionKeySpec,
      name: Option[KeyName] = None,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId]

  /** Get the public key with the given keyId */
  def getPublicKey(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, PublicKey] =
    getPublicSigningKeyInternal(keyId)
      .leftFlatMap(_ => getPublicEncryptionKeyInternal(keyId).widen[PublicKey])

  /** Get public key for signing from KMS given a KMS key identifier.
    *
    * @param keyId key identifier (e.g. AWS key ARN)
    * @return the public signing key for that keyId
    */
  def getPublicSigningKey(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, SigningPublicKey] =
    withRetries(show"get signing public key for $keyId")(
      getPublicSigningKeyInternal(keyId)
    )

  protected def getPublicSigningKeyInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, SigningPublicKey]

  /** Get public key for encryption from KMS given a KMS key identifier.
    *
    * @param keyId key identifier (e.g. AWS key ARN)
    * @return the public encryption key for that keyId
    */
  def getPublicEncryptionKey(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, EncryptionPublicKey] =
    withRetries(show"get encryption public key for $keyId")(
      getPublicEncryptionKeyInternal(keyId)
    )

  protected def getPublicEncryptionKeyInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, EncryptionPublicKey]

  /** Checks that a key identified by keyId exists in the KMS and is not deleted or disabled,
    * and therefore can be used.
    * @param keyId key identifier (e.g. AWS key ARN)
    * @return error if it fails to find key
    */
  def keyExistsAndIsActive(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    withRetries(show"verify key $keyId exists and is active", checkKeyCreation = true)(
      keyExistsAndIsActiveInternal(keyId)
    )

  protected def keyExistsAndIsActiveInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit]

  /** Symmetrically encrypt the data passed as a byte string using a KMS symmetric key.
    *
    * @param keyId key identifier (e.g. AWS key ARN)
    * @param data byte string to encrypt. The higher bound on the data size we can encrypt is 4kb
    *             (i.e. maximum accepted input size for the external KMSs that we support).
    * @return an encrypted byte string or an error if it fails to encrypt
    */
  def encryptSymmetric(
      keyId: KmsKeyId,
      data: ByteString4096,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString6144] =
    withRetries(show"symmetric encrypting with key $keyId")(
      encryptSymmetricInternal(keyId, data)
    )

  protected def encryptSymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString4096,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString6144]

  /** Symmetrically decrypt the data passed as a byte array using a KMS symmetric key.
    *
    * @param keyId key identifier (e.g. AWS key ARN)
    * @param data byte string to decrypt. The higher bound on the data size we can decrypt is 6144 bytes
    *             (i.e. maximum accepted input size for the external KMSs that we support).
    * @return a decrypted byte string or an error if it fails to decrypt
    */
  def decryptSymmetric(
      keyId: KmsKeyId,
      data: ByteString6144,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString4096] =
    withRetries(show"symmetric decrypting with key $keyId")(
      decryptSymmetricInternal(keyId, data)
    )

  protected def decryptSymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString6144,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString4096]

  /** Asymmetrically decrypt the data passed as a byte array using a KMS private key.
    *
    * @param keyId key identifier (e.g. AWS key ARN)
    * @param data byte string to decrypt. The higher bound on the data size we can decrypt is 256bytes
    *             (i.e. the ciphertext length for RSA2048-OAEP-SHA256 encryption; when using
    *             RSAES-OAEP the ciphertext size is always equal to the size of the Modulus).
    * @param encryptionAlgorithmSpec the encryption algorithm that was used to encrypt the plaintext message.
    *                                The algorithm must be compatible with the KMS key that you specify.
    * @return a decrypted byte string or an error if it fails to decrypt
    */
  def decryptAsymmetric(
      keyId: KmsKeyId,
      data: ByteString256,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString190] =
    withRetries(show"asymmetric decrypting with key $keyId")(
      decryptAsymmetricInternal(keyId, data, encryptionAlgorithmSpec)
    )

  protected def decryptAsymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString256,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString190]

  /** Sign the data passed as a byte string using a KMS key.
    *
    * @param keyId key identifier (e.g. AWS key ARN)
    * @param data byte string to sign. The higher bound on the data size we can sign is 4kb
    *             (i.e. maximum accepted input size for the external KMSs that we support).
    * @param signingAlgorithmSpec the signing algorithm to use to generate the signature
    * @return a byte string corresponding to the signature of the data
    */
  def sign(
      keyId: KmsKeyId,
      data: ByteString4096,
      signingAlgorithmSpec: SigningAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString] =
    withRetries(show"signing with key $keyId")(signInternal(keyId, data, signingAlgorithmSpec))

  protected def signInternal(
      keyId: KmsKeyId,
      data: ByteString4096,
      signingAlgorithmSpec: SigningAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString]

  /** Schedule a deletion of a KMS key (takes between 7-30 days)
    *
    * @param keyId key identifier (e.g. AWS key ARN)
    * @return an error if it fails to schedule a deletion of a key
    */
  def deleteKey(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    withRetries(show"deleting key $keyId")(deleteKeyInternal(keyId))

  protected def deleteKeyInternal(
      keyId: KmsKeyId
  )(implicit ec: ExecutionContext, tc: TraceContext): EitherT[FutureUnlessShutdown, KmsError, Unit]

  private def kmsSuccess[T]: Success[Either[KmsError, T]] = new Success(
    {
      case Right(_) => true
      case Left(err: KmsError) if err.retryable => false
      case _ => true
    }
  )

  private def withKmsBackoffRetryPolicyET[E, T](
      description: String,
      config: KmsConfig.ExponentialBackoffConfig,
      flagCloseable: FlagCloseable,
      logger: TracedLogger,
  )(task: => EitherT[FutureUnlessShutdown, E, T])(implicit
      success: Success[Either[E, T]],
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, E, T] =
    EitherT {
      retry
        .Backoff(
          logger,
          flagCloseable,
          config.maxRetries,
          initialDelay = config.initialDelay.asFiniteApproximation,
          maxDelay = config.maxDelay.duration,
          description,
        )(Jitter.equal(config.initialDelay.asFiniteApproximation))
        .unlessShutdown(task.value, NoExceptionRetryPolicy)
    }

  // Retry wrapper specific for KMS operations returning KmsErrors
  // Meant to catch transient network failures. Adjust the kmsSuccess function above as necessary
  protected def withRetries[T](
      description: String,
      checkKeyCreation: Boolean = false,
  )(
      task: => EitherT[FutureUnlessShutdown, KmsError, T]
  )(implicit ec: ExecutionContext, tc: TraceContext): EitherT[FutureUnlessShutdown, KmsError, T] = {
    val backoffConfig =
      if (checkKeyCreation) config.retries.createKeyCheck
      else config.retries.failures
    withKmsBackoffRetryPolicyET(
      description,
      backoffConfig,
      this,
      logger,
    )(task)(kmsSuccess[T], ec, tc)
      .thereafter {
        case scala.util.Success(Outcome(Right(_))) => ()
        case scala.util.Success(AbortedDueToShutdown) => ()
        // We explicitly log KMS errors that have bubbled up from the retry
        case scala.util.Success(Outcome(Left(err))) =>
          logger.warn(s"KMS operation `$description` failed: $err")
        case scala.util.Failure(err) =>
          logger.warn(s"KMS operation `$description` failed", err)
      }
  }
}

sealed trait KmsError extends Product with Serializable with PrettyPrinting {
  def retryable: Boolean = false
}

object KmsError {

  // todo i10029: create error codes for these exceptions
  final case class KmsCreateClientError(reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsCreateClientError] =
      prettyOfClass(param("reason", _.reason.unquoted))
  }

  final case class KmsNoConfigError(reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsNoConfigError] =
      prettyOfClass(param("reason", _.reason.unquoted))
  }

  final case class KmsCreateKeyRequestError(reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsCreateKeyRequestError] =
      prettyOfClass(param("reason", _.reason.unquoted))
  }

  final case class KmsCreateKeyError(reason: String, override val retryable: Boolean = false)
      extends KmsError {
    override protected def pretty: Pretty[KmsCreateKeyError] =
      prettyOfClass(param("reason", _.reason.unquoted))
  }

  final case class KmsCannotFindKeyError(keyId: KmsKeyId, reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsCannotFindKeyError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsKeyDisabledError(
      keyId: KmsKeyId,
      reason: String,
      override val retryable: Boolean = false,
  ) extends KmsError {
    override protected def pretty: Pretty[KmsKeyDisabledError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsGetPublicKeyRequestError(keyId: KmsKeyId, reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsGetPublicKeyRequestError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsGetPublicKeyError(
      keyId: KmsKeyId,
      reason: String,
      override val retryable: Boolean = false,
  ) extends KmsError {
    override protected def pretty: Pretty[KmsGetPublicKeyError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsEncryptRequestError(keyId: KmsKeyId, reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsEncryptRequestError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsEncryptError(
      keyId: KmsKeyId,
      reason: String,
      override val retryable: Boolean = false,
  ) extends KmsError {
    override protected def pretty: Pretty[KmsEncryptError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsDecryptRequestError(keyId: KmsKeyId, reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsDecryptRequestError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsDecryptError(
      keyId: KmsKeyId,
      reason: String,
      override val retryable: Boolean = false,
  ) extends KmsError {
    override protected def pretty: Pretty[KmsDecryptError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsSignRequestError(keyId: KmsKeyId, reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsSignRequestError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsSignError(
      keyId: KmsKeyId,
      reason: String,
      override val retryable: Boolean = false,
  ) extends KmsError {
    override protected def pretty: Pretty[KmsSignError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsDeleteKeyRequestError(keyId: KmsKeyId, reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsDeleteKeyRequestError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsDeleteKeyError(
      keyId: KmsKeyId,
      reason: String,
      override val retryable: Boolean = false,
  ) extends KmsError {
    override protected def pretty: Pretty[KmsDeleteKeyError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsRetrieveKeyMetadataRequestError(keyId: KmsKeyId, reason: String)
      extends KmsError {
    override protected def pretty: Pretty[KmsRetrieveKeyMetadataRequestError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsRetrieveKeyMetadataError(
      keyId: KmsKeyId,
      reason: String,
      override val retryable: Boolean = false,
  ) extends KmsError {
    override protected def pretty: Pretty[KmsRetrieveKeyMetadataError] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KmsInvalidConfigError(reason: String) extends KmsError {
    override protected def pretty: Pretty[KmsInvalidConfigError] =
      prettyOfClass(param("reason", _.reason.unquoted))
  }

}
