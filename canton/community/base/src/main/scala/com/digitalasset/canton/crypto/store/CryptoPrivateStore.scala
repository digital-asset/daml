// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.db.DbCryptoPrivateStore
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPrivateStore
import com.digitalasset.canton.error.{BaseCantonError, CantonErrorGroups}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.version.ReleaseProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

sealed trait PrivateKeyWithName extends Product with Serializable {
  type K <: PrivateKey
  def privateKey: K
  def name: Option[KeyName]
}

final case class SigningPrivateKeyWithName(
    override val privateKey: SigningPrivateKey,
    override val name: Option[KeyName],
) extends PrivateKeyWithName {
  type K = SigningPrivateKey
}

final case class EncryptionPrivateKeyWithName(
    override val privateKey: EncryptionPrivateKey,
    override val name: Option[KeyName],
) extends PrivateKeyWithName {
  type K = EncryptionPrivateKey
}

/** A store for cryptographic private material such as signing/encryption private keys and hmac secrets.
  *
  * It encapsulates only existence checks/delete operations so it can be extendable to an external
  * crypto private store (e.g. an AWS KMS store).
  */
trait CryptoPrivateStore extends AutoCloseable {

  def removePrivateKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Unit]

  def existsPrivateKey(
      keyId: Fingerprint,
      purpose: KeyPurpose,
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Boolean]

  def existsSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Boolean]

  def existsDecryptionKey(decryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Boolean]

  def toExtended: Option[CryptoPrivateStoreExtended] = this match {
    case extended: CryptoPrivateStoreExtended => Some(extended)
    case _ => None
  }
}

object CryptoPrivateStore {
  trait CryptoPrivateStoreFactory {
    def create(
        storage: Storage,
        releaseProtocolVersion: ReleaseProtocolVersion,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
        tracerProvider: TracerProvider,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, CryptoPrivateStoreError, CryptoPrivateStore]
  }

  class CommunityCryptoPrivateStoreFactory extends CryptoPrivateStoreFactory {
    override def create(
        storage: Storage,
        releaseProtocolVersion: ReleaseProtocolVersion,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
        tracerProvider: TracerProvider,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, CryptoPrivateStoreError, CryptoPrivateStore] =
      storage match {
        case _: MemoryStorage =>
          EitherT.rightT[Future, CryptoPrivateStoreError](
            new InMemoryCryptoPrivateStore(releaseProtocolVersion, loggerFactory)
          )
        case jdbc: DbStorage =>
          EitherT.rightT[Future, CryptoPrivateStoreError](
            new DbCryptoPrivateStore(jdbc, releaseProtocolVersion, timeouts, loggerFactory)
          )
      }
  }
}

sealed trait CryptoPrivateStoreError extends Product with Serializable with PrettyPrinting
object CryptoPrivateStoreError extends CantonErrorGroups.CommandErrorGroup {

  @Explanation("This error indicates that a key could not be stored.")
  @Resolution("Inspect the error details")
  object ErrorCode
      extends ErrorCode(
        id = "CRYPTO_PRIVATE_STORE_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Wrap(reason: CryptoPrivateStoreError)
        extends BaseCantonError.Impl(cause = "An error occurred with the private crypto store")

    final case class WrapStr(reason: String)
        extends BaseCantonError.Impl(cause = "An error occurred with the private crypto store")
  }

  final case class FailedToListKeys(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToListKeys] = prettyOfClass(unnamedParam(_.reason.unquoted))
  }

  final case class FailedToGetWrapperKeyId(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToGetWrapperKeyId] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  final case class FailedToReadKey(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToReadKey] = prettyOfClass(unnamedParam(_.reason.unquoted))
  }

  final case class InvariantViolation(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[InvariantViolation] = prettyOfClass(unnamedParam(_.reason.unquoted))
  }

  final case class FailedToInsertKey(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToInsertKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KeyAlreadyExists(keyId: Fingerprint, existingKeyName: Option[String])
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[KeyAlreadyExists] =
      prettyOfClass(
        param("keyId", _.keyId),
        param("existingKeyName", _.existingKeyName.getOrElse("").unquoted),
      )
  }

  final case class FailedToDeleteKey(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToDeleteKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class FailedToReplaceKeys(keyId: Seq[Fingerprint], reason: String)
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToReplaceKeys] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class EncryptedPrivateStoreError(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[EncryptedPrivateStoreError] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  final case class WrapperKeyAlreadyInUse(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[WrapperKeyAlreadyInUse] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }
}
