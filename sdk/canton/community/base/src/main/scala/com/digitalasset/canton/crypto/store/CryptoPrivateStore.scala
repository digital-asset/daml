// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.kms.{Kms, KmsKeyId}
import com.digitalasset.canton.crypto.store.db.DbCryptoPrivateStore
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPrivateStore
import com.digitalasset.canton.error.{CantonBaseError, CantonErrorGroups}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion

import scala.concurrent.ExecutionContext

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

/** A store for cryptographic private material such as signing/encryption private keys and hmac
  * secrets.
  *
  * It encapsulates only existence checks/delete operations so it can be extendable to an external
  * crypto private store (e.g. an AWS KMS store).
  */
trait CryptoPrivateStore extends AutoCloseable {

  def removePrivateKey(
      keyId: Fingerprint
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit]

  def existsPrivateKey(
      keyId: Fingerprint,
      purpose: KeyPurpose,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean]

  /** Filter signing keys by checking if their usage intersects with the provided 'filterUsage' set.
    * This ensures that only keys with one or more matching usages are retained.
    *
    * @param signingKeyIds
    *   the fingerprint of the keys to filter
    * @param filterUsage
    *   the key usages to filter for
    * @return
    */
  def filterSigningKeys(
      signingKeyIds: NonEmpty[Seq[Fingerprint]],
      filterUsage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Seq[Fingerprint]]

  def existsSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean]

  def existsDecryptionKey(decryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean]

  def toExtended: Option[CryptoPrivateStoreExtended] = this match {
    case extended: CryptoPrivateStoreExtended => Some(extended)
    case _ => None
  }

  /** Returns the KMS key id that corresponds to a given private key fingerprint or None if the
    * private key is not stored in a KMS.
    *
    * @param keyId
    *   the private key fingerprint
    * @return
    *   the KMS key id that matches the fingerprint, or None if key is not stored in a KMS
    */
  def queryKmsKeyId(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[String300]]

}

object CryptoPrivateStore {

  private def migratePrivateKeys(
      storage: Storage,
      store: CryptoPrivateStore,
      timeouts: ProcessingTimeout,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, CryptoPrivateStore] =
    for {
      _ <- store.toExtended match {
        case Some(extendedStore) => extendedStore.migratePrivateKeys(storage.isActive, timeouts)
        case None => EitherT.pure[FutureUnlessShutdown, CryptoPrivateStoreError](())
      }
    } yield store

  /** Creates a non-encrypted crypto private store, backed either by in-memory storage or a
    * database.
    */
  def create(
      storage: Storage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      batchingConfig: BatchingConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, CryptoPrivateStore] = {
    val store = storage match {
      case jdbc: DbStorage =>
        val dbCryptoPrivateStore = new DbCryptoPrivateStore(
          jdbc,
          releaseProtocolVersion,
          timeouts,
          batchingConfig,
          loggerFactory,
        )
        dbCryptoPrivateStore
      case _: MemoryStorage =>
        new InMemoryCryptoPrivateStore(releaseProtocolVersion, loggerFactory)
    }
    migratePrivateKeys(storage, store, timeouts)
  }

  /** Creates an encrypted crypto private store, which must be backed by a database.
    *
    * @param kmsKeyId
    *   defines key identifier for the wrapper key (e.g. ARN for AWS SDK). When it's None Canton
    *   will either create a new key or use a previous existent key.
    * @param reverted
    *   when true decrypts the stored private keys and stores them in clear, disabling the encrypted
    *   crypto private key store in the process.
    */
  def createEncrypted(
      storage: Storage,
      kms: Kms,
      kmsKeyId: Option[KmsKeyId],
      reverted: Boolean,
      replicaManager: Option[ReplicaManager],
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      batchingConfig: BatchingConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, CryptoPrivateStore] =
    for {
      dbCryptoPrivateStore <- storage match {
        case jdbc: DbStorage =>
          EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
            new DbCryptoPrivateStore(
              jdbc,
              releaseProtocolVersion,
              timeouts,
              batchingConfig,
              loggerFactory,
            )
          )
        case _: MemoryStorage =>
          EitherT.leftT[FutureUnlessShutdown, DbCryptoPrivateStore](
            CryptoPrivateStoreError.EncryptedPrivateStoreError(
              "encryption is only supported for database-backed stores"
            )
          )
      }
      encryptedPrivateStore <- EncryptedCryptoPrivateStore
        .create(
          storage,
          dbCryptoPrivateStore,
          kms,
          kmsKeyId,
          reverted,
          releaseProtocolVersion,
          timeouts,
          batchingConfig,
          loggerFactory,
        )
        .flatMap(migratePrivateKeys(storage, _, timeouts))
        .map {
          case encryptedPrivateStore: EncryptedCryptoPrivateStore =>
            // Register the encrypted private store with the replica manager to refresh the in-memory caches on failover
            replicaManager.foreach(_.setPrivateKeyStore(encryptedPrivateStore))
            encryptedPrivateStore

          case store => store
        }
    } yield encryptedPrivateStore

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
        extends CantonBaseError.Impl(cause = "An error occurred with the private crypto store")

    final case class WrapStr(reason: String)
        extends CantonBaseError.Impl(cause = "An error occurred with the private crypto store")
  }

  final case class FailedToGetWrapperKeyId(reason: String) extends CryptoPrivateStoreError {
    override protected def pretty: Pretty[FailedToGetWrapperKeyId] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  final case class FailedToReadKey(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override protected def pretty: Pretty[FailedToReadKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class InvariantViolation(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override protected def pretty: Pretty[InvariantViolation] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class FailedToInsertKey(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override protected def pretty: Pretty[FailedToInsertKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KeyAlreadyExists(
      keyId: Fingerprint,
      existingKeyName: Option[String],
      newKeyName: Option[String],
  ) extends CryptoPrivateStoreError {
    override protected def pretty: Pretty[KeyAlreadyExists] =
      prettyOfClass(
        param("keyId", _.keyId),
        param("existingKeyName", _.existingKeyName.getOrElse("").unquoted),
        param("newKeyName", _.newKeyName.getOrElse("").unquoted),
      )
  }

  final case class FailedToDeleteKey(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override protected def pretty: Pretty[FailedToDeleteKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class EncryptedPrivateStoreError(reason: String) extends CryptoPrivateStoreError {
    override protected def pretty: Pretty[EncryptedPrivateStoreError] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  final case class WrapperKeyAlreadyInUse(reason: String) extends CryptoPrivateStoreError {
    override protected def pretty: Pretty[WrapperKeyAlreadyInUse] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }
}
