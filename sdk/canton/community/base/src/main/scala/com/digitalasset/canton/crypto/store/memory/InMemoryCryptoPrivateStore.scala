// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto.store.db.StoredPrivateKey
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStoreError,
  CryptoPrivateStoreExtended,
  EncryptionPrivateKeyWithName,
  PrivateKeyWithName,
  SigningPrivateKeyWithName,
}
import com.digitalasset.canton.crypto.{
  EncryptionPrivateKey,
  Fingerprint,
  KeyName,
  KeyPurpose,
  PrivateKey,
  SigningPrivateKey,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.TrieMapUtil
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** The in-memory store does not provide any persistence and keys during runtime are stored in the generic caching layer.
  */
class InMemoryCryptoPrivateStore(
    override protected val releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(
    override implicit val ec: ExecutionContext
) extends CryptoPrivateStoreExtended
    with NamedLogging {

  private val storedSigningKeyMap: TrieMap[Fingerprint, SigningPrivateKeyWithName] = TrieMap.empty
  private val storedDecryptionKeyMap: TrieMap[Fingerprint, EncryptionPrivateKeyWithName] =
    TrieMap.empty

  private def wrapPrivateKeyInToStored(pk: PrivateKey, name: Option[KeyName]): StoredPrivateKey =
    new StoredPrivateKey(
      id = pk.id,
      data = (pk: @unchecked) match {
        case spk: SigningPrivateKey => spk.toByteString(releaseProtocolVersion.v)
        case epk: EncryptionPrivateKey => epk.toByteString(releaseProtocolVersion.v)
      },
      purpose = pk.purpose,
      name = name,
      wrapperKeyId = None,
    )

  private def errorDuplicate[K <: PrivateKeyWithName](
      keyId: Fingerprint,
      oldKey: K,
      newKey: K,
  ): CryptoPrivateStoreError =
    CryptoPrivateStoreError.KeyAlreadyExists(keyId, oldKey.name.map(_.unwrap))

  private[crypto] def readPrivateKey(keyId: Fingerprint, purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[StoredPrivateKey]] = {
    purpose match {
      case Signing =>
        storedSigningKeyMap
          .get(keyId)
          .parTraverse(pk =>
            EitherT.rightT[Future, CryptoPrivateStoreError](
              wrapPrivateKeyInToStored(pk.privateKey, pk.name)
            )
          )
      case Encryption =>
        storedDecryptionKeyMap
          .get(keyId)
          .parTraverse(pk =>
            EitherT.rightT[Future, CryptoPrivateStoreError](
              wrapPrivateKeyInToStored(pk.privateKey, pk.name)
            )
          )
    }
  }

  private[crypto] def writePrivateKey(
      key: StoredPrivateKey
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] = {

    def parseAndWritePrivateKey[A <: PrivateKey, B <: PrivateKeyWithName](
        pk: A,
        cache: TrieMap[Fingerprint, B],
        buildKeyWithNameFunc: (A, Option[KeyName]) => B,
    ): EitherT[Future, CryptoPrivateStoreError, Unit] =
      TrieMapUtil
        .insertIfAbsent(
          cache,
          key.id,
          buildKeyWithNameFunc(pk, key.name),
          errorDuplicate[B] _,
        )
        .toEitherT

    val storedKey = key.purpose match {
      case Signing => SigningPrivateKey.fromByteString(key.data)
      case Encryption => EncryptionPrivateKey.fromByteString(key.data)
    }

    for {
      res <- storedKey match {
        case Left(parseErr) =>
          EitherT.leftT[Future, Unit](
            CryptoPrivateStoreError.FailedToInsertKey(
              key.id,
              s"could not parse stored key (it can either be corrupted or encrypted): ${parseErr.toString}",
            ): CryptoPrivateStoreError
          )
        case Right(spk: SigningPrivateKey) =>
          parseAndWritePrivateKey[SigningPrivateKey, SigningPrivateKeyWithName](
            spk,
            storedSigningKeyMap,
            (privateKey, name) => SigningPrivateKeyWithName(privateKey, name),
          )
        case Right(epk: EncryptionPrivateKey) =>
          parseAndWritePrivateKey[EncryptionPrivateKey, EncryptionPrivateKeyWithName](
            epk,
            storedDecryptionKeyMap,
            (privateKey, name) => EncryptionPrivateKeyWithName(privateKey, name),
          )
        case _ =>
          EitherT.leftT[Future, Unit](
            CryptoPrivateStoreError.FailedToInsertKey(
              key.id,
              s"key type does not match any of the known types",
            ): CryptoPrivateStoreError
          )
      }
    } yield res
  }

  @VisibleForTesting
  private[canton] def listPrivateKeys(purpose: KeyPurpose, encrypted: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[StoredPrivateKey]] =
    (purpose match {
      case Signing =>
        storedSigningKeyMap.values.toSeq
          .parTraverse((x: SigningPrivateKeyWithName) =>
            EitherT.rightT[Future, CryptoPrivateStoreError](
              wrapPrivateKeyInToStored(x.privateKey, x.name)
            )
          )
      case Encryption =>
        storedDecryptionKeyMap.values.toSeq
          .parTraverse((x: EncryptionPrivateKeyWithName) =>
            EitherT.rightT[Future, CryptoPrivateStoreError](
              wrapPrivateKeyInToStored(x.privateKey, x.name)
            )
          )
    }).map(_.toSet)

  private[crypto] def deletePrivateKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Unit] = {
    storedSigningKeyMap.remove(keyId).discard
    storedDecryptionKeyMap.remove(keyId).discard
    EitherT.rightT(())
  }

  private[crypto] def replaceStoredPrivateKeys(newKeys: Seq[StoredPrivateKey])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    newKeys
      .parTraverse { newKey =>
        for {
          _ <- deletePrivateKey(newKey.id)
          _ <- writePrivateKey(newKey)
        } yield ()
      }
      .map(_ => ())

  private[crypto] def encrypted(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[String300]] =
    EitherT.rightT[Future, CryptoPrivateStoreError](None)

  override def close(): Unit = ()
}
