// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import cats.data.OptionT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.store.{CryptoPublicStore, CryptoPublicStoreError}
import com.digitalasset.canton.crypto.{KeyName, *}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, TrieMapUtil}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class InMemoryCryptoPublicStore(override protected val loggerFactory: NamedLoggerFactory)(
    override implicit val ec: ExecutionContext
) extends CryptoPublicStore
    with NamedLogging {

  private val storedSigningKeyMap: TrieMap[Fingerprint, SigningPublicKeyWithName] = TrieMap.empty
  private val storedEncryptionKeyMap: TrieMap[Fingerprint, EncryptionPublicKeyWithName] =
    TrieMap.empty

  private def errorKeyDuplicate[K <: PublicKeyWithName: Pretty](
      keyId: Fingerprint,
      oldKey: K,
      newKey: K,
  ): Either[CryptoPublicStoreError, Unit] =
    if (oldKey.publicKey != newKey.publicKey)
      Left(CryptoPublicStoreError.KeyAlreadyExists(keyId, oldKey, newKey))
    else Right(())

  override protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future {
      TrieMapUtil
        .insertIfAbsentE(
          storedSigningKeyMap,
          key.id,
          SigningPublicKeyWithName(key, name),
          errorKeyDuplicate[SigningPublicKeyWithName],
        )
        // An error is thrown if, and only if, the key we want to insert has the same id but different key payloads.
        .valueOr { err =>
          ErrorUtil.invalidState(
            s"Existing public key for ${key.id} is different than inserted key: $err"
          )
        }
    }

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, SigningPublicKeyWithName] =
    OptionT.fromOption(storedSigningKeyMap.get(signingKeyId))

  override def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, EncryptionPublicKeyWithName] =
    OptionT.fromOption(storedEncryptionKeyMap.get(encryptionKeyId))

  override protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    Future {
      TrieMapUtil
        .insertIfAbsentE(
          storedEncryptionKeyMap,
          key.id,
          EncryptionPublicKeyWithName(key, name),
          errorKeyDuplicate[EncryptionPublicKeyWithName],
        )
        // An error is thrown if, and only if, the key we want to insert has the same id but different key payloads.
        .valueOr { err =>
          ErrorUtil.invalidState(
            s"Existing public key for ${key.id} is different than inserted key: $err"
          )
        }
    }

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): Future[Set[SigningPublicKeyWithName]] =
    Future.successful(storedSigningKeyMap.values.toSet)

  override private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): Future[Set[EncryptionPublicKeyWithName]] =
    Future.successful(storedEncryptionKeyMap.values.toSet)

  override protected def deleteKeyInternal(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): Future[Unit] = {
    storedSigningKeyMap.remove(keyId).discard
    storedEncryptionKeyMap.remove(keyId).discard
    Future.unit
  }

  override def close(): Unit = ()

}
