// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import cats.data.OptionT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.store.{CryptoPublicStore, CryptoPublicStoreError}
import com.digitalasset.canton.crypto.{KeyName, *}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, TrieMapUtil}

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

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
  ): CryptoPublicStoreError =
    CryptoPublicStoreError.KeyAlreadyExists(keyId, oldKey, newKey)

  override protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    FutureUnlessShutdown.wrap {
      TrieMapUtil
        .insertIfAbsent(
          storedSigningKeyMap,
          key.id,
          SigningPublicKeyWithName(key, name),
          errorKeyDuplicate[SigningPublicKeyWithName] _,
        )
        .valueOr { err =>
          ErrorUtil.invalidState(
            s"Existing public key for ${key.id} is different than inserted key: $err"
          )
        }
    }
  }

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, SigningPublicKeyWithName] =
    OptionT.fromOption(storedSigningKeyMap.get(signingKeyId))

  override def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, EncryptionPublicKeyWithName] =
    OptionT.fromOption(storedEncryptionKeyMap.get(encryptionKeyId))

  override protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    FutureUnlessShutdown.wrap {
      TrieMapUtil
        .insertIfAbsent(
          storedEncryptionKeyMap,
          key.id,
          EncryptionPublicKeyWithName(key, name),
          errorKeyDuplicate[EncryptionPublicKeyWithName] _,
        )
        .valueOr { _ =>
          ErrorUtil.invalidState(
            s"Existing public key for ${key.id} is different than inserted key"
          )
        }
    }
  }

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[SigningPublicKeyWithName]] =
    FutureUnlessShutdown.pure(storedSigningKeyMap.values.toSet)

  override private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[EncryptionPublicKeyWithName]] =
    FutureUnlessShutdown.pure(storedEncryptionKeyMap.values.toSet)

  override private[crypto] def deleteKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    storedSigningKeyMap.remove(keyId).discard
    storedEncryptionKeyMap.remove(keyId).discard
    FutureUnlessShutdown.unit
  }

  override def close(): Unit = ()

}
