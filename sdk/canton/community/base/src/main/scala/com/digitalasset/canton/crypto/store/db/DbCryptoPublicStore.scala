// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import cats.data.OptionT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore, IdempotentInsert}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.dbio.DBIOAction
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.ExecutionContext

class DbCryptoPublicStore(
    override protected val storage: DbStorage,
    protected val releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends CryptoPublicStore
    with DbStore {

  import storage.api.*
  import storage.converters.*

  private implicit val setParameterEncryptionPublicKey: SetParameter[EncryptionPublicKey] =
    EncryptionPublicKey.getVersionedSetParameter(releaseProtocolVersion.v)
  private implicit val setParameterSigningPublicKey: SetParameter[SigningPublicKey] =
    SigningPublicKey.getVersionedSetParameter(releaseProtocolVersion.v)

  private def queryKeys[K: GetResult](purpose: KeyPurpose): DbAction.ReadOnly[Set[K]] =
    sql"select data, name from common_crypto_public_keys where purpose = $purpose"
      .as[K]
      .map(_.toSet)

  private def queryKeyO[K <: PublicKeyWithName: GetResult](
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[Option[K]] =
    sql"select data, name from common_crypto_public_keys where key_id = $keyId and purpose = $purpose"
      .as[K]
      .headOption

  private def queryKey[K <: PublicKeyWithName: GetResult](
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[K] =
    sql"select data, name from common_crypto_public_keys where key_id = $keyId and purpose = $purpose"
      .as[K]
      .head

  private def insertKey[K <: PublicKey: SetParameter, KN <: PublicKeyWithName: GetResult](
      key: K,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    storage.queryAndUpdate(
      IdempotentInsert.insertVerifyingConflicts(
        sql"""insert into common_crypto_public_keys (key_id, purpose, data, name)
              values (${key.id}, ${key.purpose}, $key, $name)
              on conflict do nothing""".asUpdate,
        queryKey(key.id, key.purpose),
      )(
        existingKey => existingKey.publicKey == key && existingKey.name == name,
        _ => s"Existing public key for ${key.id} is different than inserted key",
      ),
      functionFullName,
    )

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, SigningPublicKeyWithName] =
    storage
      .querySingle(
        queryKeyO[SigningPublicKeyWithName](signingKeyId, KeyPurpose.Signing),
        functionFullName,
      )

  override def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, EncryptionPublicKeyWithName] =
    storage
      .querySingle(
        queryKeyO[EncryptionPublicKeyWithName](encryptionKeyId, KeyPurpose.Encryption),
        functionFullName,
      )

  override protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    insertKey[SigningPublicKey, SigningPublicKeyWithName](key, name)

  override protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    insertKey[EncryptionPublicKey, EncryptionPublicKeyWithName](key, name)

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[SigningPublicKeyWithName]] =
    storage.query(
      queryKeys[SigningPublicKeyWithName](KeyPurpose.Signing),
      functionFullName,
    )

  override private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[EncryptionPublicKeyWithName]] =
    storage
      .query(
        queryKeys[EncryptionPublicKeyWithName](KeyPurpose.Encryption),
        functionFullName,
      )

  override private[crypto] def deleteKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage
      .update_(
        sqlu"delete from common_crypto_public_keys where key_id = $keyId",
        functionFullName,
      )

  override private[crypto] def replaceSigningPublicKeys(
      newKeys: Seq[SigningPublicKey]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = replacePublicKeys(newKeys)

  override private[crypto] def replaceEncryptionPublicKeys(
      newKeys: Seq[EncryptionPublicKey]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = replacePublicKeys(newKeys)

  private def replacePublicKeys[K <: PublicKey: SetParameter](
      newKeys: Seq[K]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    storage.update_(
      DBIOAction.sequence(newKeys.map(updateKey(_))).transactionally,
      functionFullName,
    )

  // Update the contents of a key identified by its id; `purpose` and `name` remain unchanged.
  // Used for key format migrations.
  private def updateKey[K <: PublicKey: SetParameter](
      key: K
  ): DbAction.WriteOnly[Int] =
    sqlu"""update common_crypto_public_keys set data = $key where key_id = ${key.id}"""
}
