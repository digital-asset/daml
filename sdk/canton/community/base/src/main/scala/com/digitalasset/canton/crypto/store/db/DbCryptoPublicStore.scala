// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import cats.data.OptionT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore, IdempotentInsert}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.{ExecutionContext, Future}

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

  private val insertTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("crypto-public-store-insert")
  private val queryTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("crypto-public-store-query")

  private implicit val setParameterEncryptionPublicKey: SetParameter[EncryptionPublicKey] =
    EncryptionPublicKey.getVersionedSetParameter(releaseProtocolVersion.v)
  private implicit val setParameterSigningPublicKey: SetParameter[SigningPublicKey] =
    SigningPublicKey.getVersionedSetParameter(releaseProtocolVersion.v)

  private def queryKeys[K: GetResult](purpose: KeyPurpose): DbAction.ReadOnly[Set[K]] =
    sql"select data, name from crypto_public_keys where purpose = $purpose"
      .as[K]
      .map(_.toSet)

  private def queryKeyO[K <: PublicKeyWithName: GetResult](
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[Option[K]] =
    sql"select data, name from crypto_public_keys where key_id = $keyId and purpose = $purpose"
      .as[K]
      .headOption

  private def queryKey[K <: PublicKeyWithName: GetResult](
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[K] =
    sql"select data, name from crypto_public_keys where key_id = $keyId and purpose = $purpose"
      .as[K]
      .head

  private def insertKey[K <: PublicKey: SetParameter, KN <: PublicKeyWithName: GetResult](
      key: K,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storage.queryAndUpdate(
      IdempotentInsert.insertVerifyingConflicts(
        storage,
        "crypto_public_keys ( key_id )",
        sql"crypto_public_keys (key_id, purpose, data, name) values (${key.id}, ${key.purpose}, $key, $name)",
        queryKey(key.id, key.purpose),
      )(
        // An error is thrown if, and only if, the key we want to insert has the same id but different key payloads.
        existingKey => existingKey.publicKey == key,
        _ => s"Existing public key for ${key.id} is different than inserted key",
      ),
      functionFullName,
    )

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, SigningPublicKeyWithName] =
    storage
      .querySingle(
        queryKeyO[SigningPublicKeyWithName](signingKeyId, KeyPurpose.Signing),
        functionFullName,
      )

  override def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, EncryptionPublicKeyWithName] =
    storage
      .querySingle(
        queryKeyO[EncryptionPublicKeyWithName](encryptionKeyId, KeyPurpose.Encryption),
        functionFullName,
      )

  override protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    insertKey[SigningPublicKey, SigningPublicKeyWithName](key, name)

  override protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    insertKey[EncryptionPublicKey, EncryptionPublicKeyWithName](key, name)

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): Future[Set[SigningPublicKeyWithName]] =
    storage.query(
      queryKeys[SigningPublicKeyWithName](KeyPurpose.Signing),
      functionFullName,
    )

  override private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): Future[Set[EncryptionPublicKeyWithName]] =
    storage
      .query(
        queryKeys[EncryptionPublicKeyWithName](KeyPurpose.Encryption),
        functionFullName,
      )

  override protected def deleteKeyInternal(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): Future[Unit] =
    storage
      .update_(
        sqlu"delete from crypto_public_keys where key_id = $keyId",
        functionFullName,
      )
}
