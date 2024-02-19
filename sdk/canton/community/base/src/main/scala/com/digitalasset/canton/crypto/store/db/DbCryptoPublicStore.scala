// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
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
    sql"select data, name from common_crypto_public_keys where purpose = $purpose"
      .as[K]
      .map(_.toSet)

  private def queryKey[K <: PublicKeyWithName: GetResult](
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[Option[K]] =
    sql"select data, name from common_crypto_public_keys where key_id = $keyId and purpose = $purpose"
      .as[K]
      .headOption

  private def insertKeyUpdate[K <: PublicKey: SetParameter, KN <: PublicKeyWithName: GetResult](
      key: K,
      name: Option[KeyName],
  ): DbAction.WriteOnly[Int] =
    storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
               /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( common_crypto_public_keys ( key_id ) ) */
               into common_crypto_public_keys (key_id, purpose, data, name)
           values (${key.id}, ${key.purpose}, $key, $name)"""
      case _ =>
        sqlu"""insert into common_crypto_public_keys (key_id, purpose, data, name)
           values (${key.id}, ${key.purpose}, $key, $name)
           on conflict do nothing"""
    }

  private def insertKey[K <: PublicKey: SetParameter, KN <: PublicKeyWithName: GetResult](
      key: K,
      name: Option[KeyName],
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPublicStoreError, Unit] =
    insertTime.eitherTEvent {
      for {
        inserted <- EitherT.right(storage.update(insertKeyUpdate(key, name), functionFullName))
        res <-
          if (inserted == 0) {
            // If no key was inserted by the insert query, check that the existing value matches
            storage
              .querySingle(queryKey(key.id, key.purpose), functionFullName)
              .toRight(
                CryptoPublicStoreError.FailedToInsertKey(key.id, "No key inserted and no key found")
              )
              .flatMap { existingKey =>
                EitherT
                  .cond[Future](
                    existingKey.publicKey == key && existingKey.name == name,
                    (),
                    CryptoPublicStoreError.KeyAlreadyExists(key.id, existingKey.name.map(_.unwrap)),
                  )
                  .leftWiden[CryptoPublicStoreError]
              }
          } else EitherT.rightT[Future, CryptoPublicStoreError](())
      } yield res
    }

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[SigningPublicKeyWithName]] =
    EitherTUtil.fromFuture(
      storage
        .querySingle(
          queryKey[SigningPublicKeyWithName](signingKeyId, KeyPurpose.Signing),
          functionFullName,
        )
        .value,
      err => CryptoPublicStoreError.FailedToReadKey(signingKeyId, err.toString),
    )

  override def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[EncryptionPublicKeyWithName]] =
    EitherTUtil.fromFuture(
      storage
        .querySingle(
          queryKey[EncryptionPublicKeyWithName](encryptionKeyId, KeyPurpose.Encryption),
          functionFullName,
        )
        .value,
      err => CryptoPublicStoreError.FailedToReadKey(encryptionKeyId, err.toString),
    )

  override protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit] =
    insertKey[SigningPublicKey, SigningPublicKeyWithName](key, name)

  override protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit] =
    insertKey[EncryptionPublicKey, EncryptionPublicKeyWithName](key, name)

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[SigningPublicKeyWithName]] =
    EitherTUtil.fromFuture(
      queryTime.event(
        storage.query(queryKeys[SigningPublicKeyWithName](KeyPurpose.Signing), functionFullName)
      ),
      err => CryptoPublicStoreError.FailedToListKeys(err.toString),
    )

  override private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[EncryptionPublicKeyWithName]] =
    EitherTUtil
      .fromFuture(
        queryTime.event(
          storage
            .query(queryKeys[EncryptionPublicKeyWithName](KeyPurpose.Encryption), functionFullName)
        ),
        err => CryptoPublicStoreError.FailedToListKeys(err.toString),
      )
}
