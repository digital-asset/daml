// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.{CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.crypto.store.KmsMetadataStore
import com.digitalasset.canton.crypto.store.KmsMetadataStore.KmsMetadata
import com.digitalasset.canton.crypto.{Fingerprint, KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.IdempotentInsert.insertVerifyingConflicts
import com.digitalasset.canton.resource.{DbParameterUtils, DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.{ExecutionContext, Future}

class DbKmsMetadataStore(
    override protected val storage: DbStorage,
    kmsCacheConfig: CacheConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends KmsMetadataStore
    with DbStore {

  import storage.api.*

  implicit val setSigningKeyUsageArrayOParameter
      : SetParameter[Option[NonEmpty[Set[SigningKeyUsage]]]] = (v, pp) =>
    DbParameterUtils.setArrayIntOParameterDb(v.map(_.toArray.map(_.dbType.toInt)), pp)

  implicit val getSigningKeyUsageArrayOResults: GetResult[Option[NonEmpty[Set[SigningKeyUsage]]]] =
    DbParameterUtils
      .getDataArrayOResultsDb(
        storage.profile,
        deserialize = v => SigningKeyUsage.fromDbTypeToSigningKeyUsage(v),
      )
      .andThen(_.map(arr => NonEmptyUtil.fromUnsafe(Set(arr.toSeq*))))

  implicit val kmsMetadataGetter: GetResult[KmsMetadata] =
    GetResult
      .createGetTuple4[Fingerprint, KmsKeyId, KeyPurpose, Option[NonEmpty[Set[SigningKeyUsage]]]]
      .andThen((KmsMetadata.apply _).tupled)

  private val cache =
    kmsCacheConfig.buildScaffeine().buildAsyncFuture[Fingerprint, Option[KmsMetadata]](getMetadata)

  private def getMetadata(fingerprint: Fingerprint): Future[Option[KmsMetadata]] = {
    implicit val tc: TraceContext = TraceContext.empty
    storage.query(
      sql"""SELECT fingerprint, kms_key_id, purpose, key_usage FROM common_kms_metadata_store WHERE fingerprint = $fingerprint"""
        .as[KmsMetadata]
        .headOption,
      functionFullName,
    )
  }

  /** Get a key from the store using its fingerprint
    */
  override def get(
      fingerprint: Fingerprint
  )(implicit tc: TraceContext): FutureUnlessShutdown[Option[KmsMetadataStore.KmsMetadata]] =
    FutureUnlessShutdown.outcomeF(cache.get(fingerprint))

  /** Store kms metadata in the store
    */
  override def store(metadata: KmsMetadataStore.KmsMetadata)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[Unit] =
    storage
      .queryAndUpdateUnlessShutdown(
        insertVerifyingConflicts(
          sql"""INSERT INTO common_kms_metadata_store (fingerprint, kms_key_id, purpose, key_usage)
                VALUES (${metadata.fingerprint}, ${metadata.kmsKeyId}, ${metadata.purpose}, ${metadata.usage})
                ON CONFLICT DO NOTHING""".asUpdate,
          sql"SELECT kms_key_id FROM common_kms_metadata_store WHERE fingerprint = ${metadata.fingerprint}"
            .as[KmsKeyId]
            .head,
        )(
          _ == metadata.kmsKeyId,
          existingKeyId =>
            s"Existing fingerprint ${metadata.fingerprint} has existing kms_key_id $existingKeyId but we are trying to insert ${metadata.kmsKeyId}",
        ),
        functionFullName,
      )
      .thereafter(_ => cache.synchronous().invalidate(metadata.fingerprint))

  /** Delete a key from the store using its fingerprint
    */
  override def delete(
      fingerprint: Fingerprint
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[Unit] =
    storage
      .updateUnlessShutdown_(
        sqlu"""DELETE FROM common_kms_metadata_store WHERE fingerprint = $fingerprint""",
        functionFullName,
      )
      .thereafter(_ => cache.synchronous().invalidate(fingerprint))

  /** List all keys in the store
    */
  override def list()(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[List[KmsMetadataStore.KmsMetadata]] =
    storage
      .queryUnlessShutdown(
        sql"""SELECT fingerprint, kms_key_id, purpose, key_usage FROM common_kms_metadata_store"""
          .as[KmsMetadata],
        functionFullName,
      )
      .map(_.toList)
}
