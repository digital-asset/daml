// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.Implicits.*
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.dbio.DBIOAction
import slick.jdbc.GetResult
import slick.sql.SqlAction

import scala.concurrent.{ExecutionContext, Future}

/** Represents the data to be stored in the crypto_private_keys table.
  * If wrapperKeyId is set (Some(wrapperKeyId)) then the data field is encrypted
  * otherwise (None), then the data field is in plaintext.
  * @param id canton identifier for a private key
  * @param data a ByteString that stores either: (1) the serialized private key case class, which contains the private
  *             key plus metadata, or (2) the above proto serialization but encrypted with the wrapper key if present.
  * @param purpose to identify if the key is for signing or encryption
  * @param name an alias name for the private key
  * @param wrapperKeyId identifies what is the key being used to encrypt the data field. If empty, data is
  *                     unencrypted.
  */
final case class StoredPrivateKey(
    id: Fingerprint,
    data: ByteString,
    purpose: KeyPurpose,
    name: Option[KeyName],
    wrapperKeyId: Option[String300],
) extends Product
    with Serializable {

  def isEncrypted: Boolean = { this.wrapperKeyId.isDefined }

}

object StoredPrivateKey {
  implicit def getResultStoredPrivateKey(implicit
      getResultByteString: GetResult[ByteString]
  ): GetResult[StoredPrivateKey] =
    GetResult { r =>
      StoredPrivateKey(r.<<, r.<<, r.<<, r.<<, r.<<)
    }
}

class DbCryptoPrivateStore(
    override protected val storage: DbStorage,
    override protected val releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends CryptoPrivateStoreExtended
    with DbStore {

  import storage.api.*

  private val insertTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("crypto-private-store-insert")
  private val queryTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("crypto-private-store-query")

  private def queryKeys(purpose: KeyPurpose): DbAction.ReadOnly[Set[StoredPrivateKey]] =
    sql"select key_id, data, purpose, name, wrapper_key_id from crypto_private_keys where purpose = $purpose"
      .as[StoredPrivateKey]
      .map(_.toSet)

  private def queryKey(
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[Option[StoredPrivateKey]] =
    sql"select key_id, data, purpose, name, wrapper_key_id from crypto_private_keys where key_id = $keyId and purpose = $purpose"
      .as[StoredPrivateKey]
      .headOption

  private def insertKeyUpdate(
      key: StoredPrivateKey
  ): DbAction.WriteOnly[Int] = {
    storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
               /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( CRYPTO_PRIVATE_KEYS ( key_id ) ) */
               into crypto_private_keys (key_id, purpose, data, name, wrapper_key_id)
           values (${key.id}, ${key.purpose}, ${key.data}, ${key.name}, ${key.wrapperKeyId})"""
      case _ =>
        sqlu"""insert into crypto_private_keys (key_id, purpose, data, name, wrapper_key_id)
           values (${key.id}, ${key.purpose}, ${key.data}, ${key.name}, ${key.wrapperKeyId})
           on conflict do nothing"""
    }
  }

  private def insertKey(key: StoredPrivateKey)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] = {

    def equalKeys(existingKey: StoredPrivateKey, newKey: StoredPrivateKey): Boolean = {
      if (existingKey.wrapperKeyId.isEmpty) {
        existingKey.data == newKey.data &&
        existingKey.name == newKey.name &&
        existingKey.purpose == newKey.purpose
      } else {
        // in the encrypted case we cannot compare the contents of data directly, we simply do not allow
        // keys having the same name and purpose
        existingKey.name == newKey.name &&
        existingKey.purpose == newKey.purpose
      }
    }

    insertTime.eitherTEvent {
      for {
        inserted <- EitherT.right(
          storage.update(insertKeyUpdate(key), functionFullName)
        )
        res <-
          if (inserted == 0) {
            // If no key was inserted by the insert query, check that the existing value matches
            storage
              .querySingle(queryKey(key.id, key.purpose), functionFullName)
              // If we don't find the duplicate key, it may have been concurrently deleted and we could retry to insert it.
              .toRight(
                CryptoPrivateStoreError
                  .FailedToInsertKey(key.id, "No key inserted and no key found")
              )
              .flatMap { existingKey =>
                EitherT
                  .cond[Future](
                    equalKeys(existingKey, key),
                    (),
                    CryptoPrivateStoreError.KeyAlreadyExists(key.id, existingKey.name.map(_.unwrap)),
                  )
                  .leftWiden[CryptoPrivateStoreError]
              }
          } else EitherT.rightT[Future, CryptoPrivateStoreError](())
      } yield res
    }
  }

  private[crypto] def readPrivateKey(
      keyId: Fingerprint,
      purpose: KeyPurpose,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[StoredPrivateKey]] =
    EitherTUtil.fromFuture(
      storage
        .querySingle(
          queryKey(keyId, purpose),
          functionFullName,
        )
        .value,
      err => CryptoPrivateStoreError.FailedToReadKey(keyId, err.toString),
    )

  private[crypto] def writePrivateKey(
      key: StoredPrivateKey
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    insertKey(key)

  @VisibleForTesting
  private[canton] def listPrivateKeys(purpose: KeyPurpose, encrypted: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[StoredPrivateKey]] =
    EitherTUtil
      .fromFuture(
        queryTime
          .event(
            storage.query(queryKeys(purpose), functionFullName)
          )
          .map(keys => keys.filter(_.isEncrypted == encrypted)),
        err => CryptoPrivateStoreError.FailedToListKeys(err.toString),
      )

  private def deleteKey(keyId: Fingerprint): SqlAction[Int, NoStream, Effect.Write] =
    sqlu"delete from crypto_private_keys where key_id = $keyId"

  /** Replaces keys but maintains their id stable, i.e. when the keys remain the same, but the
    * storage format changes (e.g. encrypting a key)
    */
  private[crypto] def replaceStoredPrivateKeys(newKeys: Seq[StoredPrivateKey])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    EitherTUtil.fromFuture(
      insertTime.event {
        storage
          .update_(
            DBIOAction
              .sequence(
                newKeys.map(key => deleteKey(key.id).andThen(insertKeyUpdate(key)))
              )
              .transactionally,
            functionFullName,
          )
      },
      err => CryptoPrivateStoreError.FailedToReplaceKeys(newKeys.map(_.id), err.toString),
    )

  private[crypto] def deletePrivateKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    EitherTUtil.fromFuture(
      insertTime.event(
        storage
          .update_(deleteKey(keyId), functionFullName)
      ),
      err => CryptoPrivateStoreError.FailedToDeleteKey(keyId, err.toString),
    )

  private[crypto] def encrypted(
      keyId: Fingerprint
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[String300]] =
    (for {
      sigStoreKey <- readPrivateKey(keyId, Signing)
      storedKey <- sigStoreKey.fold(readPrivateKey(keyId, Encryption))(key =>
        EitherT.rightT(Some(key))
      )
    } yield storedKey).flatMap {
      case Some(key) =>
        EitherT.rightT(key.wrapperKeyId)
      case None =>
        EitherT.leftT(CryptoPrivateStoreError.FailedToReadKey(keyId, s"could not read key"))
    }

  private[crypto] def getWrapperKeyId()(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[String300]] =
    EitherTUtil
      .fromFuture(
        queryTime
          .event(
            storage.query(
              {
                sql"select distinct wrapper_key_id from crypto_private_keys"
                  .as[Option[String300]]
                  .map(_.toSeq)
              },
              functionFullName,
            )
          ),
        err => CryptoPrivateStoreError.FailedToGetWrapperKeyId(err.toString),
      )
      .transform {
        case Left(err) => Left(err)
        case Right(wrapper_keys) =>
          if (wrapper_keys.size > 1)
            Left(
              CryptoPrivateStoreError
                .FailedToGetWrapperKeyId("Found more than one distinct wrapper_key_id")
            )
          else
            Right(wrapper_keys.flatten.headOption)
      }
}
