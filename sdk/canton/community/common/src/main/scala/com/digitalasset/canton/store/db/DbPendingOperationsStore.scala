// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.data.{EitherT, OptionT}
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.PendingOperation.{
  ConflictingPendingOperationError,
  PendingOperationTriggerType,
}
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasProtocolVersionedWrapper, VersioningCompanion}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter, TransactionIsolation}

import java.sql.Types
import scala.annotation.unused
import scala.concurrent.ExecutionContext

class DbPendingOperationsStore[Op <: HasProtocolVersionedWrapper[Op]](
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val opCompanion: VersioningCompanion[Op],
)(implicit val executionContext: ExecutionContext)
    extends DbStore
    with PendingOperationStore[Op] {

  import storage.api.*

  override def insert(
      operation: PendingOperation[Op]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConflictingPendingOperationError, Unit] = {
    implicit val pendingOperationGetResult: GetResult[PendingOperation[Op]] =
      DbPendingOperationsStore.getPendingOperationResult(opCompanion.fromTrustedByteString)

    val readAction =
      sql"""
        select operation_trigger, operation_name, operation_key, operation, synchronizer_id
        from common_pending_operations
        where synchronizer_id = ${operation.synchronizerId}
        and operation_key = ${operation.key}
        and operation_name = ${operation.name.unwrap}
      """.as[PendingOperation[Op]].headOption

    val transaction = readAction.flatMap {
      case Some(existingOperation) if existingOperation != operation =>
        DBIO.successful(
          Left(
            ConflictingPendingOperationError(
              operation.synchronizerId,
              operation.key,
              operation.name,
            )
          )
        )

      case Some(_) => DBIO.successful(Right(()))

      case None =>
        import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterByteString
        @unused
        implicit val setParameter: SetParameter[Op] = (v: Op, pp) => pp >> v.toByteString
        @unused
        implicit val setOperationTriggerType: SetParameter[PendingOperationTriggerType] =
          DbPendingOperationsStore.setOperationTriggerType(storage)

        sqlu"""
          insert into common_pending_operations
            (operation_trigger, operation_name, operation_key, operation, synchronizer_id)
          values
            (
              ${operation.trigger},
              ${operation.name.unwrap},
              ${operation.key},
              ${operation.operation},
              ${operation.synchronizerId}
            )
        """.map(_ => Right(()))
    }

    EitherT(
      storage.queryAndUpdate(
        transaction.transactionally.withTransactionIsolation(TransactionIsolation.Serializable),
        functionFullName,
      )
    )
  }

  override def delete(
      synchronizerId: SynchronizerId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val deleteAction =
      sqlu"""
        delete from common_pending_operations
        where synchronizer_id = $synchronizerId
        and operation_key = $operationKey
        and operation_name = ${operationName.unwrap}
      """
    storage.update_(deleteAction, functionFullName)
  }

  override def get(
      synchronizerId: SynchronizerId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, PendingOperation[Op]] = {
    implicit val pendingOperationGetResult: GetResult[PendingOperation[Op]] =
      DbPendingOperationsStore.getPendingOperationResult(opCompanion.fromTrustedByteString)

    val selectAction =
      sql"""
        select operation_trigger, operation_name, operation_key, operation, synchronizer_id
        from common_pending_operations
        where synchronizer_id = $synchronizerId
        and operation_key = $operationKey
        and operation_name = ${operationName.unwrap}
      """.as[PendingOperation[Op]].headOption
    OptionT.apply(storage.query(selectAction, functionFullName))
  }

}

object DbPendingOperationsStore {

  def getPendingOperationResult[Op <: HasProtocolVersionedWrapper[Op]](
      operationDeserializer: ByteString => ParsingResult[Op]
  ): GetResult[PendingOperation[Op]] = GetResult { r =>
    import DbStorage.Implicits.getResultByteString

    PendingOperation.tryCreate(
      trigger = r.<<[String],
      name = r.<<[String],
      key = r.<<[String],
      operationBytes = r.<<[ByteString],
      operationDeserializer,
      synchronizerId = r.<<[String],
    )
  }

  // For PostgreSQL, `setObject` with `Types.OTHER` is required for handling the custom enum type
  def setOperationTriggerType(storage: DbStorage): SetParameter[PendingOperationTriggerType] =
    storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        (t, pp) => pp.setObject(t.asString, Types.OTHER)
      case _: DbStorage.Profile.H2 =>
        (t, pp) => pp.setString(t.asString)
    }

}
