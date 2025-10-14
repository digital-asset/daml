// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.PendingOperation.{
  ConflictingPendingOperationError,
  PendingOperationTriggerType,
}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasProtocolVersionedWrapper, VersioningCompanion}
import com.google.protobuf.ByteString

import scala.util.Try

trait PendingOperationStore[Op <: HasProtocolVersionedWrapper[Op]] {

  protected def opCompanion: VersioningCompanion[Op]

  /** Atomically stores a pending operation, returning an error if a conflicting operation already
    * exists.
    *
    * This check-and-insert operation is performed within a serializable transaction to prevent race
    * conditions. The behavior depends on whether an operation with the same unique key
    * (`synchronizerId`, `key`, `name`) already exists in the store:
    *   - If no operation with the key exists, the new operation is inserted.
    *   - If an '''identical''' operation already exists, the operation succeeds without making
    *     changes.
    *   - If an operation with the same key but '''different''' data exists, the operation fails
    *     with an error.
    *
    * @param operation
    *   The `PendingOperation` to insert.
    * @param traceContext
    *   The context for tracing and logging.
    * @return
    *   An `EitherT` that completes with:
    *   - `Right(())` if the operation was successfully stored or an identical one already existed.
    *   - `Left(ConflictingPendingOperationError)` if a conflicting operation was found.
    */
  def insert(operation: PendingOperation[Op])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConflictingPendingOperationError, Unit]

  /** Deletes a pending operation identified by its unique composite key (`synchronizerId`,
    * `operationKey`, `operationName`).
    *
    * This operation is '''idempotent'''. It succeeds regardless of whether the record existed prior
    * to the call.
    *
    * @param synchronizerId
    *   The ID of the synchronizer scoping the operation application.
    * @param operationKey
    *   A key to distinguish between multiple instances of the same operation.
    * @param operationName
    *   The name of the operation to be executed.
    * @param traceContext
    *   The context for tracing and logging.
    * @return
    *   A future that completes when the deletion has finished.
    */
  def delete(
      synchronizerId: SynchronizerId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Fetches a pending operation by its unique composite key (`synchronizerId`, `operationKey`,
    * `operationName`).
    *
    * @param synchronizerId
    *   The ID of the synchronizer scoping the operation application.
    * @param operationKey
    *   A key to distinguish between multiple instances of the same operation.
    * @param operationName
    *   The name of the operation to be executed.
    * @param traceContext
    *   The context for tracing and logging.
    * @return
    *   A future that completes with `Some(operation)` if found and valid, `None` if not found, or
    *   fails with a `DbDeserializationException` if the stored data is corrupt.
    */
  def get(
      synchronizerId: SynchronizerId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, PendingOperation[Op]]

}

final case class PendingOperation[Op <: HasProtocolVersionedWrapper[Op]] private[store] (
    trigger: PendingOperationTriggerType,
    name: NonEmptyString,
    key: String,
    operation: Op,
    synchronizerId: SynchronizerId,
) {
  private[store] def compositeKey: (SynchronizerId, String, NonEmptyString) =
    (synchronizerId, key, name)

}

object PendingOperation {

  private[store] def create[Op <: HasProtocolVersionedWrapper[Op]](
      trigger: String,
      name: String,
      key: String,
      operationBytes: ByteString,
      operationDeserializer: ByteString => ParsingResult[Op],
      synchronizerId: String,
  ): Either[String, PendingOperation[Op]] =
    Try(
      tryCreate(trigger, name, key, operationBytes, operationDeserializer, synchronizerId)
    ).toEither.leftMap(_.getMessage)

  /** Factory method to create an instance from database values. Performs validation and throws
    * DbDeserializationException on failure.
    */
  private[store] def tryCreate[Op <: HasProtocolVersionedWrapper[Op]](
      trigger: String,
      name: String,
      key: String,
      operationBytes: ByteString,
      operationDeserializer: ByteString => ParsingResult[Op],
      synchronizerId: String,
  ): PendingOperation[Op] = {
    val validTrigger = PendingOperationTriggerType
      .fromString(trigger)
      .getOrElse(
        throw new DbDeserializationException(
          s"Invalid pending_operation_trigger_type in database: $trigger"
        )
      )
    val validName = if (name.isBlank) {
      throw new DbDeserializationException(
        s"Missing pending operation name (blank): $name"
      )
    } else {
      NonEmptyString.tryCreate(name)
    }
    val validOperation = operationDeserializer(operationBytes).valueOr(error =>
      throw new DbDeserializationException(
        s"Failed to deserialize pending operation byte string: $error"
      )
    )
    val validSynchronizerId = SynchronizerId(
      UniqueIdentifier.deserializeFromDb(synchronizerId) // throws DbDeserializationException
    )

    PendingOperation(validTrigger, validName, key, validOperation, validSynchronizerId)
  }

  sealed trait PendingOperationTriggerType extends Product with Serializable {
    def asString: String
  }

  object PendingOperationTriggerType {
    case object SynchronizerReconnect extends PendingOperationTriggerType {
      override def asString: String = "synchronizer_reconnect"
    }

    def fromString(s: String): Either[String, PendingOperationTriggerType] = s match {
      case "synchronizer_reconnect" => Right(SynchronizerReconnect)
      case _ => Left(s"Unknown pending operation trigger type: $s")
    }
  }

  /** Signals a failed attempt to insert a pending operation because it conflicts with an existing
    * one.
    *
    * A conflict occurs when an operation with the same unique key (`synchronizerId`, `key`, `name`)
    * already exists in the store but contains different data.
    *
    * @param synchronizerId
    *   The unique identifier of the synchronizer that owns the operation.
    * @param key
    *   The key that uniquely identifies the pending operation within its scope.
    * @param name
    *   The name describing the type of pending operation.
    */
  final case class ConflictingPendingOperationError(
      synchronizerId: SynchronizerId,
      key: String,
      name: NonEmptyString,
  )

}
