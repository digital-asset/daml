// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.store.PendingOperation.ConflictingPendingOperationError
import com.digitalasset.canton.store.memory.InMemoryPendingOperationStore.compositeKey
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasProtocolVersionedWrapper, VersioningCompanion}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, blocking}
import scala.util.Try

class InMemoryPendingOperationStore[Op <: HasProtocolVersionedWrapper[Op]](
    override protected val opCompanion: VersioningCompanion[Op]
)(implicit
    val executionContext: ExecutionContext
) extends PendingOperationStore[Op] {

  // Allows tests to bypass validation and insert malformed data into the store
  @VisibleForTesting
  private[memory] val store =
    TrieMap.empty[
      (SynchronizerId, String, NonEmptyString),
      InMemoryPendingOperationStore.StoredPendingOperation,
    ]

  override def insert(
      operation: PendingOperation[Op]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConflictingPendingOperationError, Unit] =
    EitherT.fromEither[FutureUnlessShutdown] {
      blocking {
        store.synchronized {
          val existingOperationO =
            store.get(operation.compositeKey).map(_.toPendingOperation(opCompanion))
          existingOperationO match {
            case Some(existingOperation) if existingOperation != operation =>
              Left(
                ConflictingPendingOperationError(
                  operation.synchronizerId,
                  operation.key,
                  operation.name,
                )
              )
            case _ =>
              val storedOperation =
                InMemoryPendingOperationStore.StoredPendingOperation.fromPendingOperation(operation)
              store.putIfAbsent(operation.compositeKey, storedOperation).discard
              Right(())
          }
        }
      }
    }

  override def delete(
      synchronizerId: SynchronizerId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    store.remove(compositeKey(synchronizerId, operationKey, operationName)).discard
    FutureUnlessShutdown.pure(())
  }

  override def get(
      synchronizerId: SynchronizerId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, PendingOperation[Op]] = {
    val resultF = FutureUnlessShutdown.fromTry(Try {
      store
        .get(compositeKey(synchronizerId, operationKey, operationName))
        .map(_.toPendingOperation(opCompanion))
    })
    OptionT(resultF)
  }
}

object InMemoryPendingOperationStore {

  /*
   * The following members are exposed with `private[memory]` visibility for testing only.
   * This allows tests to bypass validation and insert malformed data to verify
   * the store's behavior when reading corrupt records.
   */
  @VisibleForTesting
  private[memory] final case class StoredPendingOperation(
      trigger: String,
      serializedSynchronizerId: String,
      key: String,
      name: String,
      serializedOperation: ByteString,
  ) {
    def toPendingOperation[Op <: HasProtocolVersionedWrapper[Op]](
        opCompanion: VersioningCompanion[Op]
    ): PendingOperation[Op] =
      PendingOperation.tryCreate(
        trigger,
        name,
        key,
        serializedOperation,
        opCompanion.fromTrustedByteString,
        serializedSynchronizerId,
      )
  }

  @VisibleForTesting
  private[memory] object StoredPendingOperation {
    def fromPendingOperation[Op <: HasProtocolVersionedWrapper[Op]](
        po: PendingOperation[Op]
    ): StoredPendingOperation =
      StoredPendingOperation(
        po.trigger.asString,
        po.synchronizerId.toProtoPrimitive,
        po.key,
        po.name.unwrap,
        po.operation.toByteString,
      )
  }

  private def compositeKey(
      synchronizerId: SynchronizerId,
      operationKey: String,
      operationName: NonEmptyString,
  ): (SynchronizerId, String, NonEmptyString) =
    (synchronizerId, operationKey, operationName)
}
