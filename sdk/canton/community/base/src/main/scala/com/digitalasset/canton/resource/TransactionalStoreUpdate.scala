// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.syntax.foldable.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.concurrent.ExecutionContext

/** A store update operation that can be executed transactionally with other independent update operations.
  * Transactionality means that either all updates execute or none.
  * The updates in a transactional execution must be independent of each other.
  * During such an execution, partial updates may be observable by concurrent store accesses.
  *
  * Useful for updating stores on multiple synchronizers transactionally.
  */
sealed trait TransactionalStoreUpdate {

  /** Run the transactional update as a stand-alone update. */
  def runStandalone()(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit]
}

object TransactionalStoreUpdate {

  /** Executes the unordered sequence of [[TransactionalStoreUpdate]]s transactionally,
    * i.e., either all of them succeed or none.
    *
    * @throws java.lang.IllegalArgumentException if `updates` contains several DB store updates that use different [[DbStorage]] objects.
    */
  def execute(
      updates: Seq[TransactionalStoreUpdate]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
      closeContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = updates match {
    case Seq() => FutureUnlessShutdown.unit
    case Seq(singleUpdate) => singleUpdate.runStandalone()
    case _ =>
      // We first execute all DB updates in a single DB transaction and, if successful, all in-memory updates afterwards.
      // This gives transactionality as the in-memory updates cannot fail by the requirement on `InMemoryTransactionalStoreUpdate`.

      val (dbUpdates, inMemUpdates) = updates.toList.partitionMap {
        case upd: InMemoryTransactionalStoreUpdate => Right(upd)
        case upd: DbTransactionalStoreUpdate => Left(upd)
      }

      // Make sure that all DB updates use the same Db storage object.
      // Otherwise we cannot combine the SQL updates into a single DB transaction.
      val storages = dbUpdates.map(_.storage).distinct
      require(
        storages.sizeCompare(1) <= 0,
        s"Cannot execute transactional updates across multiple DB storage objects: $storages",
      )

      storages.headOption
        .traverse_ { storage =>
          import storage.api.*
          val dbUpdatesTransaction = DBIO.seq(dbUpdates.map(_.sql)*).transactionally
          storage.update_(dbUpdatesTransaction, functionFullName)
        }
        .map(_ => inMemUpdates.foreach(_.perform()))

  }

  /** Transactional update of an in-memory store.
    *
    * @param perform The update to perform. Must always succeed and never throw an exception.
    */
  private[canton] final class InMemoryTransactionalStoreUpdate(val perform: () => Unit)
      extends TransactionalStoreUpdate {
    override def runStandalone()(implicit
        traceContext: TraceContext,
        callerCloseContext: CloseContext,
    ): FutureUnlessShutdown[Unit] =
      FutureUnlessShutdown.pure(perform())
  }

  private[canton] object InMemoryTransactionalStoreUpdate {
    def apply(perform: => Unit): InMemoryTransactionalStoreUpdate =
      new InMemoryTransactionalStoreUpdate(() => perform)
  }

  /** Transactional update of a DB store.
    *
    * @param sql The DB action to perform.
    * @param storage The [[DbStorage]] to be used to execute the `sql` action.
    */
  private[canton] final class DbTransactionalStoreUpdate(
      val sql: DBIOAction[_, NoStream, Effect.Write with Effect.Transactional],
      val storage: DbStorage,
      override protected val loggerFactory: NamedLoggerFactory,
  )(implicit val ec: ExecutionContext)
      extends TransactionalStoreUpdate
      with NamedLogging {
    override def runStandalone()(implicit
        traceContext: TraceContext,
        callerCloseContext: CloseContext,
    ): FutureUnlessShutdown[Unit] =
      storage.update_(sql, functionFullName)(traceContext, callerCloseContext)

  }
}
