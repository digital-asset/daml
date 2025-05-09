// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Monad
import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.RegisteredSynchronizersStore
import com.digitalasset.canton.participant.store.SynchronizerAliasAndIdStore.{
  Error,
  SynchronizerAliasAlreadyAdded,
  SynchronizerIdAlreadyAdded,
}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class DbRegisteredSynchronizersStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisteredSynchronizersStore
    with DbStore {
  import SynchronizerAlias.*
  import SynchronizerId.*
  import storage.api.*

  override def addMapping(alias: SynchronizerAlias, synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] =
    EitherT {
      val insert = storage.profile match {
        case _: DbStorage.Profile.Postgres =>
          sqlu"""
        insert into par_synchronizers(synchronizer_alias, synchronizer_id)
        values ($alias, $synchronizerId)
        on conflict do nothing
        """
        case _ =>
          sqlu"""
        insert into par_synchronizers(synchronizer_alias, synchronizer_id)
        select $alias, $synchronizerId from dual
        where not exists (select * from par_synchronizers where synchronizer_id = $synchronizerId)
          and not exists (select * from par_synchronizers where synchronizer_alias = $alias)
          """
      }

      // Try to insert until we succeed or find a consistency error
      def step(): FutureUnlessShutdown[Either[Unit, Either[Error, Unit]]] = {
        // Use Left for short-circuiting the checks and report an error or success and Right to continue checking.
        // We swap sides at the end
        val swapped = for {
          rowCount <- EitherT.right[Either[Error, Unit]](storage.update(insert, functionFullName))
          _ <- EitherT.cond[FutureUnlessShutdown](rowCount != 1, (), Either.unit)
          // We may have inserted the row even if the row count is lower. So check whether the row is actually there.
          doubleAlias <- EitherT.right[Either[Error, Unit]](
            storage.query(
              sql"select synchronizer_id from par_synchronizers where synchronizer_alias = $alias"
                .as[SynchronizerId],
              functionFullName,
            )
          )
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            doubleAlias.headOption.fold(Either.right[Either[Error, Unit], Unit](())) {
              oldSynchronizerId =>
                Left(
                  Either.cond(
                    oldSynchronizerId == synchronizerId,
                    (),
                    SynchronizerAliasAlreadyAdded(alias, oldSynchronizerId),
                  )
                )
            }
          )
          doubleSynchronizerId <- EitherT.right[Either[Error, Unit]](
            storage.query(
              sql"select synchronizer_alias from par_synchronizers where synchronizer_id = $synchronizerId"
                .as[SynchronizerAlias],
              functionFullName,
            )
          )
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            doubleSynchronizerId.headOption.fold(Either.right[Either[Error, Unit], Unit](())) {
              oldAlias =>
                Left(
                  Either.cond(
                    oldAlias == alias,
                    (),
                    SynchronizerIdAlreadyAdded(synchronizerId, oldAlias),
                  )
                )
            }
          )
        } yield () // We get here only if rowCount is not 1 and neither the alias nor the synchronizer was found. So try inserting again.
        swapped.swap.value
      }
      Monad[FutureUnlessShutdown].tailRecM(())(_ => step())
    }

  override def aliasToSynchronizerIdMap(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerAlias, SynchronizerId]] =
    storage
      .query(
        sql"""select synchronizer_alias, synchronizer_id from par_synchronizers"""
          .as[(SynchronizerAlias, SynchronizerId)]
          .map(_.toMap),
        functionFullName,
      )

}
