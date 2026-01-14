// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.RegisteredSynchronizersStore
import com.digitalasset.canton.participant.store.SynchronizerAliasAndIdStore.{
  Error,
  InconsistentLogicalSynchronizerIds,
  SynchronizerIdAlreadyAdded,
}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import slick.dbio
import slick.dbio.DBIOAction
import slick.jdbc.TransactionIsolation

import scala.concurrent.ExecutionContext

class DbRegisteredSynchronizersStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisteredSynchronizersStore
    with DbStore {
  import SynchronizerAlias.*
  import storage.api.*
  import com.digitalasset.canton.resource.DbStorage.dbEitherT
  import DbStorage.Implicits.*

  override def addMapping(alias: SynchronizerAlias, psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {
    lazy val insert = sqlu"""
        insert into par_registered_synchronizers(synchronizer_alias, physical_synchronizer_id)
        values ($alias, $psid)
        on conflict do nothing
        """

    def checkInsertion(nrRows: Int): EitherT[DBIO, Error, Unit] = nrRows match {
      case 1 => EitherT.pure[DBIO, Error](())
      case 0 =>
        for {
          retrievedResultO <- dbEitherT[Error](
            sql"select synchronizer_alias from par_registered_synchronizers where physical_synchronizer_id=$psid"
              .as[SynchronizerAlias]
          ).map(_.headOption)

          _ <- retrievedResultO match {
            case None =>
              EitherT.liftF[DBIO, Error, Unit](
                DBIOAction.failed(
                  new IllegalStateException(
                    s"No existing synchronizer connection config found for alias $alias and id $psid but failed to insert"
                  )
                )
              )

            case Some(existingAlias) =>
              EitherT.fromEither[DBIO](
                Either.cond(
                  existingAlias == alias,
                  (),
                  SynchronizerIdAlreadyAdded(psid, existingAlias): Error,
                )
              )
          }
        } yield ()

      case _ =>
        EitherT.liftF[DBIO, Error, Unit](
          DBIOAction.failed(
            new IllegalStateException(s"Updated more than 1 row for connection configs: $nrRows")
          )
        )
    }

    val queries = for {
      _ <- checkAliasConsistent(alias, psid)
      _ <- checkLogicalIdConsistent(alias, psid)
      nrRows <- dbEitherT[Error](insert)
      _ <- checkInsertion(nrRows)
    } yield ()

    EitherT(
      storage.queryAndUpdate(
        queries.value.transactionally.withTransactionIsolation(TransactionIsolation.Serializable),
        functionFullName,
      )
    )
  }

  // Ensure this PSId is not already registered with another alias
  private def checkAliasConsistent(
      alias: SynchronizerAlias,
      synchronizerId: PhysicalSynchronizerId,
  ): EitherT[dbio.DBIO, Error, Unit] = for {
    existingAliases <- dbEitherT[Error](
      sql"select synchronizer_alias from par_registered_synchronizers where physical_synchronizer_id=$synchronizerId"
        .as[SynchronizerAlias]
    )

    _ <- existingAliases.headOption match {
      case None => EitherT.pure[DBIO, Error](())
      case Some(`alias`) => EitherT.pure[DBIO, Error](())
      case Some(otherAlias) =>
        EitherT.leftT[DBIO, Unit](SynchronizerIdAlreadyAdded(synchronizerId, otherAlias): Error)
    }
  } yield ()

  // Check that logical IDs agree for the alias
  private def checkLogicalIdConsistent(
      alias: SynchronizerAlias,
      synchronizerId: PhysicalSynchronizerId,
  ): EitherT[dbio.DBIO, Error, Unit] = for {
    psidsForAlias <- dbEitherT[Error](
      sql"select physical_synchronizer_id from par_registered_synchronizers where synchronizer_alias=$alias"
        .as[PhysicalSynchronizerId]
    )

    _ <- EitherT.fromEither[DBIO](
      psidsForAlias
        .find(_.logical != synchronizerId.logical)
        .map(existing =>
          InconsistentLogicalSynchronizerIds(
            alias = alias,
            newPSId = synchronizerId,
            existingPSId = existing,
          )
        )
        .toLeft(())
        .leftWiden[Error]
    )
  } yield ()

  override def aliasToSynchronizerIdMap(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerAlias, NonEmpty[Set[PhysicalSynchronizerId]]]] =
    storage
      .query(
        sql"""select synchronizer_alias, physical_synchronizer_id from par_registered_synchronizers"""
          .as[(SynchronizerAlias, PhysicalSynchronizerId)],
        functionFullName,
      )
      .map(_.groupMap1 { case (alias, _) => alias } { case (_, psid) => psid }.map {
        case (alias, ids) => alias -> ids.toSet
      })

}
