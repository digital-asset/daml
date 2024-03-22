// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Monad
import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.store.DomainAliasAndIdStore.{
  DomainAliasAlreadyAdded,
  DomainIdAlreadyAdded,
  Error,
}
import com.digitalasset.canton.participant.store.RegisteredDomainsStore
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class DbRegisteredDomainsStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisteredDomainsStore
    with DbStore {
  import DomainAlias.*
  import DomainId.*
  import storage.api.*

  private val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("registered-domains-store")

  override def addMapping(alias: DomainAlias, domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, Unit] = processingTime.eitherTEvent {
    EitherT {
      val insert = storage.profile match {
        case _: DbStorage.Profile.Postgres =>
          sqlu"""
        insert into participant_domains(alias, domain_id)
        values ($alias, $domainId)
        on conflict do nothing
        """
        case _ =>
          sqlu"""
        insert into participant_domains(alias, domain_id)
        select $alias, $domainId from dual
        where not exists (select * from participant_domains where domain_id = $domainId)
          and not exists (select * from participant_domains where alias = $alias)
          """
      }

      // Try to insert until we succeed or find a consistency error
      def step(_x: Unit): Future[Either[Unit, Either[Error, Unit]]] = {
        // Use Left for short-circuiting the checks and report an error or success and Right to continue checking.
        // We swap sides at the end
        val swapped = for {
          rowCount <- EitherT.right[Either[Error, Unit]](storage.update(insert, functionFullName))
          _ <- EitherT.cond[Future](rowCount != 1, (), Right(()))
          // We may have inserted the row even if the row count is lower. So check whether the row is actually there.
          doubleAlias <- EitherT.right[Either[Error, Unit]](
            storage.query(
              sql"select domain_id from participant_domains where alias = $alias".as[DomainId],
              functionFullName,
            )
          )
          _ <- EitherT.fromEither[Future](
            doubleAlias.headOption.fold(Either.right[Either[Error, Unit], Unit](())) {
              oldDomainId =>
                Left(
                  Either.cond(
                    oldDomainId == domainId,
                    (),
                    DomainAliasAlreadyAdded(alias, oldDomainId),
                  )
                )
            }
          )
          doubleDomainId <- EitherT.right[Either[Error, Unit]](
            storage.query(
              sql"select alias from participant_domains where domain_id = $domainId"
                .as[DomainAlias],
              functionFullName,
            )
          )
          _ <- EitherT.fromEither[Future](
            doubleDomainId.headOption.fold(Either.right[Either[Error, Unit], Unit](())) {
              oldAlias =>
                Left(Either.cond(oldAlias == alias, (), DomainIdAlreadyAdded(domainId, oldAlias)))
            }
          )
        } yield () // We get here only if rowCount is not 1 and neither the alias nor the domain was found. So try inserting again.
        swapped.swap.value
      }
      Monad[Future].tailRecM(())(step)
    }
  }

  override def aliasToDomainIdMap(implicit
      traceContext: TraceContext
  ): Future[Map[DomainAlias, DomainId]] =
    processingTime.event {
      storage
        .query(
          sql"""select alias, domain_id from participant_domains"""
            .as[(DomainAlias, DomainId)]
            .map(_.toMap),
          functionFullName,
        )
    }
}
