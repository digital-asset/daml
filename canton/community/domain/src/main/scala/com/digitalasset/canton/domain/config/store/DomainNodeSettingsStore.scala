// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config.store

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String1
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.{DbStorage, DbStore, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import slick.jdbc.SetParameter

import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}

object DomainNodeSettingsStore {
  def create(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): BaseNodeSettingsStore[StoredDomainNodeSettings] =
    storage match {
      case _: MemoryStorage =>
        new InMemoryBaseNodeConfigStore[StoredDomainNodeSettings](loggerFactory)
      case dbStorage: DbStorage =>
        new DbDomainNodeSettingsStore(
          dbStorage,
          timeouts,
          loggerFactory,
        )
    }
}

class DbDomainNodeSettingsStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends BaseNodeSettingsStore[StoredDomainNodeSettings]
    with DbStore {

  import storage.api.*
  import storage.converters.*

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  private val singleRowLockValue: String1 = String1.fromChar('X')

  override def fetchSettings(implicit
      traceContext: TraceContext
  ): EitherT[Future, BaseNodeSettingsStoreError, Option[StoredDomainNodeSettings]] =
    EitherTUtil.fromFuture(
      storage
        .query(
          sql"""select static_domain_parameters from domain_node_settings #${storage
              .limit(1)}""".as[StaticDomainParameters].headOption,
          functionFullName,
        )
        .map(_.map(StoredDomainNodeSettings)),
      BaseNodeSettingsStoreError.DbError,
    )

  override def saveSettings(
      settings: StoredDomainNodeSettings
  )(implicit traceContext: TraceContext): EitherT[Future, BaseNodeSettingsStoreError, Unit] = {

    val params = settings.staticDomainParameters
    @unused
    implicit val setConnParam: SetParameter[StaticDomainParameters] =
      StaticDomainParameters.getVersionedSetParameter

    EitherT.right(
      storage
        .update_(
          storage.profile match {
            case _: DbStorage.Profile.H2 =>
              sqlu"""merge into domain_node_settings
                   (lock, static_domain_parameters)
                   values
                   ($singleRowLockValue, ${params})"""
            case _: DbStorage.Profile.Postgres =>
              sqlu"""insert into domain_node_settings (static_domain_parameters)
              values (${params})
              on conflict (lock) do update set
                  static_domain_parameters = excluded.static_domain_parameters"""
            case _: DbStorage.Profile.Oracle =>
              sqlu"""merge into domain_node_settings dsc
                      using (
                        select
                          ${params} static_domain_parameters
                          from dual
                          ) excluded
                      on (dsc."LOCK" = 'X')
                       when matched then
                        update set
                            dsc.static_domain_parameters = excluded.static_domain_parameters
                       when not matched then
                        insert (static_domain_parameters)
                        values (excluded.static_domain_parameters)
                     """
          },
          functionFullName,
        )
    )
  }
}
