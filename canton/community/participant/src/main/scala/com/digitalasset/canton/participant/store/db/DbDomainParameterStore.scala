// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.DomainParameterStore
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.SetParameter

import scala.concurrent.{ExecutionContext, Future}

class DbDomainParameterStore(
    domainId: DomainId,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends DomainParameterStore
    with DbStore {

  import storage.api.*
  import storage.converters.*

  private implicit val setParameterStaticDomainParameters: SetParameter[StaticDomainParameters] =
    StaticDomainParameters.getVersionedSetParameter

  def setParameters(
      newParameters: StaticDomainParameters
  )(implicit traceContext: TraceContext): Future[Unit] = {
    // We do not check equality of the parameters on the serialized format in the DB query because serialization may
    // be different even though the parameters are the same
    val query = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
                 /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( static_domain_parameters ( domain_id ) ) */
                 into static_domain_parameters(domain_id, params)
               values ($domainId, $newParameters)"""
      case _ =>
        sqlu"""insert into static_domain_parameters(domain_id, params)
               values ($domainId, $newParameters)
               on conflict do nothing"""
    }

    storage.update(query, functionFullName).flatMap { rowCount =>
      if (rowCount == 1) Future.unit
      else
        lastParameters.flatMap {
          case None =>
            Future.failed(
              new IllegalStateException(
                "Insertion of domain parameters failed even though no domain parameters are present"
              )
            )
          case Some(old) if old == newParameters => Future.unit
          case Some(old) =>
            Future.failed(
              new IllegalArgumentException(
                s"Cannot overwrite old domain parameters $old with $newParameters."
              )
            )
        }
    }
  }

  def lastParameters(implicit
      traceContext: TraceContext
  ): Future[Option[StaticDomainParameters]] =
    storage
      .query(
        sql"select params from static_domain_parameters where domain_id=$domainId"
          .as[StaticDomainParameters]
          .headOption,
        functionFullName,
      )
}
