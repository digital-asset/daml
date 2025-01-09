// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SynchronizerParameterStore
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.SetParameter

import scala.concurrent.ExecutionContext

class DbSynchronizerParameterStore(
    synchronizerId: SynchronizerId,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SynchronizerParameterStore
    with DbStore {

  import storage.api.*
  import storage.converters.*

  private implicit val setParameterStaticSynchronizerParameters
      : SetParameter[StaticSynchronizerParameters] =
    StaticSynchronizerParameters.getVersionedSetParameter

  def setParameters(
      newParameters: StaticSynchronizerParameters
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // We do not check equality of the parameters on the serialized format in the DB query because serialization may
    // be different even though the parameters are the same
    val query =
      sqlu"""insert into par_static_domain_parameters(synchronizer_id, params)
             values ($synchronizerId, $newParameters)
             on conflict do nothing"""

    storage.update(query, functionFullName).flatMap { rowCount =>
      if (rowCount == 1) FutureUnlessShutdown.unit
      else
        lastParameters.flatMap {
          case None =>
            FutureUnlessShutdown.failed(
              new IllegalStateException(
                "Insertion of synchronizer parameters failed even though no synchronizer parameters are present"
              )
            )
          case Some(old) if old == newParameters => FutureUnlessShutdown.unit
          case Some(old) =>
            FutureUnlessShutdown.failed(
              new IllegalArgumentException(
                s"Cannot overwrite old synchronizer parameters $old with $newParameters."
              )
            )
        }
    }
  }

  def lastParameters(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[StaticSynchronizerParameters]] =
    storage
      .query(
        sql"select params from par_static_domain_parameters where synchronizer_id=$synchronizerId"
          .as[StaticSynchronizerParameters]
          .headOption,
        functionFullName,
      )
}
