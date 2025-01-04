// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.networking.data.db

import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.networking.data.P2pEndpointsStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.GetResult

import scala.concurrent.ExecutionContext

final class DbP2pEndpointsStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends P2pEndpointsStore[PekkoEnv]
    with DbStore {

  import storage.api.*

  private implicit val getEndpointRowResult: GetResult[Endpoint] =
    GetResult(r => Endpoint(r.nextString(), Port.tryCreate(r.nextInt())))

  private val profile = storage.profile

  override def listEndpoints(implicit
      traceContext: TraceContext
  ): PekkoEnv#FutureUnlessShutdownT[Seq[Endpoint]] =
    queryUnlessShutdown(
      selectEndpoints,
      listEndpointsActionName,
    )

  override def addEndpoint(endpoint: Endpoint)(implicit
      traceContext: TraceContext
  ): PekkoEnv#FutureUnlessShutdownT[Boolean] =
    updateUnlessShutdown(insertEndpoint(endpoint), addEndpointActionName(endpoint)).map {
      logAndCheckChangeCount
    }

  override def removeEndpoint(endpoint: Endpoint)(implicit
      traceContext: TraceContext
  ): PekkoEnv#FutureUnlessShutdownT[Boolean] =
    updateUnlessShutdown(deleteEndpoint(endpoint), removeEndpointActionName(endpoint)).map(
      logAndCheckChangeCount
    )

  override def clearAllEndpoints()(implicit
      traceContext: TraceContext
  ): PekkoEnv#FutureUnlessShutdownT[Unit] =
    updateUnlessShutdown_(clearEndpoints, clearAllEndpointsActionName)

  private def logAndCheckChangeCount(changeCount: Int)(implicit
      traceContext: TraceContext
  ): Boolean = {
    logger.debug(s"Updated $changeCount rows")
    changeCount > 0
  }

  private def selectEndpoints: DbAction.ReadOnly[Seq[Endpoint]] =
    sql"select host, port from ord_p2p_endpoints".as[Endpoint]

  private def insertEndpoint(endpoint: Endpoint): DbAction.WriteOnly[Int] = {
    val host =
      String256M.tryCreate(endpoint.host) // URL host names are limited to 253 characters anyway
    val port = endpoint.port.unwrap
    profile match {
      case _: Postgres =>
        sqlu"""insert into ord_p2p_endpoints(host, port)
               values ($host, $port)
               on conflict (host, port) do nothing"""
      case _: H2 =>
        sqlu"""merge into ord_p2p_endpoints
                 using dual on (
                   ord_p2p_endpoints.host = $host
                   and ord_p2p_endpoints.port = $port
                 )
                 when not matched then
                   insert (host, port)
                   values ($host, $port)"""
      case _ => raiseSupportedDbError
    }
  }

  private def deleteEndpoint(endpoint: Endpoint): DbAction.WriteOnly[Int] =
    sqlu"""delete from ord_p2p_endpoints
           where host = ${String256M.tryCreate(endpoint.host)} and port = ${endpoint.port.unwrap}"""

  private def clearEndpoints: DbAction.WriteOnly[Int] = sqlu"truncate table ord_p2p_endpoints"

  private def queryUnlessShutdown[X](
      action: DBIOAction[X, NoStream, Effect.Read],
      actionName: String,
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[X] =
    PekkoFutureUnlessShutdown(actionName, storage.queryUnlessShutdown(action, actionName))

  private def updateUnlessShutdown[X](
      action: DBIOAction[X, NoStream, Effect.Write & Effect.Transactional],
      actionName: String,
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[X] =
    PekkoFutureUnlessShutdown(actionName, storage.updateUnlessShutdown(action, actionName))

  private def updateUnlessShutdown_(
      action: DBIOAction[?, NoStream, Effect.Write & Effect.Transactional],
      actionName: String,
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    updateUnlessShutdown(action, actionName).map(_ => ())

  private def raiseSupportedDbError =
    sys.error("only Postgres and H2 are supported")
}
