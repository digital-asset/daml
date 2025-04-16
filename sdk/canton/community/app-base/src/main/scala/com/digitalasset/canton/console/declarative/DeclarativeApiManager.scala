// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.declarative

import cats.data.EitherT
import com.digitalasset.canton.config
import com.digitalasset.canton.config.LocalNodeConfig
import com.digitalasset.canton.console.GrpcAdminCommandRunner
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.{CantonNode, CantonNodeBootstrap}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Factory to create new background processes to sync the node config state */
trait DeclarativeApiManager[NodeConfig <: LocalNodeConfig] {

  /** Once the node is started, we turn on the node state synchronisation
    *
    * @param name
    *   the node name
    * @param initialConfig
    *   initial config
    * @param instance
    *   the instance running
    */
  def started(
      name: String,
      initialConfig: NodeConfig,
      instance: CantonNodeBootstrap[CantonNode],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, DeclarativeApiHandle[NodeConfig]]

}

object DeclarativeApiManager {

  def forParticipants(
      runnerFactory: String => GrpcAdminCommandRunner,
      consistencyTimeout: config.NonNegativeDuration,
      loggerFactory: NamedLoggerFactory,
  ): DeclarativeApiManager[ParticipantNodeConfig] =
    new DeclarativeApiManager[ParticipantNodeConfig] {
      override def started(
          name: String,
          initialConfig: ParticipantNodeConfig,
          instance: CantonNodeBootstrap[CantonNode],
      )(implicit
          executionContext: ExecutionContext,
          traceContext: TraceContext,
      ): EitherT[Future, String, DeclarativeApiHandle[ParticipantNodeConfig]] = {
        val myLoggerFactory = loggerFactory.append("participant", name)
        val api = new DeclarativeParticipantApi(
          name,
          initialConfig.ledgerApi.clientConfig,
          initialConfig.adminApi.clientConfig,
          consistencyTimeout,
          instance.getNode.flatMap(n => Option.when(n.isActive)(n.adminToken)),
          runnerFactory,
          instance.closeContext,
          instance.metrics.declarativeApiMetrics,
          myLoggerFactory,
        )

        val logger = myLoggerFactory.getLogger(getClass)
        logger.info(
          s"Starting declarative state refresh for $name"
        )
        val res = api.newConfig(initialConfig.alphaDynamic)
        if (res) {
          instance.registerDeclarativeChangeTrigger(() => api.runSync().discard)
          EitherT.rightT(DeclarativeApiHandle.mapConfig(api, _.alphaDynamic))
        } else {
          EitherT.leftT(s"Initial declarative state refresh for $name failed")
        }

      }

    }
}
