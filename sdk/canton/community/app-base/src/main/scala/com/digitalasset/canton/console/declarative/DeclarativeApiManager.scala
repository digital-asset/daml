// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.declarative

import cats.data.EitherT
import com.digitalasset.canton.config.LocalNodeConfig
import com.digitalasset.canton.console.GrpcAdminCommandRunner
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.{CantonNode, CantonNodeBootstrap}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.{ExecutionContext, Future}

/** Factory to create new background processes to sync the node config state */
trait DeclarativeApiManager[NodeConfig <: LocalNodeConfig] {

  /** Verify whether the state config can be read (used on startup for early failure) */
  def verifyConfig(name: String, config: NodeConfig)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[String, Unit]

  /** Once the node is started, we turn on the node state synchronisation
    *
    * @param name
    *   the node name
    * @param config
    *   the config of the node
    * @param instance
    *   the instance running
    * @param closeContext
    *   the close context of the node. the update runner will terminate when the node is closed
    */
  def started(
      name: String,
      config: NodeConfig,
      instance: CantonNodeBootstrap[CantonNode],
  )(implicit executionContext: ExecutionContext): EitherT[Future, String, Unit]

}

object DeclarativeApiManager {

  def forParticipants[C <: LocalParticipantConfig](
      runnerFactory: String => GrpcAdminCommandRunner,
      loggerFactory: NamedLoggerFactory,
  )(implicit scheduler: ScheduledExecutorService): DeclarativeApiManager[C] =
    new DeclarativeApiManager[C] {
      override def started(
          name: String,
          config: C,
          instance: CantonNodeBootstrap[CantonNode],
      )(implicit executionContext: ExecutionContext): EitherT[Future, String, Unit] = {
        val myLoggerFactory = loggerFactory.append("participant", name)
        config.init.state
          .map { stateConfig =>
            val api = new DeclarativeParticipantApi(
              name,
              config.ledgerApi.clientConfig,
              config.adminApi.clientConfig,
              stateConfig.consistencyTimeout,
              instance.getNode.flatMap(n => Option.when(n.isActive)(n.adminToken)),
              runnerFactory,
              instance.closeContext,
              stateConfig.file,
              instance.metrics.declarativeApiMetrics,
              myLoggerFactory,
            )
            val logger = myLoggerFactory.getLogger(getClass)
            TraceContext.withNewTraceContext { implicit traceContext =>
              logger.info(
                s"Starting state refreshing for $name with ${stateConfig.file} at interval=${stateConfig.refreshInterval}"
              )
              // startup (checking config file for failures)
              api
                .startRefresh(
                  scheduler = scheduler,
                  interval = stateConfig.refreshInterval,
                )
                .map { _ =>
                  instance.registerDeclarativeChangeTrigger(() => api.runSync().discard)
                }
            }
          }
          .getOrElse(EitherTUtil.unit)
      }

      override def verifyConfig(name: String, config: C)(implicit
          errorLoggingContext: ErrorLoggingContext
      ): Either[String, Unit] =
        config.init.state
          .map(c => DeclarativeParticipantApi.readConfig(c.file).map(_ => ()))
          .getOrElse(Right(()))
    }

}
