// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.PekkoExecutionSequencerPool
import com.digitalasset.canton.http.util.Logging.instanceUUIDLogCtx
import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.ledger.participant.state.WriteService
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.NoTracing
import io.grpc.Channel
import scalaz.std.anyVal.*
import scalaz.std.option.*
import scalaz.syntax.show.*

import java.nio.file.Path
import com.digitalasset.canton.config.TlsServerConfig

object HttpApiServer extends NoTracing {

  def apply(
      config: JsonApiConfig,
      httpsConfiguration: Option[TlsServerConfig],
      channel: Channel,
      writeService: WriteService,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      jsonApiMetrics: HttpApiMetrics
  ): ResourceOwner[Unit] = {
    val logger = loggerFactory.getTracedLogger(getClass)
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem("http-json-ledger-api"))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      executionSequencerFactory <- ResourceOwner.forCloseable(() =>
        new PekkoExecutionSequencerPool("httpPool")(actorSystem)
      )
      serverBinding <- instanceUUIDLogCtx(implicit loggingContextOf =>
        new HttpService(
          config,
          httpsConfiguration,
          channel,
          writeService,
          loggerFactory,
        )(
          actorSystem,
          materializer,
          executionSequencerFactory,
          loggingContextOf,
          jsonApiMetrics,
        )
      )
    } yield {
      logger.info(
        s"HTTP JSON API Server started with (address=${config.server.address: String}" +
          s", configured httpPort=${config.server.port.getOrElse(0)}" +
          s", assigned httpPort=${serverBinding.localAddress.getPort}" +
          s", portFile=${config.server.portFile: Option[Path]}" +
          s", pathPrefix=${config.server.pathPrefix}" +
          s", allowNonHttps=${config.allowInsecureTokens.shows}" +
          s", wsConfig=${config.websocketConfig.shows}" +
          ")"
      )
    }
  }
}
