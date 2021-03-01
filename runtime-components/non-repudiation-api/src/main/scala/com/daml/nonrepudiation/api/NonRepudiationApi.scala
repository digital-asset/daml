// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.{concat, pathPrefix}
import akka.http.scaladsl.server.Route
import com.daml.nonrepudiation.api.NonRepudiationApi.Configuration
import com.daml.nonrepudiation.{CertificateRepository, CommandIdString, SignedPayloadRepository}
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object NonRepudiationApi {

  // We don't need access to the underlying resource, we use
  // the owner only to manage the server's life cycle
  def owner[Context: HasExecutionContext](
      configuration: Configuration,
      certificateRepository: CertificateRepository,
      signedPayloadRepository: SignedPayloadRepository.Read[CommandIdString],
  )(implicit system: ActorSystem): AbstractResourceOwner[Context, Unit] =
    new NonRepudiationApi[Context](
      configuration,
      certificateRepository,
      signedPayloadRepository,
    ).map(_ => ())

  final case class Configuration(
      interface: String,
      port: Int,
      shutdownTimeout: FiniteDuration,
  )

  // Non-repudiation -> NR -> N = 78 and R = 82 in ASCII
  val DefaultPort: Int = 7882

  object Configuration {

    val Default: Configuration =
      Configuration(
        interface = InetAddress.getLoopbackAddress.getHostAddress,
        port = DefaultPort,
        shutdownTimeout = 10.seconds,
      )

  }

}

final class NonRepudiationApi[Context: HasExecutionContext] private (
    configuration: Configuration,
    certificates: CertificateRepository,
    signedPayloads: SignedPayloadRepository.Read[CommandIdString],
)(implicit system: ActorSystem)
    extends AbstractResourceOwner[Context, Http.ServerBinding] {

  private val route: Route =
    pathPrefix("v1") {
      concat(
        pathPrefix("certificate") { v1.CertificatesEndpoint(certificates) },
        pathPrefix("command") { v1.SignedPayloadsEndpoint(signedPayloads) },
      )
    }

  private def bindNewServer(): Future[Http.ServerBinding] =
    Http()
      .newServerAt(
        interface = configuration.interface,
        port = configuration.port,
      )
      .bind(route)

  override def acquire()(implicit context: Context): Resource[Context, Http.ServerBinding] =
    ReleasableResource(bindNewServer()) { server =>
      for {
        _ <- server.unbind()
        _ <- server.terminate(configuration.shutdownTimeout)
      } yield ()
    }

}
