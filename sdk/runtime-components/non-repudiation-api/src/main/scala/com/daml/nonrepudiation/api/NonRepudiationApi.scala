// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api

import java.net.InetSocketAddress

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives.{concat, pathPrefix}
import org.apache.pekko.http.scaladsl.server.Route
import com.daml.nonrepudiation.{CertificateRepository, CommandIdString, SignedPayloadRepository}
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object NonRepudiationApi {

  // We don't need access to the underlying resource, we use
  // the owner only to manage the server's life cycle
  def owner[Context: HasExecutionContext](
      address: InetSocketAddress,
      shutdownTimeout: FiniteDuration,
      certificateRepository: CertificateRepository,
      signedPayloadRepository: SignedPayloadRepository.Read[CommandIdString],
      actorSystem: ActorSystem,
  ): AbstractResourceOwner[Context, Unit] =
    new NonRepudiationApi[Context](
      address,
      shutdownTimeout,
      certificateRepository,
      signedPayloadRepository,
      actorSystem,
    ).map(_ => ())

}

final class NonRepudiationApi[Context: HasExecutionContext] private (
    address: InetSocketAddress,
    shutdownTimeout: FiniteDuration,
    certificates: CertificateRepository,
    signedPayloads: SignedPayloadRepository.Read[CommandIdString],
    actorSystem: ActorSystem,
) extends AbstractResourceOwner[Context, Http.ServerBinding] {

  private val route: Route =
    pathPrefix("v1") {
      concat(
        pathPrefix("certificate") { v1.CertificatesEndpoint(certificates) },
        pathPrefix("command") { v1.SignedPayloadsEndpoint(signedPayloads) },
      )
    }

  private def bindNewServer(): Future[Http.ServerBinding] = {
    implicit val system: ActorSystem = actorSystem
    Http().newServerAt(address.getAddress.getHostAddress, address.getPort).bind(route)
  }

  override def acquire()(implicit context: Context): Resource[Context, Http.ServerBinding] =
    ReleasableResource(bindNewServer()) { server =>
      for {
        _ <- server.unbind()
        _ <- server.terminate(shutdownTimeout)
      } yield ()
    }

}
