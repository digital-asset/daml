// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import java.net.SocketAddress
import java.util.concurrent.TimeUnit

import org.apache.pekko.stream.Materializer
import io.grpc.BindableService

class PekkoStreamGrpcServerResource(
    constructServices: Materializer => Iterable[BindableService],
    actorMaterializerResource: Resource[Materializer],
    address: Option[SocketAddress],
) extends DerivedResource[Materializer, ServerWithChannelProvider](actorMaterializerResource) {

  @volatile private var runningServices: Iterable[BindableService] = Nil

  def getRunningServices: Iterable[BindableService] = runningServices

  override protected def construct(source: Materializer): ServerWithChannelProvider = {

    runningServices = constructServices(actorMaterializerResource.value)
    ServerWithChannelProvider.fromServices(runningServices, address, "server")

  }

  override protected def destruct(resource: ServerWithChannelProvider): Unit = {
    val server = derivedValue.server

    server.shutdownNow()

    runningServices.foreach {
      case closeable: AutoCloseable => closeable.close()
      case _ => ()
    }
    runningServices = Nil

    server.awaitTermination(10, TimeUnit.SECONDS)
    ()
  }
}

object PekkoStreamGrpcServerResource {
  def apply(
      constructServices: Materializer => Iterable[BindableService],
      actorSystemName: String = "",
      address: Option[SocketAddress],
  ) =
    new PekkoStreamGrpcServerResource(
      constructServices,
      new ActorMaterializerResource(actorSystemName),
      address,
    )
}
