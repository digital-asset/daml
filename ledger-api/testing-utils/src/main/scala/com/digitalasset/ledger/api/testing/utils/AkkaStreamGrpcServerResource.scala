// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import java.net.SocketAddress
import java.util.concurrent.TimeUnit

import akka.stream.ActorMaterializer
import io.grpc.BindableService

class AkkaStreamGrpcServerResource(
    constructServices: ActorMaterializer => Iterable[BindableService],
    actorMaterializerResource: Resource[ActorMaterializer],
    address: Option[SocketAddress])
    extends DerivedResource[ActorMaterializer, ServerWithChannelProvider](actorMaterializerResource) {

  @volatile private var runningServices: Iterable[BindableService] = Nil

  def getRunningServices: Iterable[BindableService] = runningServices

  override protected def construct(source: ActorMaterializer): ServerWithChannelProvider = {

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

object AkkaStreamGrpcServerResource {
  def apply(
      constructServices: ActorMaterializer => Iterable[BindableService],
      actorSystemName: String = "",
      address: Option[SocketAddress]) =
    new AkkaStreamGrpcServerResource(
      constructServices,
      new ActorMaterializerResource(actorSystemName),
      address)
}
