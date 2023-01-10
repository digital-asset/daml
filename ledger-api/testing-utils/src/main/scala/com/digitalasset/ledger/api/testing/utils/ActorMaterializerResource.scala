// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import akka.actor.ActorSystem
import akka.stream.Materializer

import scala.concurrent.Await
import scala.concurrent.duration._

final class ActorMaterializerResource(actorSystemName: String = "")
    extends ManagedResource[Materializer] {
  override protected def construct(): Materializer = {
    implicit val system: ActorSystem =
      if (actorSystemName.isEmpty) ActorSystem() else ActorSystem(actorSystemName)
    Materializer(system)
  }

  override protected def destruct(resource: Materializer): Unit = {
    resource.shutdown()
    Await.result(resource.system.terminate(), 30.seconds)
    ()
  }
}

object ActorMaterializerResource {
  def apply(actorSystemName: String = ""): ActorMaterializerResource =
    new ActorMaterializerResource(actorSystemName)
}
