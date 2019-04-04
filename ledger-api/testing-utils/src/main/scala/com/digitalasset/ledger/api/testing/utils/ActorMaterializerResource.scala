// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.Await
import scala.concurrent.duration._

class ActorMaterializerResource(actorSystemName: String = "")
    extends ManagedResource[ActorMaterializer] {
  override protected def construct(): ActorMaterializer = {
    implicit val system: ActorSystem =
      if (actorSystemName.isEmpty) ActorSystem() else ActorSystem(actorSystemName)
    ActorMaterializer()
  }

  override protected def destruct(resource: ActorMaterializer): Unit = {
    resource.shutdown()
    Await.result(resource.system.terminate(), 30.seconds)
    ()
  }
}

object ActorMaterializerResource {
  def apply(actorSystemName: String = ""): ActorMaterializerResource =
    new ActorMaterializerResource(actorSystemName)
}
