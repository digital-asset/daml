// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext}

trait AkkaResourceOwnerFactories[Context] {
  protected implicit val hasExecutionContext: HasExecutionContext[Context]

  def forActorSystem(acquire: () => ActorSystem): AbstractResourceOwner[Context, ActorSystem] =
    new ActorSystemResourceOwner(acquire)

  def forMaterializer(acquire: () => Materializer): AbstractResourceOwner[Context, Materializer] =
    new ActorMaterializerResourceOwner(acquire)
}
