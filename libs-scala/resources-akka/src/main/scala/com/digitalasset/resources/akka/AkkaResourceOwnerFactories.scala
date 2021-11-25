// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.{BoundedSourceQueue, Materializer}
import akka.stream.scaladsl.RunnableGraph
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext}

trait AkkaResourceOwnerFactories[Context] {
  protected implicit val hasExecutionContext: HasExecutionContext[Context]

  def forActorSystem(acquire: () => ActorSystem): AbstractResourceOwner[Context, ActorSystem] =
    new ActorSystemResourceOwner(acquire)

  def forMaterializer(acquire: () => Materializer): AbstractResourceOwner[Context, Materializer] =
    new ActorMaterializerResourceOwner(acquire)

  def forMaterializerDirectly(
      acquire: () => ActorSystem
  ): AbstractResourceOwner[Context, Materializer] =
    for {
      actorSystem <- forActorSystem(acquire)
      materializer <- forMaterializer(() => Materializer(actorSystem))
    } yield materializer

  def forCancellable[C <: Cancellable](acquire: () => C): AbstractResourceOwner[Context, C] =
    new CancellableResourceOwner(acquire)

  def forBoundedSourceQueue[T](
      queueGraph: RunnableGraph[BoundedSourceQueue[T]]
  )(implicit materializer: Materializer): AbstractResourceOwner[Context, BoundedSourceQueue[T]] =
    new BoundedSourceQueueResourceOwner[T, Context](queueGraph)
}
