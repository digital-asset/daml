// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.Materializer
import akka.stream.scaladsl.{RunnableGraph, SourceQueue, SourceQueueWithComplete}
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

  def forSourceQueue[T](
      queueGraph: RunnableGraph[SourceQueueWithComplete[T]]
  )(implicit materializer: Materializer): AbstractResourceOwner[Context, SourceQueue[T]] =
    new SourceQueueResourceOwner[T, Context](queueGraph)
}
