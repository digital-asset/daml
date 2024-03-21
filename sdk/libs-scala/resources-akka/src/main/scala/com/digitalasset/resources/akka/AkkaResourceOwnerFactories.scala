// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.scaladsl.RunnableGraph
import akka.stream.{BoundedSourceQueue, Materializer}
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext}

import scala.concurrent.{ExecutionContext, Future}

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

  def forBoundedSourceQueue[T, U](
      queueGraph: RunnableGraph[(BoundedSourceQueue[T], Future[U])]
  )(implicit
      materializer: Materializer
  ): AbstractResourceOwner[Context, (BoundedSourceQueue[T], Future[U])] =
    new BoundedSourceQueueResourceOwner[(BoundedSourceQueue[T], Future[U]), T, Context](
      queueGraph = queueGraph,
      toSourceQueue = _._1,
      toDone = _._2.map(_ => ())(ExecutionContext.parasitic),
    )
}
