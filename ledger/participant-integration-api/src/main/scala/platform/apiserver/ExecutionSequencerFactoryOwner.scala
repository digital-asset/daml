// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.util.UUID

import akka.actor.ActorSystem
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}

import scala.concurrent.Future

private[daml] final class ExecutionSequencerFactoryOwner(implicit actorSystem: ActorSystem)
    extends ResourceOwner[ExecutionSequencerFactory] {
  // NOTE: Pick a unique pool name as we want to allow multiple LedgerApiServer instances,
  // and it's pretty difficult to wait for the name to become available again.
  // The name deregistration is asynchronous and the close method does not wait, and it isn't
  // trivial to implement.
  // https://doc.akka.io/docs/akka/2.5/actors.html#graceful-stop
  private val poolName = s"ledger-api-server-rs-grpc-bridge-${UUID.randomUUID}"

  private val ActorCount = Runtime.getRuntime.availableProcessors() * 8

  override def acquire()(implicit context: ResourceContext): Resource[ExecutionSequencerFactory] =
    Resource(Future(new AkkaExecutionSequencerPool(poolName, ActorCount)))(_.closeAsync())
}
