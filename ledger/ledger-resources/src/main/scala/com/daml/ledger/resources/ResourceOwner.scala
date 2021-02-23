// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.resources

import com.daml.resources
import com.daml.resources.akka.AkkaResourceOwnerFactories
import com.daml.resources.grpc.GrpcResourceOwnerFactories
import com.daml.resources.{HasExecutionContext, ResourceOwnerFactories}

object ResourceOwner
    extends ResourceOwnerFactories[ResourceContext]
    with AkkaResourceOwnerFactories[ResourceContext]
    with GrpcResourceOwnerFactories[ResourceContext] {
  override protected implicit val hasExecutionContext: HasExecutionContext[ResourceContext] =
    ResourceContext.`Context has ExecutionContext`

  def apply[T](f: ResourceContext => Resource[T]): ResourceOwner[T] = new ResourceOwner[T] {
    override def acquire()(implicit context: ResourceContext): resources.Resource[ResourceContext, T] =
      f(context)
  }
}
