// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.resources

import com.daml.resources.akka.AkkaResourceOwnerFactories
import com.daml.resources.grpc.GrpcResourceOwnerFactories
import com.daml.resources.{HasExecutionContext, ResourceOwnerFactories}

object ResourceOwner
    extends ResourceOwnerFactories[ResourceContext]
    with AkkaResourceOwnerFactories[ResourceContext]
    with GrpcResourceOwnerFactories[ResourceContext] {
  override protected implicit val hasExecutionContext: HasExecutionContext[ResourceContext] =
    ResourceContext.`Context has ExecutionContext`
}
