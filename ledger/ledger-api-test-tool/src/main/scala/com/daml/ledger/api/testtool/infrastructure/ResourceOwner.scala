// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.resources.{HasExecutionContext, ResourceOwnerFactories}
import com.daml.resources.akka.AkkaResourceOwnerFactories
import com.daml.resources.grpc.GrpcResourceOwnerFactories

import scala.concurrent.ExecutionContext
import HasExecutionContext.`ExecutionContext has itself`

sealed abstract class ResourceOwner[Context](
    override protected final implicit val hasExecutionContext: HasExecutionContext[Context]
) extends ResourceOwnerFactories[Context]
    with AkkaResourceOwnerFactories[Context]
    with GrpcResourceOwnerFactories[Context]

object ResourceOwner extends ResourceOwner[ExecutionContext]()(`ExecutionContext has itself`)
