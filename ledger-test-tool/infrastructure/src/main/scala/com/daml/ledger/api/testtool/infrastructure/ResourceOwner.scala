// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.resources.{HasExecutionContext, ResourceOwnerFactories}
import com.daml.resources.pekko.PekkoResourceOwnerFactories
import com.daml.resources.grpc.GrpcResourceOwnerFactories

import scala.concurrent.ExecutionContext
import HasExecutionContext.`ExecutionContext has itself`

object ResourceOwner
    extends ResourceOwnerFactories[ExecutionContext]
    with PekkoResourceOwnerFactories[ExecutionContext]
    with GrpcResourceOwnerFactories[ExecutionContext] {
  override protected implicit val hasExecutionContext: HasExecutionContext[ExecutionContext] =
    implicitly[HasExecutionContext[ExecutionContext]]
}
