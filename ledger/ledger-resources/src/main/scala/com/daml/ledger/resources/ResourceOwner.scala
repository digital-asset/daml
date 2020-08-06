// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.resources

import com.daml.resources.akka.AkkaResourceOwnerFactories
import com.daml.resources.{HasExecutionContext, ResourceOwnerFactories}

import scala.concurrent.ExecutionContext

object ResourceOwner
    extends ResourceOwnerFactories[ExecutionContext]
    with AkkaResourceOwnerFactories[ExecutionContext] {
  override protected implicit val hasExecutionContext: HasExecutionContext[ExecutionContext] =
    HasExecutionContext.`ExecutionContext has itself`
}
