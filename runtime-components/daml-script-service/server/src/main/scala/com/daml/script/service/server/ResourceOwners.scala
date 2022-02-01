// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.service.server

import akka.http.scaladsl.Http
import com.daml.resources.akka.AkkaResourceOwnerFactories
import com.daml.resources.{AbstractResourceOwner, ResourceOwnerFactories, HasExecutionContext}

import scala.concurrent.Future

// TODO evaluate putting this in a library and/or adding it to `//libs-scala/akka-resources`
private[server] final class ResourceOwners[Context](context: HasExecutionContext[Context])
    extends ResourceOwnerFactories[Context]
    with AkkaResourceOwnerFactories[Context] {
  override protected implicit val hasExecutionContext: HasExecutionContext[Context] =
    context

  def forHttpServerBinding(
      acquire: () => Future[Http.ServerBinding]
  ): AbstractResourceOwner[Context, Http.ServerBinding] =
    new HttpServerBindingResourceOwner(acquire)
}
