// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.service.server

import com.daml.resources.{AbstractResourceOwner, Resource, ReleasableResource, HasExecutionContext}
import akka.http.scaladsl.Http

import scala.concurrent.Future

final class HttpServerBindingResourceOwner[Context](acquire: () => Future[Http.ServerBinding])(
    implicit val hasExecutionContext: HasExecutionContext[Context]
) extends AbstractResourceOwner[Context, Http.ServerBinding] {

  /** Acquires the [[Resource]].
    *
    * @param context The acquisition context, including the asynchronous task execution engine.
    * @return The acquired [[Resource]].
    */
  override def acquire()(implicit context: Context): Resource[Context, Http.ServerBinding] =
    ReleasableResource(acquire())(_.unbind().map(_ => ()))

}
