// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.stream.Materializer
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, Resource}

import scala.concurrent.Future

class ActorMaterializerResourceOwner[Context: HasExecutionContext](
    acquireMaterializer: () => Materializer,
) extends AbstractResourceOwner[Context, Materializer] {
  override def acquire()(implicit context: Context): Resource[Context, Materializer] =
    Resource[Context].apply(Future(acquireMaterializer()))(materializer =>
      Future(materializer.shutdown()))
}
