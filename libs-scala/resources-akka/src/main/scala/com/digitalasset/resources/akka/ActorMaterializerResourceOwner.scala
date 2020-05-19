// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.stream.Materializer
import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

class ActorMaterializerResourceOwner(acquireMaterializer: () => Materializer)
    extends ResourceOwner[Materializer] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[Materializer] =
    Resource(Future(acquireMaterializer()))(materializer => Future(materializer.shutdown()))
}
