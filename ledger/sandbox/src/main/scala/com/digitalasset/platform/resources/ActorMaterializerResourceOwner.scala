// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import akka.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

class ActorMaterializerResourceOwner(acquireMaterializer: () => Materializer)
    extends ResourceOwner[Materializer] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[Materializer] =
    Resource(Future(acquireMaterializer()), materializer => Future(materializer.shutdown()))
}
