// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import akka.stream.ActorMaterializer

import scala.concurrent.{ExecutionContext, Future}

class ActorMaterializerResourceOwner(acquireMaterializer: () => ActorMaterializer)
    extends ResourceOwner[ActorMaterializer] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[ActorMaterializer] =
    Resource(Future(acquireMaterializer()), materializer => Future(materializer.shutdown()))
}
