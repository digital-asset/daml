// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import akka.stream.ActorMaterializer

import scala.concurrent.{ExecutionContext, Future}

class ActorMaterializerResourceOwner(acquireMaterializer: () => ActorMaterializer)
    extends ResourceOwner[ActorMaterializer] {
  override def acquire()(
      implicit _executionContext: ExecutionContext): Resource[ActorMaterializer] =
    new Resource[ActorMaterializer] {
      override protected val executionContext: ExecutionContext = _executionContext

      private val materializer: ActorMaterializer = acquireMaterializer()

      override protected val future: Future[ActorMaterializer] = Future.successful(materializer)

      override def release(): Future[Unit] = Future {
        materializer.shutdown()
      }
    }
}
