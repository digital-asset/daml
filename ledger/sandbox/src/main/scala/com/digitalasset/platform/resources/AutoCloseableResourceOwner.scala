// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class AutoCloseableResourceOwner[T <: AutoCloseable](acquireCloseable: () => T)
    extends ResourceOwner[T] {
  val closeable = Try(acquireCloseable())

  override def acquire()(implicit _executionContext: ExecutionContext): Resource[T] =
    new Resource[T] {
      override protected implicit val executionContext: ExecutionContext = _executionContext

      override protected val future: Future[T] = Future.fromTry(closeable)

      override def release(): Future[Unit] = Future.fromTry(closeable.map(_.close()))
    }
}
