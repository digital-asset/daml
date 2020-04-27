// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import com.daml.resources

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}
import scala.reflect.ClassTag

final class OwnedResource[T: ClassTag](
    owner: resources.ResourceOwner[T],
    acquisitionTimeout: FiniteDuration = 30.seconds,
    releaseTimeout: FiniteDuration = 30.seconds,
)(implicit executionContext: ExecutionContext)
    extends ManagedResource[T] {
  private var resource: resources.Resource[T] = _

  override def construct(): T = {
    resource = owner.acquire()
    Await.result(resource.asFuture, acquisitionTimeout)
  }

  override def destruct(value: T): Unit = {
    Await.result(resource.release(), releaseTimeout)
  }
}
