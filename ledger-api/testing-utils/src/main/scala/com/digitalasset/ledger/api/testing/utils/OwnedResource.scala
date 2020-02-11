// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.resources

import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.reflect.ClassTag

class OwnedResource[T: ClassTag](
    owner: resources.ResourceOwner[T],
    acquisitionTimeout: FiniteDuration = 10.seconds,
    releaseTimeout: FiniteDuration = 10.seconds,
) extends ManagedResource[T] {
  private var resource: resources.Resource[T] = _

  override def construct(): T = {
    resource = owner.acquire()(DirectExecutionContext)
    Await.result(resource.asFuture, acquisitionTimeout)
  }

  override def destruct(value: T): Unit = {
    Await.result(resource.release(), releaseTimeout)
  }
}
