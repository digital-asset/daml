// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import com.daml.resources
import com.daml.resources.AbstractResourceOwner

import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.reflect.ClassTag

final class OwnedResource[Context, T: ClassTag](
    owner: AbstractResourceOwner[Context, T],
    acquisitionTimeout: FiniteDuration = 30.seconds,
    releaseTimeout: FiniteDuration = 30.seconds,
)(implicit context: Context)
    extends ManagedResource[T] {
  private var resource: resources.Resource[Context, T] = _

  override def construct(): T = {
    resource = owner.acquire()
    Await.result(resource.asFuture, acquisitionTimeout)
  }

  override def destruct(value: T): Unit = {
    Await.result(resource.release(), releaseTimeout)
  }
}
