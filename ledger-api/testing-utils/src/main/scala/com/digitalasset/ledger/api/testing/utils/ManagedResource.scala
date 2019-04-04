// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import java.util.concurrent.atomic.AtomicReference

import scala.reflect.ClassTag

abstract class ManagedResource[Value: ClassTag] extends Resource[Value] {

  private val resourceRef = new AtomicReference[Value]()

  protected def construct(): Value

  protected def destruct(resource: Value): Unit

  override def value: Value = {
    val res = resourceRef.get()
    if (res != null) res
    else
      throw new IllegalStateException(
        s"Attempted to read non-initialized resource of class ${implicitly[ClassTag[Value]].runtimeClass.getName}")
  }

  override def setup(): Unit = {
    resourceRef.updateAndGet(
      (resource: Value) =>
        if (resource == null) construct()
        else throw new IllegalStateException(s"Resource $resource is already set up"))
    ()
  }

  override def close(): Unit = {
    resourceRef.updateAndGet { (resource: Value) =>
      if (resource != null) {
        destruct(resource)
        null.asInstanceOf[Value]
      } else {
        resource
      }
    }
    ()
  }
}
