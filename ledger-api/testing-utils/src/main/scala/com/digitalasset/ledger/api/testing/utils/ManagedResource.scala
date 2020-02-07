// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import java.util.concurrent.atomic.AtomicReference

import scala.reflect.ClassTag

abstract class ManagedResource[Value: ClassTag] extends Resource[Value] {

  private val resourceRef = new AtomicReference[Value]()

  protected def construct(): Value

  protected def destruct(resource: Value): Unit

  final override def value: Value = {
    val res = resourceRef.get()
    if (res != null) res
    else
      throw new IllegalStateException(
        s"Attempted to read non-initialized resource of class ${implicitly[ClassTag[Value]].runtimeClass.getName}")
  }

  final override def setup(): Unit = {
    resourceRef.updateAndGet(
      (resource: Value) =>
        if (resource == null) construct()
        else throw new IllegalStateException(s"Resource $resource is already set up"))
    ()
  }

  final override def close(): Unit = {
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
