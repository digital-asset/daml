// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import java.util.concurrent.atomic.AtomicReference

import scala.reflect.ClassTag

abstract class DerivedResource[Source, Target: ClassTag](source: Resource[Source])
    extends Resource[(Source, Target)] {

  private val resourceRef = new AtomicReference[Target]()

  def derivedValue: Target = {
    val res = resourceRef.get()
    if (res != null) res
    else
      throw new IllegalStateException(
        s"Attempted to read non-initialized resource of class ${implicitly[ClassTag[Target]].runtimeClass.getName}"
      )
  }

  override def value: (Source, Target) = {
    source.value -> derivedValue
  }

  override def setup(): Unit = {
    resourceRef.updateAndGet((resource: Target) =>
      if (resource == null) {
        source.setup()
        construct(source.value)
      } else throw new IllegalStateException(s"Resource $resource is already set up")
    )
    ()
  }

  protected def construct(source: Source): Target

  override def close(): Unit = {
    resourceRef.updateAndGet(resource => {
      if (resource != null) {
        destruct(resource)
        source.close()
      }
      null.asInstanceOf[Target]
    })
    ()
  }

  protected def destruct(target: Target): Unit
}
