// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import scala.reflect.ClassTag

/** Some kind of resource that needs to be initialized before usage, and torn down after.
  */
trait Resource[+Value] extends AutoCloseable {

  /** Access the resource.
    */
  def value: Value

  /** Initialize the resource.
    */
  def setup(): Unit

  def map[Target](f: Value => Target): Resource[Target] = MappedResource(this, f)

  def derive[Target: ClassTag](
      transform: Value => Target,
      tearDown: Target => Unit = (_: Target) => (),
  ): Resource[(Value, Target)] =
    new DerivedResource[Value, Target](this) {
      override protected def construct(source: Value): Target = transform(source)

      override protected def destruct(target: Target): Unit = tearDown(target)
    }
}

object Resource {

  case object NoResource extends Resource[Unit] {
    override def value: Unit = ()

    override def setup(): Unit = ()

    override def close(): Unit = ()
  }

}
