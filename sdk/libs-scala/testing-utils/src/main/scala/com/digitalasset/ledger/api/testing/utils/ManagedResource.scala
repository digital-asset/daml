// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import scala.reflect.ClassTag

abstract class ManagedResource[Value: ClassTag] extends Resource[Value] {
  private var resource: Option[Value] = None

  protected def construct(): Value

  protected def destruct(resource: Value): Unit

  final override def value: Value =
    resource.getOrElse {
      throw new IllegalStateException(
        s"Attempted to read non-initialized resource of ${implicitly[ClassTag[Value]].runtimeClass}"
      )
    }

  final override def setup(): Unit =
    synchronized {
      if (resource.isEmpty) {
        resource = Some(construct())
      }
    }

  final override def close(): Unit =
    synchronized {
      if (resource.isDefined) {
        destruct(resource.get)
        resource = None
        ()
      }
    }
}
