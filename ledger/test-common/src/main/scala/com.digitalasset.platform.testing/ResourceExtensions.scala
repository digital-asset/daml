// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.testing

import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.ledger.api.testing.utils.Resource

object ResourceExtensions {

  implicit class MultiResource[C](resource: Resource[C]) {

    def withResource[Target](other: Option[Resource[Target]])(
        updateContext: (C, Target) => C): Resource[C] =
      other.map(o => resource.withResource(o)(updateContext)).getOrElse(resource)

    def withResource[Target](other: Resource[Target])(
        updateContext: (C, Target) => C): Resource[C] = {

      new Resource[C] {

        private val resourceRef = new AtomicReference[Option[C]](None)

        override def value: C = {
          resourceRef
            .get()
            .getOrElse(sys.error(s"Attempted to read non-initialized resource of $this"))
        }

        override def setup(): Unit = {
          resource.setup()
          other.setup()
          val newContext = updateContext(resource.value, other.value)
          if (!resourceRef.compareAndSet(None, Some(newContext))) {
            sys.error(s"Resource $this already setup")
          }
        }

        override def close(): Unit = {
          other.close()
          resource.close()
          resourceRef.set(None)
        }

        override def toString: String = s"$resource:$other"
      }

    }
  }

}
