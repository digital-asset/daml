// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api

/** Bundles a stored element (or something derived from it) with the index of the next one */
final case class WithOffset[I, T](offset: I, item: T) {

  def map[U](transform: T => U) = WithOffset(offset, transform(item))
}

object WithOffset {

  def lift[I, T, U](partialFunction: PartialFunction[T, U])
    : PartialFunction[WithOffset[I, T], WithOffset[I, U]] = {
    new PartialFunction[WithOffset[I, T], WithOffset[I, U]] {
      override def isDefinedAt(x: WithOffset[I, T]): Boolean =
        partialFunction.isDefinedAt(x.item)
      override def apply(v1: WithOffset[I, T]): WithOffset[I, U] =
        WithOffset(v1.offset, partialFunction(v1.item))
    }
  }
}
