// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api

/** Bundles a stored element (or something derived from it) with the index of the next one */
final case class WithOffset[T](offset: BigInt, item: T) {

  def map[U](transform: T => U) = WithOffset(offset, transform(item))
}

object WithOffset {

  def lift[T, U](
      partialFunction: PartialFunction[T, U]): PartialFunction[WithOffset[T], WithOffset[U]] = {
    new PartialFunction[WithOffset[T], WithOffset[U]] {
      override def isDefinedAt(x: WithOffset[T]): Boolean = partialFunction.isDefinedAt(x.item)
      override def apply(v1: WithOffset[T]): WithOffset[U] =
        WithOffset(v1.offset, partialFunction(v1.item))
    }
  }
}
