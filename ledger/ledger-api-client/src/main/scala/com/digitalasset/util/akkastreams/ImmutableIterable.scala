// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.util.akkastreams

import scala.collection.immutable

/**
  * Added to make akka-streams .mapConcat accept collections parsed from proto.
  */
final case class ImmutableIterable[T](iterable: Iterable[T]) extends immutable.Iterable[T] {

  override def iterator: Iterator[T] = iterable.iterator
}
