// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.util.akkastreams

import scala.collection.immutable

/** Added to make akka-streams .mapConcat accept collections parsed from proto.
  */
final case class ImmutableIterable[T](iterable: Iterable[T]) extends immutable.Iterable[T] {

  override def iterator: Iterator[T] = iterable.iterator
}
