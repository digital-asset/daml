// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import scala.collection.immutable

object Compat {
  private[binding] type MapLike[
      K,
      +V,
      +CC[X, +Y],
      +C <: immutable.MapLike[K, V, C] with immutable.Map[K, V],
  ] =
    immutable.MapLike[K, V, C]

  private[binding] type MapFactory[
      CC[K, +V] <: immutable.Map[K, V] with immutable.MapLike[K, V, CC[K, V]]
  ] = scala.collection.generic.ImmutableMapFactory[CC]

  type DummyImplicit = scala.Predef.DummyImplicit
}
