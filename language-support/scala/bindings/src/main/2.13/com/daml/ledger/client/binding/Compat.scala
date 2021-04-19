// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import scala.collection.immutable

object Compat {
  private[binding] type MapLike[
      K,
      +V,
      +CC[X, +Y] <: immutable.MapOps[X, Y, CC, _],
      +C <: immutable.MapOps[K, V, CC, C],
  ] =
    immutable.MapOps[K, V, CC, C]

  private[binding] type CanBuildFrom[-A, -B, +That] = scala.collection.Factory[B, That]
  private[binding] type MapFactory[CC[K, V]] = scala.collection.MapFactory[CC]

  type DummyImplicit = scala.DummyImplicit
}
