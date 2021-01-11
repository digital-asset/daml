// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import scala.collection.immutable

private[binding] object CollectionCompat {
  type MapLike[K, +V, +C <: immutable.MapOps[K, V, immutable.Map, C]] =
    immutable.MapOps[K, V, immutable.Map, C]
}
