// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.test

object CollectionCompat {
  type IterableFactory[
      CC[X] <: Traversable[X] with collection.generic.GenericTraversableTemplate[X, CC]
  ] =
    collection.generic.TraversableFactory[CC]
}
