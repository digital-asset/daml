// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.test

object CollectionCompat {
  type IterableFactory[+CC[_]] = scala.collection.IterableFactory[CC]
}
