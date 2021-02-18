// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.nonempty

import scala.collection.{IterableLike, SeqLike}

private[nonempty] object NonEmptyCollCompat {
  type IterableOps[A, +CC[_], +C] = IterableLike[A, CC[A] with C]
  type SeqOps[A, +CC[_], +C] = SeqLike[A, CC[A] with C]
}
