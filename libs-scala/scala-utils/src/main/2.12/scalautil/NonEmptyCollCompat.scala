// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import scala.collection.{IterableLike, SeqLike}

private[scalautil] trait NonEmptyCollCompat {
  protected[this] type IterableOps[A, CC[_], C] = IterableLike[A, CC[C]]
  protected[this] type SeqOps[A, CC[_], C] = SeqLike[A, CC[C]]
}

