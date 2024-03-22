// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty

import NonEmptyCollCompat._

/** Total version of [[+:]]. */
object +-: {
  def unapply[A, CC[_], C](t: NonEmpty[SeqOps[A, CC, C]]): Some[(A, C)] =
    Some((t.head, t.tail))
}

/** Total version of [[:+]]. */
object :-+ {
  def unapply[A, CC[_], C](t: NonEmpty[SeqOps[A, CC, C]]): Some[(C, A)] =
    Some((t.init, t.last))
}
