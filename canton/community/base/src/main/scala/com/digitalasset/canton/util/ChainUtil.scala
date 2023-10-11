// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.Chain

/** Provides utility functions for the `cats` implementation of a `Chain`. This is a data-structure similar to a List,
  * with constant time prepend and append. Note that the `Chain` has a performance hit when pattern matching as there is
  * no constant-time uncons operation.
  *
  * Documentation on the `cats` `Chain`: https://typelevel.org/cats/datatypes/chain.html.
  */
object ChainUtil {

  def lastOption[A](chain: Chain[A]): Option[A] = {
    val revIter = chain.reverseIterator
    if (revIter.hasNext) Some(revIter.next()) else None
  }

}
