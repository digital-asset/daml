// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.discard

object Implicits {

  /** Evaluate the expression and discard the result. */
  implicit final class DiscardOps[A](private val a: A) extends AnyVal {
    @inline
    def discard[B](implicit ev: A =:= B): Unit = ()
  }

}
