// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import scalaz.\/

import scala.collection.SeqLike

object Collections {

  implicit final class SeqOps[A, Self](private val self: SeqLike[A, Self]) extends AnyVal {

    import collection.generic.CanBuildFrom

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def partitionMap[E, B, Es, That](f: A => E \/ B)(
        implicit es: CanBuildFrom[Self, E, Es],
        that: CanBuildFrom[Self, B, That]): (Es, That) = {
      val esb = es(self.repr)
      val thatb = that(self.repr)
      self foreach { a =>
        f(a) fold (esb.+=, thatb.+=)
      }
      (esb.result, thatb.result)
    }
  }
}
