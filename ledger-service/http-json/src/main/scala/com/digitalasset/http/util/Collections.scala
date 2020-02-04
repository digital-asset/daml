// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import scalaz.{NonEmptyList, \/}

import scala.collection.TraversableLike

object Collections {

  implicit final class `cdhuc TraversableOps`[A, Self](private val self: TraversableLike[A, Self])
      extends AnyVal {

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

    /*
    def unzipMap[B, C, Bs, Cs](f: A => (B, C))(implicit bs: CanBuildFrom[Self, B, Bs], cs: CanBuildFrom[Self, C, Cs]) = {
      val bsb = bs(self.repr)
      val csb = cs(self.repr)
      self foreach { a =>
        val (b, c) = f(a)
        bsb += b
        csb += c
      }
      (bsb.result, csb.result)
    }
   */
  }

  implicit final class `cdhuc Nel Ops`[A](private val self: NonEmptyList[A]) extends AnyVal {
    def collect[B](f: A PartialFunction B): Option[NonEmptyList[B]] =
      self.list.collect(f).toNel
  }
}
