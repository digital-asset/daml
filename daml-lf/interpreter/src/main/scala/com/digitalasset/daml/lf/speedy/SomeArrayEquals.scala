// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import scala.annotation.tailrec
import scala.util.hashing.MurmurHash3._

trait SomeArrayEquals extends Product with Serializable {
  override final def equals(o: Any): Boolean = o match {
    case oo: AnyRef if oo eq this => true
    case oo: SomeArrayEquals
        if (this canEqual oo) && (oo canEqual this) && productArity == oo.productArity =>
      val arr = productArity
      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      @tailrec def lp(i: Int): Boolean =
        if (i >= arr) true
        else
          (productElement(i) match {
            case a: Array[_] =>
              oo.productElement(i) match {
                case b: Array[_] => (a eq b) || (a sameElements b)
                case _ => false
              }
            case a => a == oo.productElement(i)
          }) && lp(i + 1)
      lp(0)
    case _ => false
  }

  override final def hashCode(): Int = {
    val arr = productArity
    if (arr == 0) productPrefix.##
    else {
      @tailrec def lp(i: Int, seed: Int): Int =
        if (i >= arr) seed
        else
          lp(
            i + 1,
            mix(
              seed,
              productElement(i) match {
                case a: Array[_] => arrayHash(a)
                case a => a.##
              },
            ),
          )
      finalizeHash(lp(0, productSeed), arr)
    }
  }
}

object SomeArrayEquals {
  private[speedy] final case class ComparableArray(a: Array[_]) {
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    override def equals(o: Any): Boolean = o match {
      case oo: AnyRef if oo eq this => true
      case oo: ComparableArray if oo canEqual this =>
        val b = oo.a
        (a eq b) || {
          (a ne null) && (b ne null) && {
            a sameElements b
          }
        }
      case _ => false
    }

    override def hashCode(): Int = {
      if (a eq null) null.## else arrayHash(a)
    }
  }
}
