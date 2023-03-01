// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.Map

class Primitive213Spec extends AnyWordSpec with Matchers {
  import Primitive213Spec._

  "GenMap" when {
    import Primitive.GenMap
    val mii: Map[Int, Int] = Map(1 -> 2)

    "converting with .to(GenMap)" should {
      "preserve identity" in {
        val gm = mii.to(GenMap)
        isExactly(gm, ofType[GenMap[Int, Int]])
        gm should ===(mii)
        (gm eq mii) should ===(mii.to(Map) eq mii)
      }
    }

    ".map" should {
      "preserve type" in {
        val gm: GenMap[Int, Int] = mii
        val mapped = gm map identity
        isExactly(mapped, ofType[GenMap[Int, Int]])
      }
    }
  }
}

object Primitive213Spec {
  private final class Proxy[A](val ignore: Unit) extends AnyVal
  // test conformance while disabling implicit conversion
  private def ofType[T]: Proxy[T] = new Proxy(())
  // as a rule, the *singleton* type ac.type will not be ~ Ex; we are interested
  // in what expression `ac` infers to *absent context*.
  private def isExactly[Ac, Ex](ac: Ac, ex: Proxy[Ex])(implicit ev: Ac =:= Ex): Unit = ()
}
