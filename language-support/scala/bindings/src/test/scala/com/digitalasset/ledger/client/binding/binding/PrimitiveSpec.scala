// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.github.ghik.silencer.silent
import scala.collection.compat._
import scala.collection.immutable.Map

class PrimitiveSpec extends AnyWordSpec with Matchers {
  import PrimitiveSpec._

  "GenMap" when {
    import Primitive.{GenMap, TextMap}
    val mii: Map[Int, Int] = Map(1 -> 2)
    val tmi: TextMap[Int] = TextMap("one" -> 2)

    "implicitly converting" should {
      "preserve Map identity" in {
        val gm: GenMap[Int, Int] = mii
        gm should be theSameInstanceAs mii
      }

      "preserve TextMap identity" in {
        val gm: GenMap[String, Int] = tmi
        gm should be theSameInstanceAs tmi
      }
    }

    "part of a contract" should {
      "construct with Map" in {
        import Primitive.Int64
        final case class FakeContract(m: GenMap[Int64, Int64])
        val withGm = FakeContract(GenMap(1L -> 2L))
        val withM = FakeContract(Map(1L -> 2L))
        withM should ===(withGm)
      }
    }

    /* 2.13 only
    "converting with .to(GenMap)" should {
      "preserve identity" in {
        val gm = mii.to(GenMap)
        isExactly(gm, ofType[GenMap[Int, Int]])
        gm should ===(mii)
        (gm eq mii) should ===(mii.to(Map) eq mii)
      }
    }
     */

    "converting with from" should {
      "preserve identity" in {
        val gm = GenMap from mii
        isExactly(gm, ofType[GenMap[Int, Int]])
        gm should ===(mii)
        (gm eq mii) should ===(Map.from(mii) eq mii)
      }
    }

    ".map" should {

      /* 2.13 only
  "preserve type" in {
      val gm: GenMap[Int, Int] = mii
    val mapped = gm map identity
    isExactly(mapped, ofType[GenMap[Int, Int]])
  }
       */

      import org.scalatest.matchers.dsl.ResultOfATypeInvocation
      import scala.collection.immutable.{HashMap, ListMap}
      Seq[(ResultOfATypeInvocation[_ <: Map[_, _]], GenMap[Int, Int])](
        a[HashMap[_, _]] -> HashMap(1 -> 2),
        a[ListMap[_, _]] -> ListMap(1 -> 2),
      ).foreach { case (mapClass, classedMap) =>
        s"preserve class ${mapClass.clazz.getSimpleName}" in {
          val Clazz = mapClass.clazzTag
          (classedMap: Map[Int, Int]).map(identity) match {
            case Clazz(_) => classedMap.map(identity) shouldBe mapClass
            case _ => succeed
          }
        }
      }
    }
  }
}

object PrimitiveSpec {
  private final class Proxy[A](val ignore: Unit) extends AnyVal
// test conformance while disabling implicit conversion
  private def ofType[T]: Proxy[T] = new Proxy(())
// private def is[Ac, Ex](ac: Ac, ex: Proxy[Ex])(implicit conforms: Ac <:< Ex): Unit = ()
// as a rule, the *singleton* type ac.type will not be ~ Ex; we are interested
// in what expression `ac` infers to *absent context*.
  @silent("parameter value (ex|ac|ev) .* is never used")
  private def isExactly[Ac, Ex](ac: Ac, ex: Proxy[Ex])(implicit ev: Ac =:= Ex): Unit = ()
}
