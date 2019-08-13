// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.types

import org.scalatest.{WordSpec, Matchers, Inside}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scalaz.std.anyVal._
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.syntax.bifunctor._

class NamespaceSpec extends WordSpec with Matchers with Inside with GeneratorDrivenPropertyChecks {
  "fromHierarchy" should {
    "be lossless for keysets" in forAll { m: Map[List[Int], Int] =>
      NamespaceSpec
        .paths(Namespace.fromHierarchy(m))
        .collect { case (ns, Some(a)) => (ns, a) }
        .toMap shouldBe m
    }
  }
}

object NamespaceSpec {
  import com.digitalasset.codegen.lf.HierarchicalOutput.`scalaz ==>> future`

  def paths[K, A](n: Namespace[K, A]): Vector[(List[K], A)] =
    n.foldTreeStrict[Vector[(List[K], A)]] { (a, kVecs) =>
      kVecs.foldMapWithKey { (k, vec) =>
        vec.map(_ leftMap (k :: _))
      } :+ ((List(), a))
    }
}
