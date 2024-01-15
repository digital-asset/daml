// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.types

import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scalaz.std.anyVal._
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.syntax.bifunctor._

class NamespaceSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {
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
  import com.daml.lf.codegen.lf.HierarchicalOutput.`scalaz ==>> future`

  def paths[K, A](n: Namespace[K, A]): Vector[(List[K], A)] =
    n.foldTreeStrict[Vector[(List[K], A)]] { (a, kVecs) =>
      kVecs.foldMapWithKey { (k, vec) =>
        vec.map(_ leftMap (k :: _))
      } :+ ((List(), a))
    }
}
