// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen
package lf

import com.daml.lf.data.Ref
import com.daml.lf.iface
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UsedTypeParamsSpec extends AnyWordSpec with Matchers {
  import UsedTypeParams.{ResolvedVariance, Variance}, Variance.{Covariant, Invariant}

  private[this] def ref(n: String) = Ref.Identifier.assertFromString(s"abc:Mod:$n")

  val sampleEi: iface.EnvironmentInterface = iface.EnvironmentInterface {
    import iface.{DefDataType => DT, Record /*, Variant*/}, com.daml.lf.data.ImmArray.{
      ImmArraySeq => IASeq
    }, Ref.Name.{assertFromString => rn}
    val k = rn("k")
    val v = rn("v")
    Map(
      "JustMap" -> DT(
        IASeq(k, v),
        Record(
          IASeq(
            (rn("unwrap"), iface.TypePrim(iface.PrimType.GenMap, IASeq(k, v) map iface.TypeVar))
          )
        ),
      )
    ).map { case (k, v) => (ref(k), iface.InterfaceType.Normal(v)) }
  }

  "allCovariant" should {
    "see the simplest implication of JustMap" in {
      ResolvedVariance.Empty.allCovariantVars(ref("JustMap"), sampleEi)._2 should ===(
        Seq(Invariant, Covariant)
      )
    }
  }
}
