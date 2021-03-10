// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen
package lf

import com.daml.lf.data.Ref
import com.daml.lf.iface
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class UsedTypeParamsSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  import UsedTypeParams.{ResolvedVariance, Variance}, Variance.{Covariant, Invariant}

  private[this] def ref(n: String) = Ref.Identifier.assertFromString(s"abc:Mod:$n")

  private val sampleEi: iface.EnvironmentInterface = iface.EnvironmentInterface {
    import iface.{DefDataType => DT, Record, Variant, TypeVar => TVar}, com.daml.lf.data.ImmArray.{
      ImmArraySeq => IASeq
    }, Ref.Name.{assertFromString => rn}
    val a = rn("a")
    val k = rn("k")
    val v = rn("v")
    Map(
      "JustMap" -> DT(
        IASeq(k, v),
        Record(
          IASeq(
            (rn("unwrap"), iface.TypePrim(iface.PrimType.GenMap, IASeq(k, v) map TVar))
          )
        ),
      ),
      "FlippedMap" -> DT(
        IASeq(v, k),
        Record(
          IASeq(
            (rn("unwrap"), iface.TypeCon(iface.TypeConName(ref("JustMap")), IASeq(k, v) map TVar))
          )
        ),
      ),
      "MyList" -> DT(
        IASeq(a),
        Variant(
          IASeq(
            (rn("Cons"), iface.TypeCon(iface.TypeConName(ref("MyList_Cons")), IASeq(a) map TVar)),
            (rn("Nil"), iface.TypeCon(iface.TypeConName(ref("MyList_Nil")), IASeq(a) map TVar)),
          )
        ),
      ),
    ).map { case (k, v) => (ref(k), iface.InterfaceType.Normal(v)) }
  }

  private val exVariances =
    Seq("JustMap" -> Seq(Invariant, Covariant), "FlippedMap" -> Seq(Covariant, Invariant))

  private val exVarianceTable = Table(("type ctor", "positional variances"), exVariances: _*)

  "allCovariant" should {
    "find each variance set independently" in forEvery(exVarianceTable) { (ctor, variance) =>
      ResolvedVariance.Empty.allCovariantVars(ref(ctor), sampleEi)._2 should ===(variance)
    }
  }
}
