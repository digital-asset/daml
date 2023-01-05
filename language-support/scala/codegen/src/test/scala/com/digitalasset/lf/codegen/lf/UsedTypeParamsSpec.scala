// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen
package lf

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.typesig
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class UsedTypeParamsSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  import UsedTypeParams.{ResolvedVariance, Variance}, Variance.{Covariant, Invariant}

  private[this] def ref(n: String) = Ref.Identifier.assertFromString(s"abc:Mod:$n")
  private[this] def reftc(name: String, typArgs: typesig.Type*) =
    typesig.TypeCon(typesig.TypeConName(ref(name)), ImmArraySeq(typArgs: _*))

  private val sampleDecls = {
    import typesig.{DefDataType => DT, Record, Variant, TypeVar => TVar},
    com.daml.lf.data.ImmArray.{ImmArraySeq => IASeq}, Ref.Name.{assertFromString => rn}
    val a = rn("a")
    val b = rn("b")
    val k = rn("k")
    val v = rn("v")
    Map(
      "JustMap" -> DT(
        IASeq(k, v),
        Record(
          IASeq(
            rn("unwrap") -> typesig.TypePrim(typesig.PrimType.GenMap, IASeq(k, v) map TVar)
          )
        ),
      ),
      "FlippedMap" -> DT(
        IASeq(v, k),
        Record(
          IASeq(
            rn("unwrap") -> reftc("JustMap", TVar(k), TVar(v))
          )
        ),
      ),
      // prerequisite
      "Tuple2" -> DT(IASeq(a, b), Record(IASeq(rn("_1") -> TVar(a), rn("_2") -> TVar(b)))),
      // degenerate self recursion
      "Explosion" -> DT(
        IASeq(a, b),
        Record(
          IASeq(
            rn("unwrap") -> reftc(
              "Explosion",
              reftc("Explosion", TVar(a), TVar(b)),
              reftc("Explosion", TVar(b), TVar(a)),
            )
          )
        ),
      ),
      // self recursion
      "NonFancyList" -> DT(
        IASeq(a),
        Record(
          IASeq(
            rn("unwrap") -> typesig.TypePrim(
              typesig.PrimType.Optional,
              IASeq(reftc("Tuple2", TVar(a), reftc("NonFancyList", TVar(a)))),
            )
          )
        ),
      ),
      // reductionist case of mutual recursion
      "MyList" -> DT(
        IASeq(a),
        Variant(
          IASeq(
            rn("Cons") -> reftc("MyList_Cons", TVar(a)),
            rn("Nil") -> reftc("MyList_Nil", TVar(a)),
          )
        ),
      ),
      "MyList_Cons" -> DT(
        IASeq(a),
        Record(IASeq(rn("head") -> TVar(a), rn("tail") -> reftc("MyList", TVar(a)))),
      ),
      "MyList_Nil" -> DT(
        IASeq(a),
        Record(IASeq.empty),
      ),
      // indirect invariant
      "MapOfAb" -> DT(
        IASeq(a, b),
        Record(IASeq(rn("unwrap") -> reftc("JustMap", reftc("Tuple2", TVar(a), TVar(b)), TVar(a)))),
      ),
      // mutual recursion
      "FooMapAndBar" -> DT(
        IASeq(a, b),
        Record(
          IASeq(
            rn("head") -> reftc("JustMap", TVar(a), TVar(b)),
            rn("tail") -> reftc("BarMapAndFoo", TVar(a), TVar(b)),
          )
        ),
      ),
      "BarMapAndFoo" -> DT(
        IASeq(a, b),
        Record(
          IASeq(
            rn("head") -> reftc("FlippedMap", TVar(a), TVar(b)),
            rn("tail") -> reftc("FooMapAndBar", TVar(a), TVar(b)),
          )
        ),
      ),
      "UsesInterface" -> DT(
        IASeq(a),
        Record(
          IASeq(
            rn("cid") -> typesig.TypePrim(
              typesig.PrimType.ContractId,
              IASeq(reftc("SomeInterface")),
            )
          )
        ),
      ),
      // no type vars
      "Concrete" -> DT(
        IASeq.empty,
        Record(IASeq(rn("it") -> typesig.TypePrim(typesig.PrimType.Int64, IASeq.empty))),
      ),
      "Phantom" -> DT(
        IASeq(a),
        Record(IASeq(rn("it") -> typesig.TypePrim(typesig.PrimType.Int64, IASeq.empty))),
      ),
    ).map { case (k, v) => (ref(k), typesig.PackageSignature.TypeDecl.Normal(v)) }
  }

  private val sampleInterfaces = Map(
    ref("SomeInterface") -> typesig.DefInterface(
      Map.empty,
      retroImplements = Set.empty,
      viewType = None,
    )
  )

  private val sampleEi = typesig.EnvironmentSignature(Map.empty, sampleDecls, sampleInterfaces)

  private val exVariances =
    Seq(
      "JustMap" -> Seq(Invariant, Covariant),
      "FlippedMap" -> Seq(Covariant, Invariant),
      "Tuple2" -> Seq(Covariant, Covariant),
      "Explosion" -> Seq(Covariant, Covariant),
      "NonFancyList" -> Seq(Covariant),
      "MyList" -> Seq(Covariant),
      "MapOfAb" -> Seq(Invariant, Invariant),
      "FooMapAndBar" -> Seq(Invariant, Invariant),
      "BarMapAndFoo" -> Seq(Invariant, Invariant),
      "UsesInterface" -> Seq(Covariant),
      "Concrete" -> Seq(),
      "Phantom" -> Seq(Covariant),
      "SomeInterface" -> Seq(),
    )

  private val exVarianceTable = Table(("type ctor", "positional variances"), exVariances: _*)

  "allCovariant" should {
    "find each variance set independently" in forEvery(exVarianceTable) { (ctor, variance) =>
      ResolvedVariance.Empty.allCovariantVars(ref(ctor), sampleEi)._2 should ===(variance)
    }

    "find the same variance no matter order of resolution" in forEvery(
      Table("order", exVariances, exVariances.reverse)
    ) { order =>
      order.foldLeft(ResolvedVariance.Empty) { case (st0, (ctor, variance)) =>
        val (st1, cVariance) = st0.allCovariantVars(ref(ctor), sampleEi)
        (ctor, cVariance) should ===((ctor, variance))
        st1
      }
    }
  }
}
