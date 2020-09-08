// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy
package svalue

import com.daml.lf.data.{FrontStack, Ref}
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SExpr.SEImportValue
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value
import com.daml.lf.value.test.TypedValueGenerators.genAddend
import com.daml.lf.value.test.ValueGenerators.{cidV0Gen, comparableCoidsGen}
import com.daml.lf.PureCompiledPackages
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{
  Checkers,
  GeneratorDrivenPropertyChecks,
  TableDrivenPropertyChecks,
  TableFor2
}
import org.scalatest.{Matchers, WordSpec}
import scalaz.{Order, Tag}
import scalaz.syntax.order._
import scalaz.scalacheck.{ScalazProperties => SzP}

import scala.language.implicitConversions

class OrderingSpec
    extends WordSpec
    with Matchers
    with Checkers
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks {

  private val pkgId = Ref.PackageId.assertFromString("pkgId")

  implicit def toTypeConName(s: String): Ref.TypeConName =
    Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

  implicit def toName(s: String): Ref.Name =
    Ref.Name.assertFromString(s)

  private val randomComparableValues: TableFor2[String, Gen[SValue]] = {
    import com.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
    implicit val ordNo
      : Order[Nothing] = Order order [Nothing]((_: Any, _: Any) => sys.error("impossible"))
    def r(name: String, va: VA)(sv: va.Inj[Nothing] => SValue) =
      (name, va.injarb[Nothing].arbitrary map sv)
    Table(
      ("comparable value subset", "generator"),
      Seq(
        r("Int64", VA.int64)(SInt64),
        r("Text", VA.text)(SText),
        r("Int64 Option List", VA.list(VA.optional(VA.int64))) { loi =>
          SList(loi.map(oi => SOptional(oi map SInt64)).to[FrontStack])
        },
      ) ++
        comparableCoidsGen.zipWithIndex.map {
          case (g, ix) => (s"ContractId $ix", g map SContractId)
        }: _*
    )
  }

  "Order.compare" should {
    "be lawful on each subset" in forEvery(randomComparableValues) { (_, svGen) =>
      implicit val svalueOrd: Order[SValue] = Order fromScalaOrdering Ordering
      implicit val svalueArb: Arbitrary[SValue] = Arbitrary(svGen)
      forEvery(Table(("law", "prop"), SzP.order.laws[SValue].properties: _*)) { (_, p) =>
        check(p, minSuccessful(50))
      }
    }
  }

  // A problem in this test *usually* indicates changes that need to be made
  // in Value.orderInstance or TypedValueGenerators, rather than to svalue.Ordering.
  // The tests are here as this is difficult to test outside daml-lf/interpreter.
  "txn Value Ordering" should {
    import Value.{ContractId => Cid}
    // SContractId V1 ordering is nontotal so arbitrary generation of them is unsafe to use
    implicit val cidArb: Arbitrary[Cid] = Arbitrary(cidV0Gen)
    implicit val svalueOrd: Order[SValue] = Order fromScalaOrdering Ordering
    implicit val cidOrd: Order[Cid] = svalueOrd contramap SValue.SContractId
    val EmptyScope: Value.LookupVariantEnum = _ => None

    "match SValue Ordering" in forAll(genAddend, minSuccessful(100)) { va =>
      import va.{injarb, injshrink}
      implicit val valueOrd: Order[Value[Cid]] = Tag unsubst Value.orderInstance[Cid](EmptyScope)
      forAll(minSuccessful(20)) { (a: va.Inj[Cid], b: va.Inj[Cid]) =>
        import va.injord
        val ta = va.inj(a)
        val tb = va.inj(b)
        val bySvalue = translatePrimValue(ta) ?|? translatePrimValue(tb)
        (a ?|? b, ta ?|? tb) should ===((bySvalue, bySvalue))
      }
    }

    "match global ContractId ordering" in forEvery(Table("gen", comparableCoidsGen: _*)) {
      coidGen =>
        forAll(coidGen, coidGen, minSuccessful(50)) { (a, b) =>
          Cid.`Cid Order`.order(a, b) should ===(cidOrd.order(a, b))
        }
    }
  }

  private val noPackages =
    PureCompiledPackages(Map.empty, Map.empty, Compiler.FullStackTrace, Compiler.NoProfile)

  private def translatePrimValue(v: Value[Value.ContractId]) = {
    val machine = Speedy.Machine.fromPureSExpr(noPackages, SEImportValue(v))
    machine.run() match {
      case SResultFinalValue(value) => value
      case _ => throw new Error(s"error while translating value $v")
    }
  }
}
