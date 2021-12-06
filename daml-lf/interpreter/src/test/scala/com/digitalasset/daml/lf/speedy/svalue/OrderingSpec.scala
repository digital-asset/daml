// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy
package svalue

import com.daml.lf.crypto
import com.daml.lf.data.{Bytes, FrontStack, Ref}
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SExpr.{SEApp, SEImportValue, SELocA, SEMakeClo}
import com.daml.lf.value.Value
import com.daml.lf.value.test.TypedValueGenerators.genAddend
import com.daml.lf.value.test.ValueGenerators.{cidV0Gen, comparableCoidsGen}
import com.daml.lf.PureCompiledPackages
import com.daml.lf.iface
import com.daml.lf.interpretation.Error.ContractIdComparability
import com.daml.lf.language.{Ast, Util => AstUtil}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Inside
import org.scalatest.prop.TableFor2
import org.scalatestplus.scalacheck.{
  Checkers,
  ScalaCheckDrivenPropertyChecks,
  ScalaCheckPropertyChecks,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.{Order, Tag}
import scalaz.syntax.order._
import scalaz.scalacheck.{ScalazProperties => SzP}

import scala.language.implicitConversions
import scala.util.{Failure, Try}

class OrderingSpec
    extends AnyWordSpec
    with Inside
    with Matchers
    with Checkers
    with ScalaCheckDrivenPropertyChecks
    with ScalaCheckPropertyChecks {

  private[lf] def toAstType(typ: iface.Type): Ast.Type = typ match {
    case iface.TypeCon(name, typArgs) =>
      typArgs.foldLeft[Ast.Type](Ast.TTyCon(name.identifier))((acc, typ) =>
        Ast.TApp(acc, toAstType(typ))
      )
    case iface.TypeNumeric(scale) =>
      AstUtil.TNumeric(Ast.TNat(scale))
    case iface.TypePrim(prim, typArgs) =>
      val init = prim match {
        case iface.PrimTypeBool => AstUtil.TBool
        case iface.PrimTypeInt64 => AstUtil.TInt64
        case iface.PrimTypeText => AstUtil.TText
        case iface.PrimTypeDate => AstUtil.TDate
        case iface.PrimTypeTimestamp => AstUtil.TTimestamp
        case iface.PrimTypeParty => AstUtil.TParty
        case iface.PrimTypeContractId => AstUtil.TContractId.cons
        case iface.PrimTypeList => AstUtil.TList.cons
        case iface.PrimTypeUnit => AstUtil.TUnit
        case iface.PrimTypeOptional => AstUtil.TOptional.cons
        case iface.PrimTypeTextMap => AstUtil.TTextMap.cons
        case iface.PrimTypeGenMap => AstUtil.TGenMap.cons
      }
      typArgs.foldLeft[Ast.Type](init)((acc, typ) => Ast.TApp(acc, toAstType(typ)))
    case iface.TypeVar(name) =>
      Ast.TVar(name)
  }

  private val pkgId = Ref.PackageId.assertFromString("pkgId")

  implicit def toTypeConName(s: String): Ref.TypeConName =
    Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

  implicit def toName(s: String): Ref.Name =
    Ref.Name.assertFromString(s)

  private val randomComparableValues: TableFor2[String, Gen[SValue]] = {
    import com.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
    implicit val cidArb: Arbitrary[Value.ContractId] = Arbitrary(Gen.fail[Value.ContractId])
    def r(name: String, va: VA)(sv: va.Inj => SValue) =
      (name, va.injarb.arbitrary map sv)
    Table(
      ("comparable value subset", "generator"),
      Seq(
        r("Int64", VA.int64)(SInt64),
        r("Text", VA.text)(SText),
        r("Int64 Option List", VA.list(VA.optional(VA.int64))) { loi =>
          SList(loi.map(oi => SOptional(oi map SInt64)).to(FrontStack))
        },
      ) ++
        comparableCoidsGen.zipWithIndex.map { case (g, ix) =>
          (s"ContractId $ix", g map SContractId)
        }: _*
    )
  }

  "Order.compare" should {
    "be lawful on each subset" in forEvery(randomComparableValues) { (_, svGen) =>
      implicit val svalueOrd: Order[SValue] = Order fromScalaOrdering Ordering
      implicit val svalueArb: Arbitrary[SValue] = Arbitrary(svGen)
      forEvery(Table(("law", "prop"), SzP.order.laws[SValue].properties.toSeq: _*)) { (_, p) =>
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
    implicit val cidOrd: Order[Cid] = svalueOrd contramap SContractId
    val EmptyScope: Value.LookupVariantEnum = _ => None

    "match SValue Ordering" in forAll(genAddend, minSuccessful(100)) { va =>
      import va.{injarb, injshrink}
      implicit val valueOrd: Order[Value] = Tag unsubst Value.orderInstance(EmptyScope)
      forAll(minSuccessful(20)) { (a: va.Inj, b: va.Inj) =>
        import va.injord
        val ta = va.inj(a)
        val tb = va.inj(b)
        val bySvalue = translatePrimValue(va.t, ta) ?|? translatePrimValue(va.t, tb)
        (a ?|? b, ta ?|? tb) should ===((bySvalue, bySvalue))
      }
    }

    "match global ContractId ordering" in forEvery(Table("gen", comparableCoidsGen: _*)) {
      coidGen =>
        forAll(coidGen, coidGen, minSuccessful(50)) { (a, b) =>
          Cid.`Cid Order`.order(a, b) should ===(cidOrd.order(a, b))
        }
    }

    "fail when trying to compare local contract ID with global contract ID with same discriminator" in {

      val discriminator1 = crypto.Hash.hashPrivateKey("discriminator1")
      val discriminator2 = crypto.Hash.hashPrivateKey("discriminator2")
      val suffix1 = Bytes.assertFromString("00")
      val suffix2 = Bytes.assertFromString("01")

      val cid10 = Value.ContractId.V1(discriminator1, Bytes.Empty)
      val cid11 = Value.ContractId.V1(discriminator1, suffix1)
      val cid12 = Value.ContractId.V1(discriminator1, suffix2)
      val cid21 = Value.ContractId.V1(discriminator2, suffix1)

      val List(vCid10, vCid11, vCid12, vCid21) = List(cid10, cid11, cid12, cid21).map(SContractId)

      val negativeTestCases =
        Table(
          "cid1" -> "cid2",
          vCid10 -> vCid10,
          vCid11 -> vCid11,
          vCid11 -> vCid12,
          vCid11 -> vCid21,
        )

      val positiveTestCases = Table(
        "glovalCid2",
        cid11,
        cid12,
      )

      forEvery(negativeTestCases) { (cid1, cid2) =>
        Ordering.compare(cid1, cid2) == 0 shouldBe cid1 == cid2
        Ordering.compare(cid2, cid1) == 0 shouldBe cid1 == cid2
      }

      forEvery(positiveTestCases) { globalCid =>
        Try(Ordering.compare(vCid10, SContractId(globalCid))) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(globalCid)))
        Try(Ordering.compare(SContractId(globalCid), vCid10)) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(globalCid)))
      }

    }
  }

  private[this] val txSeed = crypto.Hash.hashPrivateKey("OrderingSpec")
  private[this] val committers = Set(Ref.Party.assertFromString("Alice"))

  private def translatePrimValue(typ: iface.Type, v: Value) = {

    val machine = Speedy.Machine.fromUpdateSExpr(
      PureCompiledPackages.Empty,
      transactionSeed = txSeed,
      updateSE = SEApp(SEMakeClo(Array(), 2, SELocA(0)), Array(SEImportValue(toAstType(typ), v))),
      committers = committers,
    )

    machine.run() match {
      case SResultFinalValue(value) => value
      case _ => throw new Error(s"error while translating value $v")
    }
  }
}
