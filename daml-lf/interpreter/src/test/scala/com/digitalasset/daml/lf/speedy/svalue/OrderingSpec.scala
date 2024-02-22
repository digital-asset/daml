// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy
package svalue

import com.daml.lf.crypto
import com.daml.lf.data.{Bytes, FrontStack, Ref}
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SExpr.{SEImportValue, SELet1, SELocF, SELocS, SEMakeClo}
import com.daml.lf.value.Value
import com.daml.lf.value.test.TypedValueGenerators.genAddend
import com.daml.lf.value.test.ValueGenerators.{comparableCoidsGen, suffixedV1CidGen}
import com.daml.lf.PureCompiledPackages
import com.daml.lf.typesig
import com.daml.lf.interpretation.Error.ContractIdComparability
import com.daml.lf.language.{Ast, LanguageMajorVersion, Util => AstUtil}
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

class OrderingSpecV1 extends OrderingSpec(LanguageMajorVersion.V1)
//class OrderingSpecV2 extends OrderingSpec(LanguageMajorVersion.V2)

class OrderingSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Inside
    with Matchers
    with Checkers
    with ScalaCheckDrivenPropertyChecks
    with ScalaCheckPropertyChecks {

  import SpeedyTestLib.loggingContext

  private[lf] def toAstType(typ: typesig.Type): Ast.Type = typ match {
    case typesig.TypeCon(name, typArgs) =>
      typArgs.foldLeft[Ast.Type](Ast.TTyCon(name.identifier))((acc, typ) =>
        Ast.TApp(acc, toAstType(typ))
      )
    case typesig.TypeNumeric(scale) =>
      AstUtil.TNumeric(Ast.TNat(scale))
    case typesig.TypePrim(prim, typArgs) =>
      import typesig.{PrimType => P}
      val init = prim match {
        case P.Bool => AstUtil.TBool
        case P.Int64 => AstUtil.TInt64
        case P.Text => AstUtil.TText
        case P.Date => AstUtil.TDate
        case P.Timestamp => AstUtil.TTimestamp
        case P.Party => AstUtil.TParty
        case P.ContractId => AstUtil.TContractId.cons
        case P.List => AstUtil.TList.cons
        case P.Unit => AstUtil.TUnit
        case P.Optional => AstUtil.TOptional.cons
        case P.TextMap => AstUtil.TTextMap.cons
        case P.GenMap => AstUtil.TGenMap.cons
      }
      typArgs.foldLeft[Ast.Type](init)((acc, typ) => Ast.TApp(acc, toAstType(typ)))
    case typesig.TypeVar(name) =>
      Ast.TVar(name)
  }

  private val pkgId = Ref.PackageId.assertFromString("pkgId")

  implicit def toTypeConName(s: String): Ref.TypeConName =
    Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

  implicit def toName(s: String): Ref.Name =
    Ref.Name.assertFromString(s)

  private val randomComparableValues: TableFor2[String, Gen[SValue]] = {
    import com.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
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
    implicit val cidArb: Arbitrary[Cid] = Arbitrary(suffixedV1CidGen)
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

  private def translatePrimValue(typ: typesig.Type, v: Value) = {

    val machine = Speedy.Machine.fromUpdateSExpr(
      PureCompiledPackages.Empty(Compiler.Config.Default(majorLanguageVersion)),
      transactionSeed = txSeed,
      updateSE =
        SELet1(SEImportValue(toAstType(typ), v), SEMakeClo(Array(SELocS(1)), 1, SELocF(0))),
      committers = committers,
    )

    machine.run() match {
      case SResultFinal(value) => value
      case _ => throw new Error(s"error while translating value $v")
    }
  }
}
