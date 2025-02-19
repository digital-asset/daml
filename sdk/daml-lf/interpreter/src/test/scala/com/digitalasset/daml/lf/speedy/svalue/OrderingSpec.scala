// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy
package svalue

import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{Bytes, FrontStack, Ref}
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.test.ValueGenerators.comparableCoidsGen
import com.digitalasset.daml.lf.typesig
import com.digitalasset.daml.lf.interpretation.Error.ContractIdComparability
import com.digitalasset.daml.lf.language.{Ast, Util => AstUtil}
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
import scalaz.Order
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
        case P.Bytes => AstUtil.TBytes
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
    import com.digitalasset.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
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
    implicit val svalueOrd: Order[SValue] = Order fromScalaOrdering Ordering
    implicit val cidOrd: Order[Cid] = svalueOrd contramap SContractId

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
}
