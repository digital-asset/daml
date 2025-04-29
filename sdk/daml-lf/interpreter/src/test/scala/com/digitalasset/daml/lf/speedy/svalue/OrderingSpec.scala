// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy
package svalue

import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{Bytes, FrontStack, Ref, Time}
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

    "fail when trying to compare local/relative contract ID with global/relative/absolute contract ID with same prefix" in {

      val discriminator1 = crypto.Hash.hashPrivateKey("discriminator1")
      val discriminator2 = crypto.Hash.hashPrivateKey("discriminator2")
      val suffix1 = Bytes.assertFromString("00")
      val suffix2 = Bytes.assertFromString("80")
      val suffix3 = Bytes.assertFromString("11")

      val cid10 = Value.ContractId.V1(discriminator1, Bytes.Empty)
      val cid11 = Value.ContractId.V1(discriminator1, suffix1)
      val cid12 = Value.ContractId.V1(discriminator1, suffix2)
      val cid21 = Value.ContractId.V1(discriminator2, suffix1)

      val local1 = Value.ContractId.V2.unsuffixed(Time.Timestamp.MinValue, discriminator1).local
      val local2 = Value.ContractId.V2.unsuffixed(Time.Timestamp.MinValue, discriminator2).local
      val cid30 = Value.ContractId.V2(local1, Bytes.Empty)
      val cid31 = Value.ContractId.V2(local1, suffix1)
      val cid32 = Value.ContractId.V2(local1, suffix2)
      val cid41 = Value.ContractId.V2(local2, suffix1)
      val cid43 = Value.ContractId.V2(local2, suffix3)

      val List(vCid10, vCid11, vCid12, vCid21, vCid30, vCid31, vCid32, vCid41, vCid43) =
        List(cid10, cid11, cid12, cid21, cid30, cid31, cid32, cid41, cid43).map(SContractId)

      val negativeTestCases =
        Table(
          "cid1" -> "cid2",
          vCid10 -> vCid10,
          vCid11 -> vCid11,
          vCid11 -> vCid12,
          vCid11 -> vCid21,
          vCid30 -> vCid30,
          vCid31 -> vCid31,
          vCid31 -> vCid41,
          vCid31 -> vCid43,
          vCid32 -> vCid43,
          vCid10 -> vCid30,
          vCid10 -> vCid31,
          vCid11 -> vCid30,
        )

      val positiveLocalTestCases = Table(
        "local Cid" -> "global/absolute/relative Cid",
        vCid10 -> cid11,
        vCid10 -> cid12,
        vCid30 -> cid31,
        vCid30 -> cid32,
      )

      val positiveRelativeTestCases = Table(
        "relative Cid" -> "absolute/relative Cid",
        // relative -> absolute
        cid31 -> cid32,
        // relative -> relative
        cid41 -> cid43,
      )

      forEvery(negativeTestCases) { (cid1, cid2) =>
        (Ordering.compare(cid1, cid2) == 0) shouldBe (cid1 == cid2)
        (Ordering.compare(cid2, cid1) == 0) shouldBe (cid1 == cid2)
      }

      forEvery(positiveLocalTestCases) { (localCid, globalCid) =>
        Try(Ordering.compare(localCid, SContractId(globalCid))) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(globalCid)))
        Try(Ordering.compare(SContractId(globalCid), localCid)) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(globalCid)))
      }

      forEvery(positiveRelativeTestCases) { (relativeCid, otherCid) =>
        Try(Ordering.compare(SContractId(relativeCid), SContractId(otherCid))) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(relativeCid)))
        Try(Ordering.compare(SContractId(otherCid), SContractId(relativeCid))) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(otherCid)))
      }
    }
  }
}
