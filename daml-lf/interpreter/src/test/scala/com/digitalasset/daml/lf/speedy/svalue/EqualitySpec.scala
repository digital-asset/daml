// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy.svalue

import java.util

import com.daml.lf.data.{FrontStack, ImmArray, Numeric, Ref, Time}
import com.daml.lf.language.{Ast, Util => AstUtil}
import com.daml.lf.speedy.Profile.LabelUnset
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{SBuiltin, SExpr, SValue}
import com.daml.lf.value.Value.ContractId
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor2}
import org.scalatest.{Matchers, WordSpec}
import scalaz._
import Scalaz._
import com.daml.lf.speedy.SError.SErrorCrash

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.language.implicitConversions

class EqualitySpec extends WordSpec with Matchers with TableDrivenPropertyChecks {

  private val pkgId = Ref.PackageId.assertFromString("pkgId")

  implicit def toTypeConName(s: String): Ref.TypeConName =
    Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

  implicit def toName(s: String): Ref.Name =
    Ref.Name.assertFromString(s)

  private val EnumTypeCon: Ref.TypeConName = "Color"

  private val EnumCon1: Ref.Name = "Red"
  private val EnumCon2: Ref.Name = "Green"

  private val Record0TypeCon: Ref.TypeConName = "Unit"
  private val Record2TypeCon: Ref.TypeConName = "Tuple"
  private val record2Fields = ImmArray[Ref.Name]("fst", "snd")

  private val VariantTypeCon: Ref.TypeConName = "Either"
  private val VariantCon1: Ref.Name = "Left"
  private val VariantCon2: Ref.Name = "Right"

  private val struct2Fields = Ref.Name.Array("fst", "snd")

  private val unit = SValue.SValue.Unit

  private val units =
    List(unit)
  private val bools =
    List(SValue.SValue.True, SValue.SValue.False)
  private val ints =
    List(SInt64(-1L), SInt64(0L), SInt64(1L))
  private val decimals =
    List("-10000.0000000000", "0.0000000000", "10000.0000000000")
      .map(x => SNumeric(Numeric.assertFromString(x)))
  private val numerics =
    List("-10000.", "0.", "10000.").map(SNumeric compose Numeric.assertFromString)
  private val texts =
    List(""""some text"""", """"some other text"""").map(SText)
  private val dates =
    List("1969-07-21", "1970-01-01").map(SDate compose Time.Date.assertFromString)
  private val timestamps =
    List("1969-07-21T02:56:15.000000Z", "1970-01-01T00:00:00.000000Z")
      .map(STimestamp compose Time.Timestamp.assertFromString)
  private val parties =
    List("alice", "bob").map(SParty compose Ref.Party.assertFromString)
  private val contractIds =
    List("a", "b").map(x => SContractId(ContractId.assertFromString("#" + x)))

  private val enums = List(
    SEnum(EnumTypeCon, EnumCon1, 0),
    SEnum(EnumTypeCon, EnumCon2, 1)
  )

  private val struct0 = List(SStruct(Ref.Name.Array.empty, ArrayList()))

  private val records0 = List(SRecord(Record0TypeCon, ImmArray.empty, ArrayList()))

  private val typeReps = List(
    AstUtil.TUnit,
    AstUtil.TList(AstUtil.TContractId(Ast.TTyCon(Record0TypeCon))),
    AstUtil.TUpdate(Ast.TTyCon(EnumTypeCon)),
  ).map(STypeRep)

  private def mkRecord2(fst: List[SValue], snd: List[SValue]) =
    for {
      x <- fst
      y <- snd
    } yield SRecord(Record2TypeCon, record2Fields, ArrayList(x, y))

  private def mkVariant(as: List[SValue], bs: List[SValue]) =
    as.map(SVariant(VariantTypeCon, VariantCon1, 0, _)) ++
      bs.map(SVariant(VariantTypeCon, VariantCon2, 1, _))

  private def mkStruct2(fst: List[SValue], snd: List[SValue]) =
    for {
      x <- fst
      y <- snd
    } yield SStruct(struct2Fields, ArrayList(x, y))

  private def lists(atLeast3Values: List[SValue]) = {
    val s = atLeast3Values.take(3)
    val r = List.iterate(List.empty[List[SValue]], 4)(s :: _).flatMap(_.sequence)
    assert(r.length == 40)
    r
  }

  private def optLists(atLeast3Values: List[SValue]) = {
    val s = SOptional(Option.empty) :: atLeast3Values.take(3).map(x => SOptional(Some(x)))
    val r = List.iterate(List.empty[List[SValue]], 4)(s :: _).flatMap(_.sequence)
    assert(r.length == 85)
    r
  }

  private def mkOptionals(values: List[SValue]): List[SValue] =
    SOptional(None) +: values.map(x => SOptional(Some(x)))

  private def mkLists(lists: List[List[SValue]]): List[SValue] =
    lists.map(xs => SList(FrontStack(xs)))

  private def mkTextMaps(lists: List[List[SValue]]): List[SValue] = {
    val keys = List("a", "b", "c")
    lists.map(xs => STextMap(HashMap(keys zip xs: _*)))
  }

  private def mkGenMaps(keys: List[SValue], lists: List[List[SValue]]): List[SValue] =
    lists.map(xs => SGenMap((keys.iterator) zip (xs.iterator)))

  private def anys = {
    val wrappedInts = ints.map(SAny(AstUtil.TInt64, _))
    val wrappedIntOptional = ints.map(SAny(AstUtil.TOptional(AstUtil.TInt64), _))
    val wrappedAnyInts = wrappedInts.map(SAny(AstUtil.TAny, _))
    // add a bit more cases  here
    wrappedInts ++ wrappedIntOptional ++ wrappedAnyInts
  }

  private val equatableValues: TableFor1[TableFor1[SValue]] = Table(
    "equatable values",
    Table("textMap_1", mkTextMaps(lists(ints)): _*),
    // Atomic values
    Table("Unit", units: _*),
    Table("Bool", bools: _*),
    Table("Int64", ints: _*),
    Table("Decimal", decimals: _*),
    Table("Numeric0", numerics: _*),
    Table("Text", texts: _*),
    Table("Date", dates: _*),
    Table("Timestamp", timestamps: _*),
    Table("party", parties: _*),
    Table("contractId", contractIds: _*),
    Table("enum", enums: _*),
    Table("record0", records0: _*),
    Table("struct0", struct0: _*),
    Table("typeRep", typeReps: _*),
    // 1 level nested values
    Table("record2_1", mkRecord2(texts, texts): _*),
    Table("variant_1", mkVariant(texts, texts): _*),
    Table("struct2_1", mkStruct2(texts, texts): _*),
    Table("optional_1", mkOptionals(texts): _*),
    Table("list_1", mkLists(lists(ints)): _*),
    Table("textMap_1", mkTextMaps(lists(ints)): _*),
    Table("genMap_1", mkGenMaps(ints, lists(ints)): _*),
    // 2 level nested values
    Table("record2_2", mkRecord2(mkOptionals(texts), mkOptionals(texts)): _*),
    Table("variant_2", mkVariant(mkOptionals(texts), mkOptionals(texts)): _*),
    Table("struct2_2", mkStruct2(mkOptionals(texts), mkOptionals(texts)): _*),
    Table("optional_2", mkOptionals(mkOptionals(texts)): _*),
    Table("list_2", mkLists(optLists(ints)): _*),
    Table("textMap_2", mkTextMaps(optLists(ints)): _*),
    Table("genMap_2", mkGenMaps(mkOptionals(ints), optLists(ints)): _*),
    // any
    Table("any", anys: _*),
  )

  private val fun = SPAP(PBuiltin(SBuiltin.SBAddInt64), ArrayList(SInt64(1)), 2)

  private val funs = List(
    fun,
    SPAP(PClosure(LabelUnset, SExpr.SEVar(2), Array()), ArrayList(SValue.SValue.Unit), 2),
  )

  private def nonEquatableLists(atLeast2InEquatableValues: List[SValue]) = {
    val a :: b :: _ = atLeast2InEquatableValues
    List(
      List(a),
      List(b),
      List(a, a),
      List(b, b),
      List(a, b),
    )
  }

  private def nonEquatableAnys = {
    val Type0 = AstUtil.TFun(AstUtil.TInt64, AstUtil.TInt64)
    val wrappedFuns = funs.map(SAny(Type0, _))
    val wrappedFunOptional = funs.map(SAny(AstUtil.TOptional(Type0), _))
    val wrappedAnyFuns = wrappedFuns.map(SAny(AstUtil.TAny, _))
    // add a bit more cases  here
    wrappedFuns ++ wrappedFunOptional ++ wrappedAnyFuns
  }

  private val nonEquatableValues: TableFor1[TableFor1[SValue]] =
    Table(
      "nonEquatable values",
      Table("funs", funs: _*),
      Table("token", SValue.SToken),
      Table("nat", SValue.STNat(Numeric.Scale.MinValue), SValue.STNat(Numeric.Scale.MaxValue)),
      Table("nonEquatable record", mkRecord2(funs, units) ++ mkRecord2(units, funs): _*),
      Table("nonEquatable struct", mkStruct2(funs, units) ++ mkStruct2(units, funs): _*),
      Table("nonEquatable optional", funs.map(x => SOptional(Some(x))): _*),
      Table("nonEquatable list", mkLists(nonEquatableLists(funs)): _*),
      Table("nonEquatable textMap", mkTextMaps(nonEquatableLists(funs)): _*),
      Table("nonEquatable genMap", mkGenMaps(ints, nonEquatableLists(funs)): _*),
      Table("nonEquatable variant", mkVariant(funs, funs): _*),
      Table("nonEquatable any", nonEquatableAnys: _*),
    )

  private val nonEquatableWithEquatableValues: TableFor2[SValue, SValue] =
    Table(
      "nonEquatable values" -> "equatable values",
      SOptional(None) ->
        SOptional(Some(fun)),
      SList(FrontStack.empty) ->
        SList(FrontStack(fun)),
      STextMap(HashMap.empty) ->
        STextMap(HashMap("a" -> fun)),
      SGenMap.Empty -> SGenMap(SInt64(0) -> fun),
      SVariant(VariantTypeCon, VariantCon1, 0, SInt64(0)) ->
        SVariant(VariantTypeCon, VariantCon2, 1, fun),
      SAny(AstUtil.TInt64, SInt64(1)) ->
        SAny(AstUtil.TFun(AstUtil.TInt64, AstUtil.TInt64), fun),
    )

  "Equality.areEqual" should {

    import Equality.areEqual

    // In the following tests, we check only well-type equalities

    "be reflexive on equatable values" in {
      forEvery(equatableValues)(atoms => forEvery(atoms)(x => assert(areEqual(x, x))))
    }

    "return false when applied on two different equatable values" in {
      forAll(equatableValues)(atoms =>
        for {
          (x, i) <- atoms.zipWithIndex
          (y, j) <- atoms.zipWithIndex
          if i != j
        } assert(!areEqual(x, y)))
    }

    "fail on non-equatable values" in {
      forEvery(nonEquatableValues)(atoms =>
        forEvery(atoms)(x => an[SErrorCrash] should be thrownBy areEqual(x, x)))
    }

    "return false when applied on an equatable and a nonEquatable values" in {
      forEvery(nonEquatableWithEquatableValues) {
        case (nonEq, eq) =>
          assert(!areEqual(nonEq, eq))
          assert(!areEqual(eq, nonEq))
      }
    }

    val x1 = SVariant(VariantTypeCon, VariantCon1, 0, SInt64(1))
    val x2 = SVariant(VariantTypeCon, VariantCon1, 0, SInt64(2))
    val x3 = SVariant(VariantTypeCon, VariantCon1, 0, SInt64(3))
    val u = SVariant(VariantTypeCon, VariantCon2, 1, fun)

    "shortcut failure in records " in {
      def record(x: SValue, y: SValue) =
        SRecord(Record2TypeCon, record2Fields, ArrayList(x, y))

      val negativeTestCases = Table(
        "first arg" -> "second arg",
        record(u, x1) -> record(u, x1),
        record(u, x1) -> record(u, x2),
        record(x1, u) -> record(x1, u)
      )
      val positiveTestCases = Table(
        "first arg" -> "second arg",
        record(x1, u) -> record(x2, u)
      )

      forEvery(negativeTestCases)((x, y) => an[SErrorCrash] should be thrownBy areEqual(x, y))
      forEvery(positiveTestCases)((x, y) => areEqual(x, y))
    }

    "shortcut failure in variant" in {
      val negativeTestCases = Table(
        "first arg" -> "second arg",
        u -> u,
      )
      val positiveTestCases = Table(
        "first arg" -> "second arg",
        u -> x1
      )
      forEvery(negativeTestCases) { (x, y) =>
        an[SErrorCrash] should be thrownBy areEqual(x, y)
        an[SErrorCrash] should be thrownBy areEqual(y, x)
      }
      forEvery(positiveTestCases) { (x, y) =>
        areEqual(x, y)
        areEqual(y, x)
      }
    }

    "shortcut failure in list" in {
      def list(xs: SValue*) = SList(FrontStack(xs))

      val negativeTestCases = Table(
        "first arg" -> "second arg",
        list(u) -> list(u),
        list(u, x1) -> list(u),
        list(x1, u) -> list(x1, u),
        list(x1, u, x1) -> list(x1, u),
      )
      val positiveTestCases = Table(
        "first arg" -> "second arg",
        list(x1, u) -> list(x2, u),
        list(u) -> list(),
        list() -> list(u)
      )

      forEvery(negativeTestCases) { (x, y) =>
        an[SErrorCrash] should be thrownBy areEqual(x, y)
        an[SErrorCrash] should be thrownBy areEqual(y, x)
      }
      forEvery(positiveTestCases) { (x, y) =>
        areEqual(x, y)
        areEqual(y, x)
      }
    }

    "shortcut failure in optional" in {
      val negativeTestCases = Table(
        "first arg" -> "second arg",
        SOptional(Some(u)) -> SOptional(Some(u)),
      )
      val positiveTestCases = Table(
        "first arg" -> "second arg",
        SOptional(None) -> SOptional(Some(u))
      )
      forEvery(negativeTestCases) { (x, y) =>
        an[SErrorCrash] should be thrownBy areEqual(x, y)
        an[SErrorCrash] should be thrownBy areEqual(y, x)
      }
      forEvery(positiveTestCases) { (x, y) =>
        areEqual(x, y)
        areEqual(y, x)
      }
    }

    "shortcut failure in text map" in {
      def map(xs: (String, SValue)*) = STextMap(HashMap(xs: _*))

      val k1 = "a"
      val k2 = "b"
      val k3 = "c"
      assert(k1 < k2)
      assert(k2 < k3)

      val negativeTestCases = Table(
        "first arg" -> "second arg",
        map(k1 -> u) -> map(k1 -> u),
        map(k1 -> u, k2 -> x1) -> map(k1 -> u),
        map(k1 -> u, k2 -> x1) -> map(k1 -> u, k3 -> x1),
        map(k1 -> u, k2 -> x1) -> map(k1 -> u, k2 -> x2),
        map(k1 -> x1, k2 -> u) -> map(k1 -> x1, k2 -> u),
      )
      val positiveTestCases = Table(
        "first arg" -> "second arg",
        map(k1 -> x1, k2 -> u) -> map(k1 -> x2, k2 -> u),
        map(k1 -> x1) -> map(k2 -> u),
        map(k1 -> x1, k2 -> u) -> map(k1 -> x1, k3 -> u),
        map(k1 -> x1, k2 -> u) -> map(k2 -> u),
        map(k1 -> x1, k2 -> u) -> map(k2 -> u),
        map(k1 -> x1, k2 -> u) -> map(k2 -> x1, k3 -> u),
        map(k1 -> u) -> map(),
      )
      forEvery(negativeTestCases) { (x, y) =>
        an[SErrorCrash] should be thrownBy areEqual(x, y)
        an[SErrorCrash] should be thrownBy areEqual(y, x)
      }
      forEvery(positiveTestCases) { (x, y) =>
        areEqual(x, y)
        areEqual(y, x)
      }
    }

    "shortcut failure in gen map" in {
      def map(xs: (SValue, SValue)*) = SGenMap(xs.iterator)

      val k1 = x1
      val k2 = x2
      val k3 = x3
      assert(Ordering.compare(k1, k2) < 0)
      assert(Ordering.compare(k2, k3) < 0)

      val negativeTestCases = Table(
        "first arg" -> "second arg",
        map(k1 -> u) -> map(k1 -> u),
        map(k1 -> u, k2 -> x1) -> map(k1 -> u),
        map(k1 -> u, k2 -> x1) -> map(k1 -> u, k3 -> x1),
        map(k1 -> u, k2 -> x1) -> map(k1 -> u, k2 -> x2),
        map(k1 -> x1, k2 -> u) -> map(k1 -> x1, k2 -> u),
      )
      val positiveTestCases = Table(
        "first arg" -> "second arg",
        map(k1 -> x1, k2 -> u) -> map(k1 -> x2, k2 -> u),
        map(k1 -> x1) -> map(k2 -> u),
        map(k1 -> x1, k2 -> u) -> map(k1 -> x1, k3 -> u),
        map(k1 -> x1, k2 -> u) -> map(k2 -> u),
        map(k1 -> x1, k2 -> u) -> map(k2 -> u),
        map(k1 -> x1, k2 -> u) -> map(k2 -> x1, k3 -> u),
        map(k1 -> u) -> map(),
      )
      forEvery(negativeTestCases) { (x, y) =>
        an[SErrorCrash] should be thrownBy areEqual(x, y)
        an[SErrorCrash] should be thrownBy areEqual(y, x)
      }
      forEvery(positiveTestCases) { (x, y) =>
        areEqual(x, y)
        areEqual(y, x)
      }
    }

    "shortcut failure in struct" in {
      def struct(x: SValue, y: SValue) =
        SStruct(struct2Fields, ArrayList(x, y))

      val negativeTestCases = Table(
        "first arg" -> "second arg",
        struct(u, x1) -> struct(u, x1),
        struct(u, x1) -> struct(u, x2),
        struct(x1, u) -> struct(x1, u)
      )
      val positiveTestCases = Table(
        "first arg" -> "second arg",
        struct(x1, u) -> struct(x2, u)
      )

      forEvery(negativeTestCases)((x, y) => an[SErrorCrash] should be thrownBy areEqual(x, y))
      forEvery(positiveTestCases)((x, y) => areEqual(x, y))
    }

  }

  "Hasher.hashCode" should {

    "not fail on equatable values" in {
      forEvery(equatableValues)(atoms => forEvery(atoms)(Hasher.hash))
    }

    "fail on non-equatable values" in {
      forEvery(nonEquatableValues)(atoms =>
        forEvery(atoms)(x => a[Hasher.NonHashableSValue] should be thrownBy Hasher.hash(x)))
    }

  }

  private def ArrayList[X](as: X*): util.ArrayList[X] =
    new util.ArrayList[X](as.asJava)

}
