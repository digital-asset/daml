// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy
package svalue

import java.util

import com.daml.lf.crypto
import com.daml.lf.data.{FrontStack, ImmArray, Numeric, Ref, Time}
import com.daml.lf.language.{Ast, Util => AstUtil}
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SExpr.SEImportValue
import com.daml.lf.speedy.{SBuiltin, SExpr, SValue}
import com.daml.lf.value.Value
import com.daml.lf.value.TypedValueGenerators.genAddend
import com.daml.lf.value.ValueGenerators.{absCoidV0Gen, comparableAbsCoidsGen}
import com.daml.lf.PureCompiledPackages
import org.scalacheck.Arbitrary
import org.scalatest.prop.{
  GeneratorDrivenPropertyChecks,
  TableDrivenPropertyChecks,
  TableFor1,
  TableFor2
}
import org.scalatest.{Matchers, WordSpec}
import scalaz.{Order, Tag}
import scalaz.syntax.order._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.language.implicitConversions
import scala.util.Random

class OrderingSpec
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks {

  private val pkgId = Ref.PackageId.assertFromString("pkgId")

  implicit def toTypeConName(s: String): Ref.TypeConName =
    Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

  implicit def toName(s: String): Ref.Name =
    Ref.Name.assertFromString(s)

  private val EnumTypeCon: Ref.TypeConName = "Color"

  private val EnumCon1: Ref.Name = "Blue"
  private val EnumCon2: Ref.Name = "Green"
  private val EnumCon3: Ref.Name = "Red"

  private val Record0TypeCon: Ref.TypeConName = "Unit"
  private val Record2TypeCon: Ref.TypeConName = "Tuple"
  private val record2Fields = Ref.Name.Array("fst", "snd")

  private val VariantTypeCon: Ref.TypeConName = "Either"
  private val VariantCon1: Ref.Name = "Left"
  private val VariantCon2: Ref.Name = "Middle"
  private val VariantCon3: Ref.Name = "Right"

  private val units =
    List(SValue.SValue.Unit)
  private val bools =
    List(SValue.SValue.False, SValue.SValue.True)
  private val ints =
    List(SInt64(-1L), SInt64(0L), SInt64(1L))
  private val decimals =
    List("-10000.0000000000", "0.0000000000", "10000.0000000000")
      .map(x => SNumeric(Numeric.assertFromString(x)))
  private val numerics =
    List("-10000.", "0.", "10000.").map(SNumeric compose Numeric.assertFromString)
  private val texts =
    List("""a bit of text""", """some other text""", """some other text again""").map(SText)
  private val dates =
    List("1969-07-21", "1970-01-01", "2020-02-02").map(SDate compose Time.Date.assertFromString)
  private val timestamps =
    List(
      "1969-07-21T02:56:15.000000Z",
      "1970-01-01T00:00:00.000000Z",
      "2020-02-02T20:20:02.020000Z",
    ).map(STimestamp compose Time.Timestamp.assertFromString)
  private val parties =
    List("alice", "bob", "carol").map(SParty compose Ref.Party.assertFromString)
  private val absoluteContractId =
    List("a", "b", "c")
      .map(x => SContractId(Value.AbsoluteContractId.assertFromString("#" + x)))
//  private val relativeContractId =
//    List(0, 1).map(x => SContractId(Value.RelativeContractId(Value.NodeId(x))))
  private val contractIds = absoluteContractId //++ relativeContractId

  private val enums =
    List(EnumCon1, EnumCon2, EnumCon3).zipWithIndex.map {
      case (con, rank) => SEnum(EnumTypeCon, con, rank)
    }

  private val struct0 = List(SStruct(Ref.Name.Array.empty, ArrayList()))

  private val records0 = List(SRecord(Record0TypeCon, Ref.Name.Array.empty, ArrayList()))

  private val builtinTypeReps = List(
    Ast.BTUnit,
    Ast.BTBool,
    Ast.BTInt64,
    Ast.BTText,
    Ast.BTNumeric,
    Ast.BTTimestamp,
    Ast.BTDate,
    Ast.BTParty,
    Ast.BTContractId,
    Ast.BTArrow,
    Ast.BTOptional,
    Ast.BTList,
    Ast.BTTextMap,
    Ast.BTGenMap,
    Ast.BTAny,
    Ast.BTTypeRep,
    Ast.BTUpdate,
    Ast.BTScenario
  ).map(bt => STypeRep(Ast.TBuiltin(bt)))

  private val typeNatReps = List(
    Numeric.Scale.MinValue,
    Numeric.Scale.values(10),
    Numeric.Scale.MaxValue,
  ).map(s => STypeRep(Ast.TNat(s)))

  private val typeTyConReps1 = List(
    ("a", "a", "a"),
    ("a", "a", "b"),
    ("a", "b", "a"),
    ("a", "b", "b"),
    ("b", "a", "a"),
    ("b", "a", "b"),
    ("b", "b", "a"),
    ("b", "b", "b"),
  ).map {
    case (pkg, mod, name) =>
      STypeRep(
        Ast.TTyCon(
          Ref.Identifier(
            Ref.PackageId.assertFromString(pkg),
            Ref.QualifiedName(
              Ref.DottedName.assertFromString(mod),
              Ref.DottedName.assertFromString(name),
            ))))
  }

  private val typeTyConReps2 = {
    val names = List("a", "a.a", "a.b", "b.a", "b.b").map(Ref.DottedName.assertFromString)
    for {
      mod <- names
      name <- names
    } yield STypeRep(Ast.TTyCon(Ref.Identifier(pkgId, Ref.QualifiedName(mod, name))))
  }

  private val typeTyConReps3 = {
    val names = List("a", "aa", "ab", "ba", "bb").map(Ref.DottedName.assertFromString)
    for {
      mod <- names
      name <- names
    } yield STypeRep(Ast.TTyCon(Ref.Identifier(pkgId, Ref.QualifiedName(mod, name))))
  }

  private val typeStructReps = List(
    ImmArray.empty,
    ImmArray(Ref.Name.assertFromString("field0") -> AstUtil.TUnit),
    ImmArray(Ref.Name.assertFromString("field0") -> AstUtil.TInt64),
    ImmArray(Ref.Name.assertFromString("field1") -> AstUtil.TUnit),
    ImmArray(
      Ref.Name.assertFromString("field1") -> AstUtil.TUnit,
      Ref.Name.assertFromString("field2") -> AstUtil.TUnit,
    ),
    ImmArray(
      Ref.Name.assertFromString("field1") -> AstUtil.TUnit,
      Ref.Name.assertFromString("field2") -> AstUtil.TInt64,
    ),
    ImmArray(
      Ref.Name.assertFromString("field1") -> AstUtil.TInt64,
      Ref.Name.assertFromString("field2") -> AstUtil.TUnit,
    ),
    ImmArray(
      Ref.Name.assertFromString("field1") -> AstUtil.TUnit,
      Ref.Name.assertFromString("field3") -> AstUtil.TUnit,
    ),
  ).map(t => STypeRep(Ast.TStruct(t)))

  private val typeTypeAppRep = {
    val types = List(AstUtil.TBool, AstUtil.TInt64)
    for {
      a <- types
      b <- types
    } yield STypeRep(Ast.TApp(a, b))
  }

  private val typeReps =
    List(
      Ast.TBuiltin(Ast.BTUnit),
      Ast.TBuiltin(Ast.BTScenario),
      Ast.TTyCon(EnumTypeCon),
      Ast.TTyCon(VariantTypeCon),
      Ast.TNat(Numeric.Scale.MinValue),
      Ast.TNat(Numeric.Scale.MaxValue),
      Ast.TStruct(ImmArray.empty),
      Ast.TStruct(ImmArray(Ref.Name.assertFromString("field") -> AstUtil.TUnit)),
      Ast.TApp(Ast.TBuiltin(Ast.BTArrow), Ast.TBuiltin(Ast.BTUnit)),
      Ast.TApp(
        Ast.TApp(Ast.TBuiltin(Ast.BTArrow), Ast.TBuiltin(Ast.BTUnit)),
        Ast.TBuiltin(Ast.BTUnit)),
    ).map(STypeRep)

  private def mkRecord2(fst: List[SValue], snd: List[SValue]) =
    for {
      x <- fst
      y <- snd
    } yield SRecord(Record2TypeCon, record2Fields, ArrayList(x, y))

  private def mkVariant(as: List[SValue], bs: List[SValue]) =
    as.map(SVariant(VariantTypeCon, VariantCon1, 0, _)) ++
      as.map(SVariant(VariantTypeCon, VariantCon2, 1, _)) ++
      bs.map(SVariant(VariantTypeCon, VariantCon3, 2, _))

  private def mkStruct2(fst: List[SValue], snd: List[SValue]) =
    for {
      x <- fst
      y <- snd
    } yield SStruct(record2Fields, ArrayList(x, y))

  @tailrec
  private def mkList(
      values: List[SValue],
      i: Int,
      l0: List[List[SValue]] = List.empty): List[List[SValue]] =
    if (i <= 0)
      l0
    else {
      val l = for {
        h <- values
        t <- l0
      } yield h :: t
      mkList(values, i - 1, List.empty +: l)
    }

  private def lists(atLeast3Values: List[SValue]) = {
    val r = mkList(atLeast3Values.take(3), 4)
    assert(r.length == 40)
    r
  }

  private def optLists(atLeast3Values: List[SValue]) = {
    val r =
      mkList(SOptional(Option.empty) :: atLeast3Values.take(3).map(x => SOptional(Some(x))), 4)
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

  private def mkGenMaps(keys: List[SValue], lists: List[List[SValue]]): List[SValue] = {
    lists.map(xs => SGenMap(keys.iterator zip xs.iterator))
  }

  private def anys = {
    val wrappedInts = ints.map(SAny(AstUtil.TInt64, _))
    val wrappedAnyInts = wrappedInts.map(SAny(AstUtil.TAny, _))
    val wrappedIntOptional = ints.map(SAny(AstUtil.TOptional(AstUtil.TInt64), _))

    wrappedInts ++ wrappedAnyInts ++ wrappedIntOptional
  }

  private val comparableValues: TableFor1[TableFor1[SValue]] = Table(
    "comparable values",
    // Atomic values
    Table("Unit", units: _*),
    Table("Bool", bools: _*),
    Table("Int64", ints: _*),
    Table("Decimal", decimals: _*),
    Table("Numeric with scale 0", numerics: _*),
    Table("Text", texts: _*),
    Table("Date", dates: _*),
    Table("Timestamp", timestamps: _*),
    Table("Party", parties: _*),
    Table("ContractId", contractIds: _*),
    Table("Enum", enums: _*),
    Table("Record of size 0", records0: _*),
    Table("struct of size 0", struct0: _*),
    Table("Representation of builtin types", builtinTypeReps: _*),
    Table("Representation of nat types", typeNatReps: _*),
    Table("Representation of types constructors (1)", typeTyConReps1: _*),
    Table("Representation of types constructors (2)", typeTyConReps2: _*),
    Table("Representation of types constructors (3)", typeTyConReps3: _*),
    Table("Representation of struct types", typeStructReps: _*),
    Table("Representation of type applications", typeTypeAppRep: _*),
    Table("Representation of type", typeReps: _*),
    // 1 level nested values
    Table("Int64 record of size 2", mkRecord2(ints, ints): _*),
    Table("Text record of size 2", mkRecord2(texts, texts): _*),
    Table("Variant", mkVariant(texts, texts): _*),
    Table("Struct of size 2", mkStruct2(texts, texts): _*),
    Table("Optional", mkOptionals(texts): _*),
    Table("Int64 Lists", mkLists(lists(ints)): _*),
    Table("Int64 TextMap", mkTextMaps(lists(ints)): _*),
    Table("genMap_1", mkGenMaps(ints, lists(ints)): _*),
    // 2 level nested values
    Table("Int64 Option Records of size 2", mkRecord2(mkOptionals(texts), mkOptionals(texts)): _*),
    Table("Text Option Variants ", mkVariant(mkOptionals(texts), mkOptionals(texts)): _*),
    Table("Text Option Struct", mkStruct2(mkOptionals(texts), mkOptionals(texts)): _*),
    Table("Text Option Option", mkOptionals(mkOptionals(texts)): _*),
    Table("Int64 Option List", mkLists(optLists(ints)): _*),
    Table("Int64 Option TextMap", mkTextMaps(optLists(ints)): _*),
    Table("genMap_2", mkGenMaps(mkOptionals(ints), optLists(ints)): _*),
    // any
    Table("any", anys: _*),
  )

  private val lfFunction = SPAP(PBuiltin(SBuiltin.SBAddInt64), ArrayList(SInt64(1)), 2)

  private val funs = List(
    lfFunction,
    SPAP(PClosure(null, SExpr.SEVar(2), Array()), ArrayList(SValue.SValue.Unit), 2),
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

  val nonComparable: TableFor1[TableFor1[SValue]] =
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
//      Table("nonEquatable genMap", mkGenMaps(ints, nonEquatableLists(funs)): _*),
      Table("nonEquatable variant", mkVariant(funs, funs): _*),
      Table("nonEquatable any", nonEquatableAnys: _*),
    )

  val nonEquatableWithEquatableValues: TableFor2[SValue, SValue] =
    Table(
      "nonEquatable values" -> "equatable values",
      SOptional(None) ->
        SOptional(Some(lfFunction)),
      SList(FrontStack.empty) ->
        SList(FrontStack(lfFunction)),
      STextMap(HashMap.empty) ->
        STextMap(HashMap("a" -> lfFunction)),
      SGenMap.Empty -> SGenMap(SInt64(0) -> lfFunction),
      SVariant(VariantTypeCon, VariantCon1, 0, SInt64(0)) ->
        SVariant(VariantTypeCon, VariantCon2, 1, lfFunction),
      SAny(AstUtil.TInt64, SInt64(1)) ->
        SAny(AstUtil.TFun(AstUtil.TInt64, AstUtil.TInt64), lfFunction),
    )

  "Order.compare" should {

    // In the following tests, we check only well-type equalities

    "be reflexive on comparable values" in {
      forEvery(comparableValues)(atoms =>
        forEvery(atoms) { x =>
          assert(Ordering.compare(x, x) == 0)
      })
    }

    "be symmetric on comparable values" in {
      forEvery(comparableValues) { atoms =>
        for {
          x <- atoms
          y <- atoms
        } assert(Ordering.compare(x, y).signum == -Ordering.compare(y, x).signum)
      }
    }

    "be transitive on comparable values" in {
      forEvery(comparableValues) { atoms =>
        val rAtoms = Random.shuffle(atoms).take(10)
        for {
          x <- rAtoms
          y <- rAtoms
          if Ordering.compare(x, y) <= 0
          z <- rAtoms
          if Ordering.compare(y, z) <= 0
        } assert(Ordering.compare(x, z) <= 0)
      }
    }

    "agree with equality comparable values" in {
      forEvery(comparableValues) { atoms =>
        for {
          x <- atoms
          y <- atoms
        } {
          assert((Ordering.compare(x, y) == 0) == Equality.areEqual(x, y))
        }
      }
    }

    "preserves the specified order" in {
      forEvery(comparableValues) { atoms =>
        val inputs = atoms.toList
        val sorted = new Random(0).shuffle(inputs).sorted(Ordering)
        assert((inputs zip sorted).forall { case (x, y) => Equality.areEqual(x, y) })
      }
    }

  }

  // A problem in this test *usually* indicates changes that need to be made
  // in Value.orderInstance or TypedValueGenerators, rather than to svalue.Ordering.
  // The tests are here as this is difficult to test outside daml-lf/interpreter.
  "txn Value Ordering" should {
    import Value.{AbsoluteContractId => Cid}
    // SContractId V1 ordering is nontotal so arbitrary generation of them is unsafe to use
    implicit val cidArb: Arbitrary[Cid] = Arbitrary(absCoidV0Gen)
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

    "match global AbsoluteContractId ordering" in forEvery(Table("gen", comparableAbsCoidsGen: _*)) {
      coidGen =>
        forAll(coidGen, coidGen, minSuccessful(50)) { (a, b) =>
          Cid.`AbsCid Order`.order(a, b) should ===(cidOrd.order(a, b))
        }
    }
  }

  private def ArrayList[X](as: X*): util.ArrayList[X] =
    new util.ArrayList[X](as.asJava)

  private val txSeed = crypto.Hash.hashPrivateKey("SBuiltinTest")
  private def initMachine(expr: SExpr) = Speedy.Machine fromSExpr (
    sexpr = expr,
    compiledPackages = PureCompiledPackages(Map.empty, Map.empty, Compiler.NoProfile),
    submissionTime = Time.Timestamp.now(),
    seeding = InitialSeeding.TransactionSeed(txSeed),
    globalCids = Set.empty,
  )

  private def translatePrimValue(v: Value[Value.AbsoluteContractId]) = {
    val machine = initMachine(SEImportValue(v))
    machine.run() match {
      case SResultFinalValue(value) => value
      case _ => throw new Error(s"error while translating value $v")
    }
  }
}
