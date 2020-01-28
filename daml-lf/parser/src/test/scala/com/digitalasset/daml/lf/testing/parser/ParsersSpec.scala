// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import java.math.BigDecimal

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{ImmArray, Numeric, Time}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.testing.parser.Implicits._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.language.implicitConversions

class ParsersSpec extends WordSpec with TableDrivenPropertyChecks with Matchers {

  private implicit def toScale(i: Int): Numeric.Scale = Numeric.Scale.assertFromInt(i)

  "kind parser" should {

    "parses properly kinds" in {
      val testCases = Table[String, Kind](
        "string to parse" -> "expected kind",
        "*" -> KStar,
        "nat" -> KNat,
        "* -> *" -> KArrow(KStar, KStar),
        "* -> * -> *" -> KArrow(KStar, KArrow(KStar, KStar)),
        "(* -> *) -> *" -> KArrow(KArrow(KStar, KStar), KStar),
        "* -> nat -> *" -> KArrow(KStar, KArrow(KNat, KStar)),
        "(nat -> *) -> *" -> KArrow(KArrow(KNat, KStar), KStar),
      )

      forEvery(testCases)((stringToParse, expectedKind) =>
        parseKind(stringToParse) shouldBe Right(expectedKind))
    }

    "does not parse keywords alone" in {
      forEvery(keywords)(
        parseKind(_) shouldBe an[Left[String, Type]]
      )
    }
  }

  "type parser" should {
    "parses properly BuiltinType" in {

      val testCases = Table[String, BuiltinType](
        "string to parse" -> "expected builtin type",
        "Int64" -> BTInt64,
        "Numeric" -> BTNumeric,
        "Text" -> BTText,
        "Timestamp" -> BTTimestamp,
        "Party" -> BTParty,
        "Unit" -> BTUnit,
        "Bool" -> BTBool,
        "List" -> BTList,
        "Update" -> BTUpdate,
        "Scenario" -> BTScenario,
        "Date" -> BTDate,
        "ContractId" -> BTContractId,
        "Arrow" -> BTArrow,
        "Option" -> BTOptional,
        "TextMap" -> BTTextMap,
      )

      forEvery(testCases)((stringToParse, expectedBuiltinType) =>
        parseType(stringToParse) shouldBe Right(TBuiltin(expectedBuiltinType)))
    }

    "parses properly type constructor" in {
      val testCases = Table[String, TypeConName](
        "string to parse" -> "expected type constructor",
        "Mod:T" -> T.tycon,
        "'-pkgId-':Mod:T" -> T.tycon,
        "A.B:C.D" -> Identifier(
          defaultPackageId,
          QualifiedName(
            DottedName.assertFromSegments(ImmArray("A", "B").toSeq),
            DottedName.assertFromSegments(ImmArray("C", "D").toSeq)))
      )

      forEvery(testCases)((stringToParse, expectedTypeConstructor) =>
        parseType(stringToParse) shouldBe Right(TTyCon(expectedTypeConstructor)))
    }

    "parses properly types" in {
      val testCases = Table[String, Type](
        "string to parse" -> "expected type",
        "a" -> α,
        "a b" -> TApp(α, β),
        "3" -> TNat(3),
        "a 3" -> TApp(α, TNat(3)),
        "Mod:T a b" -> TApp(TApp(T, α), β),
        "a -> b" -> TApp(TApp(TBuiltin(BTArrow), α), β),
        "a -> b -> a" -> TApp(TApp(TBuiltin(BTArrow), α), TApp(TApp(TBuiltin(BTArrow), β), α)),
        "forall (a: *). Mod:T a" -> TForall((α.name, KStar), TApp(T, α)),
        "<f1: a, f2: Bool, f3:Mod:T>" -> TStruct(
          ImmArray[(FieldName, Type)](n"f1" -> α, n"f2" -> TBuiltin(BTBool), n"f3" -> T))
      )

      forEvery(testCases)((stringToParse, expectedType) =>
        parseType(stringToParse) shouldBe Right(expectedType))
    }

    "does not parse keywords alone" in {
      forEvery(keywords)(stringToParse => parseType(stringToParse) shouldBe an[Left[String, Type]])
    }
  }

  "expression parser" should {

    "parses properly primitiveCons" in {
      val testCases = Table[String, PrimCon](
        "string to parse" -> "expected primitive constructor",
        "()" -> PCUnit,
        "False" -> PCFalse,
        "True" -> PCTrue
      )

      forEvery(testCases)((stringToParse, expectedCons) =>
        parseExpr(stringToParse) shouldBe Right(EPrimCon(expectedCons)))
    }

    "parses properly literal" in {

      val testCases = Table[String, PrimLit](
        "string to parse" -> "expected literal",
        "1" -> PLInt64(1),
        "-2" -> PLInt64(-2),
        "1.0000000000" -> PLNumeric(Numeric.assertFromBigDecimal(10, BigDecimal.ONE)),
        "1.0" -> PLNumeric(Numeric.assertFromBigDecimal(1, BigDecimal.ONE)),
        "-10.00" -> PLNumeric(Numeric.assertFromBigDecimal(2, BigDecimal.TEN.negate)),
        """"some text"""" -> PLText("some text"),
        """ " \n\r\"\\ " """ -> PLText(" \n\r\"\\ "),
        """ "français" """ -> PLText("français"),
        "1970-01-02" -> PLDate(Time.Date.assertFromDaysSinceEpoch(1)),
        "1970-01-01T00:00:00.000001Z" -> PLTimestamp(Time.Timestamp.assertFromLong(1)),
        "1970-01-01T00:00:01Z" -> PLTimestamp(Time.Timestamp.assertFromLong(1000000)),
        "'party'" -> PLParty(Party.assertFromString("party")),
        """ ' aB0-_ ' """ -> PLParty(Party.assertFromString(" aB0-_ ")),
      )

      forEvery(testCases)((stringToParse, expectedCons) =>
        parseExpr(stringToParse) shouldBe Right(EPrimLit(expectedCons)))
    }

    "reject literal that do not map a valid value" in {
      val testCases = Table[String](
        "string to parsed",
        "9223372036854775808",
        "-9223372036854775809",
        "10000000000000000000000000000.0000000000",
        "-100000000000000000000000000000000000000.",
        "0000-01-01",
        "2100-02-29",
        "2019-13-28",
        "2019-13-28T00:00:00.000000Z",
        "1970-01-01T25:00:00.000000Z",
        "1970-01-01T00:61:00.000000Z",
        """ "\a" """,
        """ '\a' """,
        """ 'français' """
      )

      forEvery(testCases)(
        parseExpr(_) shouldBe an[Left[String, Expr]]
      )
    }

    "parses properly builtin functions" in {
      val testCases = Table[String, BuiltinFunction](
        "string to parse" -> "builtin",
        "TRACE" -> BTrace,
        "ADD_NUMERIC" -> BAddNumeric,
        "SUB_NUMERIC" -> BSubNumeric,
        "MUL_NUMERIC" -> BMulNumeric,
        "DIV_NUMERIC" -> BDivNumeric,
        "ROUND_NUMERIC" -> BRoundNumeric,
        "CAST_NUMERIC" -> BCastNumeric,
        "SHIFT_NUMERIC" -> BShiftNumeric,
        "ADD_INT64" -> BAddInt64,
        "SUB_INT64" -> BSubInt64,
        "MUL_INT64" -> BMulInt64,
        "DIV_INT64" -> BDivInt64,
        "MOD_INT64" -> BModInt64,
        "EXP_INT64" -> BExpInt64,
        "INT64_TO_NUMERIC" -> BInt64ToNumeric,
        "NUMERIC_TO_INT64" -> BNumericToInt64,
        "DATE_TO_UNIX_DAYS" -> BDateToUnixDays,
        "UNIX_DAYS_TO_DATE" -> BUnixDaysToDate,
        "TIMESTAMP_TO_UNIX_MICROSECONDS" -> BTimestampToUnixMicroseconds,
        "UNIX_MICROSECONDS_TO_TIMESTAMP" -> BUnixMicrosecondsToTimestamp,
        "FOLDL" -> BFoldl,
        "FOLDR" -> BFoldr,
        "EXPLODE_TEXT" -> BExplodeText,
        "IMPLODE_TEXT" -> BImplodeText,
        "APPEND_TEXT" -> BAppendText,
        "TO_TEXT_INT64" -> BToTextInt64,
        "TO_TEXT_NUMERIC" -> BToTextNumeric,
        "TO_TEXT_TEXT" -> BToTextText,
        "TO_TEXT_TIMESTAMP" -> BToTextTimestamp,
        "TO_TEXT_PARTY" -> BToTextParty,
        "TO_TEXT_DATE" -> BToTextDate,
        "ERROR" -> BError,
        "LESS_INT64" -> BLessInt64,
        "LESS_NUMERIC" -> BLessNumeric,
        "LESS_TEXT" -> BLessText,
        "LESS_TIMESTAMP" -> BLessTimestamp,
        "LESS_DATE" -> BLessDate,
        "LESS_EQ_INT64" -> BLessEqInt64,
        "LESS_EQ_NUMERIC" -> BLessEqNumeric,
        "LESS_EQ_TEXT" -> BLessEqText,
        "LESS_EQ_TIMESTAMP" -> BLessEqTimestamp,
        "LESS_EQ_DATE" -> BLessEqDate,
        "GREATER_INT64" -> BGreaterInt64,
        "GREATER_NUMERIC" -> BGreaterNumeric,
        "GREATER_TEXT" -> BGreaterText,
        "GREATER_TIMESTAMP" -> BGreaterTimestamp,
        "GREATER_DATE" -> BGreaterDate,
        "GREATER_EQ_INT64" -> BGreaterEqInt64,
        "GREATER_EQ_NUMERIC" -> BGreaterEqNumeric,
        "GREATER_EQ_TEXT" -> BGreaterEqText,
        "GREATER_EQ_TIMESTAMP" -> BGreaterEqTimestamp,
        "GREATER_EQ_DATE" -> BGreaterEqDate,
        "EQUAL_NUMERIC" -> BEqualNumeric,
        "EQUAL_LIST" -> BEqualList,
        "EQUAL_CONTRACT_ID" -> BEqualContractId,
        "EQUAL" -> BEqual,
        "COERCE_CONTRACT_ID" -> BCoerceContractId,
      )

      forEvery(testCases)((stringToParse, expectedBuiltin) =>
        parseExpr(stringToParse) shouldBe Right(EBuiltin(expectedBuiltin)))
    }

    "parses properly expressions " in {
      val testCases = Table[String, Expr](
        "string to parse" ->
          "expected expression",
        "x" ->
          x,
        "Mod:v" ->
          v,
        "Mod:R {}" ->
          ERecCon(TypeConApp(R.tycon, ImmArray.empty), ImmArray.empty),
        "Mod:R @Int64 @Bool {f1 = 1, f2 = False}" ->
          ERecCon(RIntBool, ImmArray(n"f1" -> e"1", n"f2" -> e"False")),
        "Mod:R @Int64 @Bool {f1} x" ->
          ERecProj(RIntBool, n"f1", e"x"),
        "Mod:R @Int64 @Bool {x with f1 = 1}" ->
          ERecUpd(RIntBool, n"f1", e"x", e"1"),
        "Mod:R:V @Int64 @Bool 1" ->
          EVariantCon(RIntBool, n"V", e"1"),
        "Mod:R:C" ->
          EEnumCon(R.tycon, n"C"),
        "< f1 =2, f2=False >" ->
          EStructCon(ImmArray(n"f1" -> e"2", n"f2" -> e"False")),
        "(x).f1" ->
          EStructProj(n"f1", e"x"),
        "x y" ->
          EApp(e"x", e"y"),
        "x y z" ->
          EApp(EApp(e"x", e"y"), e"z"),
        "x (y z)" ->
          EApp(e"x", EApp(e"y", e"z")),
        "x @Int64" ->
          ETyApp(e"x", t"Int64"),
        "x @Int64 @Bool" ->
          ETyApp(ETyApp(e"x", t"Int64"), t"Bool"),
        """\ (x:Int64) -> x""" ->
          EAbs((x.value, t"Int64"), e"x", None),
        """\ (x:Int64) (y:Bool) -> <f1=x, f2=y>""" -> EAbs(
          (x.value, t"Int64"),
          e"""\ (y:Bool) -> <f1=x, f2=y>""",
          None),
        """/\ (a:*). x @a""" ->
          ETyAbs(n"a" -> KStar, e"x @a"),
        "Nil @a" ->
          ENil(TVar(n"a")),
        "Cons @a [e1, e2] tail" ->
          ECons(TVar(n"a"), ImmArray(EVar(n"e1"), EVar(n"e2")), EVar(n"tail")),
        "None @a" ->
          ENone(TVar(n"a")),
        "Some @a e" ->
          ESome(TVar(n"a"), EVar(n"e")),
        "let x:Int64 = 2 in x" ->
          ELet(Binding(Some(x.value), t"Int64", e"2"), e"x"),
        "case e of () -> ()" ->
          ECase(e"e", ImmArray(CaseAlt(CPPrimCon(PCUnit), e"()"))),
        "case e of True -> False" ->
          ECase(e"e", ImmArray(CaseAlt(CPPrimCon(PCTrue), e"False"))),
        "case e of False -> True" ->
          ECase(e"e", ImmArray(CaseAlt(CPPrimCon(PCFalse), e"True"))),
        "case e of Nil -> True" ->
          ECase(e"e", ImmArray(CaseAlt(CPNil, e"True"))),
        "case e of Cons h t -> Mod:f h t" ->
          ECase(e"e", ImmArray(CaseAlt(CPCons(n"h", n"t"), e"Mod:f h t"))),
        "case e of None -> ()" ->
          ECase(e"e", ImmArray(CaseAlt(CPNone, e"()"))),
        "case e of Some x -> x" ->
          ECase(e"e", ImmArray(CaseAlt(CPSome(n"x"), e"x"))),
        "case e of Mod:T:V x -> x " ->
          ECase(e"e", ImmArray(CaseAlt(CPVariant(T.tycon, n"V", n"x"), e"x"))),
        "case e of True -> False | False -> True" ->
          ECase(
            e"e",
            ImmArray(CaseAlt(CPPrimCon(PCTrue), e"False"), CaseAlt(CPPrimCon(PCFalse), e"True"))),
      )

      forEvery(testCases)((stringToParse, expectedExp) =>
        parseExpr(stringToParse) shouldBe Right(expectedExp))
    }

    "parses properly scenarios" in {
      val testCases = Table[String, Scenario](
        "string to parse" ->
          "expected scenario",
        "spure @tau e" ->
          ScenarioPure(t"tau", e"e"),
        "sbind x: tau <- e in f x" ->
          ScenarioBlock(ImmArray(Binding(Some(n"x"), t"tau", e"e")), e"f x"),
        "sbind x: tau <- e1 ; y: sigma <- e2 in f x y" ->
          ScenarioBlock(
            ImmArray(Binding(Some(n"x"), t"tau", e"e1"), Binding(Some(n"y"), t"sigma", e"e2")),
            e"f x y"),
        "commit @tau party body" ->
          ScenarioCommit(e"party", e"body", t"tau"),
        "must_fail_at @tau party update" ->
          ScenarioMustFailAt(e"party", e"update", t"tau"),
        "pass e" ->
          ScenarioPass(e"e"),
        "sget_time" ->
          ScenarioGetTime,
        "sget_party party" ->
          ScenarioGetParty(e"party"),
        "sembed_expr @tau e" -> ScenarioEmbedExpr(t"tau", e"e")
      )

      forEvery(testCases)((stringToParse, expectedScenario) =>
        parseExpr(stringToParse) shouldBe Right(EScenario(expectedScenario)))
    }

    "parses update properly" in {
      val testCases = Table[String, Update](
        "string to parse" ->
          "expected update statement",
        "upure @tau e" ->
          UpdatePure(t"tau", e"e"),
        "ubind x: tau <- e in f x" ->
          UpdateBlock(ImmArray(Binding(Some(n"x"), t"tau", e"e")), e"f x"),
        "ubind x: tau <- e1; y: sigma <- e2 in f x y" ->
          UpdateBlock(
            ImmArray(Binding(Some(n"x"), t"tau", e"e1"), Binding(Some(n"y"), t"sigma", e"e2")),
            e"f x y"),
        "create @Mod:T e" ->
          UpdateCreate(T.tycon, e"e"),
        "fetch @Mod:T e" ->
          UpdateFetch(T.tycon, e"e"),
        "exercise @Mod:T Choice cid arg" ->
          UpdateExercise(T.tycon, n"Choice", e"cid", None, e"arg"),
        "exercise_with_actors @Mod:T Choice cid actor arg" ->
          UpdateExercise(T.tycon, n"Choice", e"cid", Some(e"actor"), e"arg"),
        "fetch_by_key @Mod:T e" ->
          UpdateFetchByKey(RetrieveByKey(T.tycon, e"e")),
        "lookup_by_key @Mod:T e" ->
          UpdateLookupByKey(RetrieveByKey(T.tycon, e"e")),
        "uget_time" ->
          UpdateGetTime,
        "uembed_expr @tau e" ->
          UpdateEmbedExpr(t"tau", e"e"),
      )

      forEvery(testCases)((stringToParse, expectedUpdate) =>
        parseExpr(stringToParse) shouldBe Right(EUpdate(expectedUpdate)))
    }

    "does not parse keywords alone" in {
      forEvery(keywords)(
        parseExpr(_) shouldBe an[Left[String, Type]]
      )
    }
  }

  "program parser" should {

    "parses variant/record/enum definitions" in {

      val p =
        """
          module Mod {

            variant Tree (a : * ) = Leaf : Unit | Node : Mod:Tree.Node a ;
            record Tree.Node (a: *) = { value: a, left : Mod:Tree a, right : Mod:Tree a };
            enum Color = Red | Green | Blue;

          }
        """.stripMargin

      val varDef = DDataType(
        false,
        ImmArray(n"a" -> KStar),
        DataVariant(ImmArray(n"Leaf" -> t"Unit", n"Node" -> t"Mod:Tree.Node a"))
      )
      val recDef = DDataType(
        false,
        ImmArray(n"a" -> KStar),
        DataRecord(
          ImmArray(n"value" -> t"a", n"left" -> t"Mod:Tree a", n"right" -> t"Mod:Tree a"),
          None)
      )
      val enumDef = DDataType(
        false,
        ImmArray.empty,
        DataEnum(ImmArray(n"Red", n"Green", n"Blue"))
      )

      parseModules(p) shouldBe Right(
        List(Module(
          name = modName,
          definitions = List(
            DottedName.assertFromSegments(ImmArray("Tree", "Node").toSeq) -> recDef,
            DottedName.assertFromSegments(ImmArray("Tree").toSeq) -> varDef,
            DottedName.assertFromSegments(ImmArray("Color").toSeq) -> enumDef
          ),
          templates = List.empty,
          languageVersion = defaultLanguageVersion,
          featureFlags = FeatureFlags.default
        )))

    }

    "parse value definitions" in {

      val p =
        """
         module Mod {

           val @noPartyLiterals fact : Int64 -> Int64 = \(x: Int64) -> ERROR @INT64 "not implemented";

         }
        """

      val valDef =
        DValue(t"Int64 -> Int64", true, e"""\(x: Int64) -> ERROR @INT64 "not implemented"""", false)

      parseModules(p) shouldBe Right(
        List(Module(
          name = modName,
          definitions = List(DottedName.assertFromString("fact") -> valDef),
          templates = List.empty,
          languageVersion = defaultLanguageVersion,
          featureFlags = FeatureFlags.default
        )))

    }

    "parse template definitions" in {

      val p =
        """
        module Mod {

          record Person = { person: Party, name: Text } ;

          template (this : Person) =  {
            precondition True,
            signatories Cons @Party [person] (Nil @Party),
            observers Cons @Party ['Alice'] (Nil @Party),
            agreement "Agreement",
            choices {
              choice Sleep : Unit by Cons @Party [person] (Nil @Party) to upure @Unit (),
              choice @nonConsuming Nap (i : Int64) : Int64 by Cons @Party [person] (Nil @Party) to upure @Int64 i
            },
            key @Party (Mod:Person {name} this) (\ (p: Party) -> p)
          } ;
        }
      """

      val template =
        Template(
          param = n"this",
          precond = e"True",
          signatories = e"Cons @Party [person] (Nil @Party)",
          agreementText = e""" "Agreement" """,
          choices = Map(
            n"Sleep" -> TemplateChoice(
              name = n"Sleep",
              consuming = true,
              controllers = e"Cons @Party [person] (Nil @Party)",
              selfBinder = n"this",
              argBinder = None -> TBuiltin(BTUnit),
              returnType = t"Unit",
              update = e"upure @Unit ()"
            ),
            n"Nap" -> TemplateChoice(
              name = n"Nap",
              consuming = false,
              controllers = e"Cons @Party [person] (Nil @Party)",
              selfBinder = n"this",
              argBinder = Some(n"i") -> TBuiltin(BTInt64),
              returnType = t"Int64",
              update = e"upure @Int64 i"
            )
          ),
          observers = e"Cons @Party ['Alice'] (Nil @Party)",
          key = Some(TemplateKey(t"Party", e"(Mod:Person {name} this)", e"""\ (p: Party) -> p"""))
        )

      val recDef = DDataType(
        false,
        ImmArray.empty,
        DataRecord(ImmArray(n"person" -> t"Party", n"name" -> t"Text"), Some(template))
      )
      parseModules(p) shouldBe Right(
        List(Module(
          name = modName,
          definitions = List(DottedName.assertFromString("Person") -> recDef),
          templates = List.empty,
          languageVersion = defaultLanguageVersion,
          featureFlags = FeatureFlags.default
        )))

    }

    "parses template without key" in {

      val p =
        """
          module Mod {

            record R = { } ;

            template (this : R) =  {
              precondition True,
              signatories Nil @Unit,
              observers Nil @Unit,
              agreement "Agreement",
              choices { }
            } ;
          }
        """

      val template =
        Template(
          param = n"this",
          precond = e"True",
          signatories = e"Nil @Unit",
          agreementText = e""" "Agreement" """,
          choices = Map.empty,
          observers = e"Nil @Unit",
          key = None
        )

      val recDef = DDataType(
        false,
        ImmArray.empty,
        DataRecord(ImmArray.empty, Some(template))
      )
      parseModules(p) shouldBe Right(
        List(Module(
          name = modName,
          definitions = List(DottedName.assertFromString("R") -> recDef),
          templates = List.empty,
          languageVersion = defaultLanguageVersion,
          featureFlags = FeatureFlags.default
        )))

    }
  }

  private val keywords = Table(
    "forall",
    "let",
    "in",
    "with",
    "case",
    "of",
    "sbind",
    "ubind",
    "create",
    "fetch",
    "exercise",
    "by",
    "to",
  )

  private val modName = DottedName.assertFromString("Mod")

  private def qualify(s: String) =
    Identifier(defaultPackageId, QualifiedName(modName, DottedName.assertFromString(s)))

  private val T: TTyCon = TTyCon(qualify("T"))
  private val R: TTyCon = TTyCon(qualify("R"))
  private val RIntBool = TypeConApp(R.tycon, ImmArray(t"Int64", t"Bool"))
  private val α: TVar = TVar(n"a")
  private val β: TVar = TVar(n"b")

  private val x = EVar(n"x")
  private val v = EVal(qualify("v"))
}
