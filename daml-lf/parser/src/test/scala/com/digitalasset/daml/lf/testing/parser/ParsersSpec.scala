// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

import java.math.BigDecimal
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Numeric, Struct, Time}
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.language.Util._
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.immutable.VectorMap
import scala.language.implicitConversions

class ParsersSpecV1 extends ParsersSpec(LanguageMajorVersion.V1)
class ParsersSpecV2 extends ParsersSpec(LanguageMajorVersion.V2)

class ParsersSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with ScalaCheckPropertyChecks
    with Matchers {

  implicit val parserParameters =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)
  val defaultPackageId = parserParameters.defaultPackageId

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
        parseKind(stringToParse) shouldBe Right(expectedKind)
      )
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
        "BigNumeric" -> BTBigNumeric,
        "RoundingMode" -> BTRoundingMode,
        "AnyException" -> BTAnyException,
        "TypeRep" -> BTTypeRep,
      )

      forEvery(testCases)((stringToParse, expectedBuiltinType) =>
        parseType(stringToParse) shouldBe Right(TBuiltin(expectedBuiltinType))
      )
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
            DottedName.assertFromSegments(ImmArray("C", "D").toSeq),
          ),
        ),
      )

      forEvery(testCases)((stringToParse, expectedTypeConstructor) =>
        parseType(stringToParse) shouldBe Right(TTyCon(expectedTypeConstructor))
      )
    }

    "parses properly types" in {
      val testCases = Table[String, Type](
        "string to parse" -> "expected type",
        "a" -> α,
        "$alpha$" -> TVar(n"$$alpha$$"),
        "a b" -> TApp(α, β),
        "3" -> TNat(3),
        "a 3" -> TApp(α, TNat(3)),
        "Mod:T a b" -> TApp(TApp(T, α), β),
        "a -> b" -> TApp(TApp(TBuiltin(BTArrow), α), β),
        "a -> b -> a" -> TApp(TApp(TBuiltin(BTArrow), α), TApp(TApp(TBuiltin(BTArrow), β), α)),
        "forall (a: *). Mod:T a" -> TForall((α.name, KStar), TApp(T, α)),
        "<f1: a, f2: Bool, f3:Mod:T>" ->
          TStruct(Struct.assertFromSeq(List(n"f1" -> α, n"f2" -> TBuiltin(BTBool), n"f3" -> T))),
      )

      forEvery(testCases)((stringToParse, expectedType) =>
        parseType(stringToParse) shouldBe Right(expectedType)
      )
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
        "True" -> PCTrue,
      )

      forEvery(testCases)((stringToParse, expectedCons) =>
        parseExpr(stringToParse) shouldBe Right(EPrimCon(expectedCons))
      )
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
        "ROUNDING_UP" -> PLRoundingMode(java.math.RoundingMode.UP),
      )

      forEvery(testCases)((stringToParse, expectedCons) =>
        parseExpr(stringToParse) shouldBe Right(EPrimLit(expectedCons))
      )
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
        """ 'français' """,
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
        "MUL_NUMERIC_LEGACY" -> BMulNumericLegacy,
        "MUL_NUMERIC" -> BMulNumeric,
        "DIV_NUMERIC_LEGACY" -> BDivNumericLegacy,
        "DIV_NUMERIC" -> BDivNumeric,
        "ROUND_NUMERIC" -> BRoundNumeric,
        "CAST_NUMERIC_LEGACY" -> BCastNumericLegacy,
        "CAST_NUMERIC" -> BCastNumeric,
        "SHIFT_NUMERIC_LEGACY" -> BShiftNumericLegacy,
        "SHIFT_NUMERIC" -> BShiftNumeric,
        "ADD_INT64" -> BAddInt64,
        "SUB_INT64" -> BSubInt64,
        "MUL_INT64" -> BMulInt64,
        "DIV_INT64" -> BDivInt64,
        "MOD_INT64" -> BModInt64,
        "EXP_INT64" -> BExpInt64,
        "INT64_TO_NUMERIC_LEGACY" -> BInt64ToNumericLegacy,
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
        "INT64_TO_TEXT" -> BInt64ToText,
        "NUMERIC_TO_TEXT" -> BNumericToText,
        "TEXT_TO_TEXT" -> BTextToText,
        "TIMESTAMP_TO_TEXT" -> BTimestampToText,
        "PARTY_TO_TEXT" -> BPartyToText,
        "DATE_TO_TEXT" -> BDateToText,
        "ERROR" -> BError,
        "LESS_NUMERIC" -> BLessNumeric,
        "LESS_EQ_NUMERIC" -> BLessEqNumeric,
        "GREATER_NUMERIC" -> BGreaterNumeric,
        "GREATER_EQ_NUMERIC" -> BGreaterEqNumeric,
        "EQUAL_NUMERIC" -> BEqualNumeric,
        "EQUAL_LIST" -> BEqualList,
        "EQUAL_CONTRACT_ID" -> BEqualContractId,
        "EQUAL" -> BEqual,
        "LESS" -> BLess,
        "LESS_EQ" -> BLessEq,
        "GREATER" -> BGreater,
        "GREATER_EQ" -> BGreaterEq,
        "COERCE_CONTRACT_ID" -> BCoerceContractId,
        "ANY_EXCEPTION_MESSAGE" -> BAnyExceptionMessage,
        "TYPE_REP_TYCON_NAME" -> BTypeRepTyConName,
      )

      forEvery(testCases)((stringToParse, expectedBuiltin) =>
        parseExpr(stringToParse) shouldBe Right(EBuiltin(expectedBuiltin))
      )
    }

    "parses properly expressions " in {
      val testCases = Table[String, Expr](
        "string to parse" ->
          "expected expression",
        "x" ->
          x,
        "Mod:v" ->
          v,
        "'-pkgId-':Mod:v" ->
          v,
        "Mod:R {}" ->
          ERecCon(TypeConApp(R.tycon, ImmArray.Empty), ImmArray.Empty),
        "Mod:R @Int64 @Bool {f1 = 1, f2 = False}" ->
          ERecCon(RIntBool, ImmArray(n"f1" -> e"1", n"f2" -> e"False")),
        "'-pkgId-':Mod:R @Int64 @Bool {f1 = 1, f2 = False}" ->
          ERecCon(RIntBool, ImmArray(n"f1" -> e"1", n"f2" -> e"False")),
        "Mod:R @Int64 @Bool {f1} x" ->
          ERecProj(RIntBool, n"f1", e"x"),
        "'-pkgId-':Mod:R @Int64 @Bool {f1} x" ->
          ERecProj(RIntBool, n"f1", e"x"),
        "Mod:R @Int64 @Bool {x with f1 = 1}" ->
          ERecUpd(RIntBool, n"f1", e"x", e"1"),
        "'-pkgId-':Mod:R @Int64 @Bool {x with f1 = 1}" ->
          ERecUpd(RIntBool, n"f1", e"x", e"1"),
        "Mod:R:V @Int64 @Bool 1" ->
          EVariantCon(RIntBool, n"V", e"1"),
        "'-pkgId-':Mod:R:V @Int64 @Bool 1" ->
          EVariantCon(RIntBool, n"V", e"1"),
        "Mod:R:C" ->
          EEnumCon(R.tycon, n"C"),
        "'-pkgId-':Mod:R:C" ->
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
          None,
        ),
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
        "let _:Int64 = 2 in 3" ->
          ELet(Binding(None, t"Int64", e"2"), e"3"),
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
            ImmArray(CaseAlt(CPPrimCon(PCTrue), e"False"), CaseAlt(CPPrimCon(PCFalse), e"True")),
          ),
        "to_any_exception @Mod:E exception" ->
          EToAnyException(E, e"exception"),
        "from_any_exception @Mod:E anyException" ->
          EFromAnyException(E, e"anyException"),
        "throw @Unit @Mod:E exception" ->
          EThrow(TUnit, E, e"exception"),
        "call_method @Mod:I method body" ->
          ECallInterface(I.tycon, n"method", e"body"),
        "call_method @'-pkgId-':Mod:I method body" ->
          ECallInterface(I.tycon, n"method", e"body"),
        "to_interface @Mod:T @Mod:I body" ->
          EToInterface(T.tycon, I.tycon, e"body"),
        "to_interface @'-pkgId-':Mod:T @'-pkgId-':Mod:I body" ->
          EToInterface(T.tycon, I.tycon, e"body"),
        "from_interface @Mod:T @Mod:I body" ->
          EFromInterface(T.tycon, I.tycon, e"body"),
        "from_interface @'-pkgId-':Mod:T @'-pkgId-':Mod:I body" ->
          EFromInterface(T.tycon, I.tycon, e"body"),
        "unsafe_from_interface @Mod:T @Mod:I cid body" ->
          EUnsafeFromInterface(T.tycon, I.tycon, e"cid", e"body"),
        "unsafe_from_interface @'-pkgId-':Mod:T @'-pkgId-':Mod:I cid body" ->
          EUnsafeFromInterface(T.tycon, I.tycon, e"cid", e"body"),
        "to_required_interface @Mod:T @Mod:I body" ->
          EToRequiredInterface(T.tycon, I.tycon, e"body"),
        "to_required_interface @'-pkgId-':Mod:T @'-pkgId-':Mod:I body" ->
          EToRequiredInterface(T.tycon, I.tycon, e"body"),
        "from_required_interface @Mod:T @Mod:I body" ->
          EFromRequiredInterface(T.tycon, I.tycon, e"body"),
        "from_required_interface @'-pkgId-':Mod:T @'-pkgId-':Mod:I body" ->
          EFromRequiredInterface(T.tycon, I.tycon, e"body"),
        "unsafe_from_required_interface @Mod:T @Mod:I cid body" ->
          EUnsafeFromRequiredInterface(T.tycon, I.tycon, e"cid", e"body"),
        "unsafe_from_required_interface @'-pkgId-':Mod:T @'-pkgId-':Mod:I cid body" ->
          EUnsafeFromRequiredInterface(T.tycon, I.tycon, e"cid", e"body"),
        "interface_template_type_rep @Mod:I body" ->
          EInterfaceTemplateTypeRep(I.tycon, e"body"),
        "interface_template_type_rep @'-pkgId-':Mod:I body" ->
          EInterfaceTemplateTypeRep(I.tycon, e"body"),
        "signatory_interface @Mod:I body" ->
          ESignatoryInterface(I.tycon, e"body"),
        "signatory_interface @'-pkgId-':Mod:I body" ->
          ESignatoryInterface(I.tycon, e"body"),
        "observer_interface @Mod:I body" ->
          EObserverInterface(I.tycon, e"body"),
        "observer_interface @'-pkgId-':Mod:I body" ->
          EObserverInterface(I.tycon, e"body"),
        "choice_controller @Mod:T ChoiceName contract choiceArg" ->
          EChoiceController(T.tycon, n"ChoiceName", e"contract", e"choiceArg"),
        "choice_controller @'-pkgId-':Mod:T ChoiceName contract choiceArg" ->
          EChoiceController(T.tycon, n"ChoiceName", e"contract", e"choiceArg"),
        "choice_observer @Mod:T ChoiceName contract choiceArg" ->
          EChoiceObserver(T.tycon, n"ChoiceName", e"contract", e"choiceArg"),
        "choice_observer @'-pkgId-':Mod:T ChoiceName contract choiceArg" ->
          EChoiceObserver(T.tycon, n"ChoiceName", e"contract", e"choiceArg"),
      )

      forEvery(testCases)((stringToParse, expectedExp) =>
        parseExpr(stringToParse) shouldBe Right(expectedExp)
      )
    }

    "parse properly valid enum expressions" in {
      val successfulTestCases = Table[String, Expr](
        "string to parse" ->
          "expected expression",
        "Mod:R:V @Int64 @Bool 1" ->
          EVariantCon(RIntBool, n"V", e"1"),
        "'-pkgId-':Mod:R:V @Int64 @Bool 1" ->
          EVariantCon(RIntBool, n"V", e"1"),
        "Mod:R:V 1" ->
          EVariantCon(TypeConApp(R.tycon, ImmArray.empty), n"V", e"1"),
        "Mod:R:C" ->
          EEnumCon(R.tycon, n"C"),
        "'-pkgId-':Mod:R:C" ->
          EEnumCon(R.tycon, n"C"),
      )

      forEvery(successfulTestCases) { (stringToParse, expectedExp) =>
        parseExpr(stringToParse) shouldBe Right(expectedExp)
      }
    }

    "fail to parse invalid enum expressions" in {
      val failingTestCases = Table[String](
        "string to parse",
        "Mod:R:C @Bool",
        "Mod:R:C @Bool @Int64",
        "'-pkgId-':Mod:R:C @Bool",
        "'-pkgId-':Mod:R:C @Bool @Int64",
      )

      forEvery(failingTestCases) { stringToParse =>
        parseExpr(stringToParse) shouldBe Left(
          "enum type does not take type parameters at line 1, column 1"
        )
      }
    }

    "parse properly rounding Mode" in {
      import java.math.RoundingMode._

      val testCases = Table(
        "string" -> "rounding mode",
        "ROUNDING_UP" -> UP,
        "ROUNDING_DOWN" -> DOWN,
        "ROUNDING_CEILING" -> CEILING,
        "ROUNDING_FLOOR" -> FLOOR,
        "ROUNDING_HALF_UP" -> HALF_UP,
        "ROUNDING_HALF_DOWN" -> HALF_DOWN,
        "ROUNDING_HALF_EVEN" -> HALF_EVEN,
        "ROUNDING_UNNECESSARY" -> UNNECESSARY,
      )

      forEvery(testCases)((stringToParse, expectedMode) =>
        parseExpr(stringToParse) shouldBe Right(EPrimLit(PLRoundingMode(expectedMode)))
      )

    }

    "parses properly experiment" in {
      parseExpr("experimental ANSWER (Unit -> Int64)") shouldBe Right(
        EExperimental("ANSWER", t"Unit -> Int64")
      )
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
            e"f x y",
          ),
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
        "sembed_expr @tau e" -> ScenarioEmbedExpr(t"tau", e"e"),
      )

      forEvery(testCases)((stringToParse, expectedScenario) =>
        parseExpr(stringToParse) shouldBe Right(EScenario(expectedScenario))
      )
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
            e"f x y",
          ),
        "create @Mod:T e" ->
          UpdateCreate(T.tycon, e"e"),
        "create_by_interface @Mod:I e" ->
          UpdateCreateInterface(I.tycon, e"e"),
        "fetch_template @Mod:T e" ->
          UpdateFetchTemplate(T.tycon, e"e"),
        "fetch_interface @Mod:I e" ->
          UpdateFetchInterface(I.tycon, e"e"),
        "exercise @Mod:T Choice cid arg" ->
          UpdateExercise(T.tycon, n"Choice", e"cid", e"arg"),
        "dynamic_exercise @Mod:T Choice cid arg" ->
          UpdateDynamicExercise(T.tycon, n"Choice", e"cid", e"arg"),
        "exercise_interface @Mod:I Choice cid arg" ->
          UpdateExerciseInterface(I.tycon, n"Choice", e"cid", e"arg", None),
        "exercise_interface_with_guard @Mod:I Choice cid arg guard" ->
          UpdateExerciseInterface(I.tycon, n"Choice", e"cid", e"arg", Some(e"guard")),
        "exercise_by_key @Mod:T Choice key arg" ->
          UpdateExerciseByKey(T.tycon, n"Choice", e"key", e"arg"),
        "fetch_by_key @Mod:T e" ->
          UpdateFetchByKey(RetrieveByKey(T.tycon, e"e")),
        "lookup_by_key @Mod:T e" ->
          UpdateLookupByKey(RetrieveByKey(T.tycon, e"e")),
        "uget_time" ->
          UpdateGetTime,
        "uembed_expr @tau e" ->
          UpdateEmbedExpr(t"tau", e"e"),
        "try @tau body catch err -> handler err" ->
          UpdateTryCatch(t"tau", e"body", n"err", e"handler err"),
      )

      forEvery(testCases)((stringToParse, expectedUpdate) =>
        parseExpr(stringToParse) shouldBe Right(EUpdate(expectedUpdate))
      )
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
        DataVariant(ImmArray(n"Leaf" -> t"Unit", n"Node" -> t"Mod:Tree.Node a")),
      )
      val recDef = DDataType(
        false,
        ImmArray(n"a" -> KStar),
        DataRecord(ImmArray(n"value" -> t"a", n"left" -> t"Mod:Tree a", n"right" -> t"Mod:Tree a")),
      )
      val enumDef = DDataType(
        false,
        ImmArray.Empty,
        DataEnum(ImmArray(n"Red", n"Green", n"Blue")),
      )

      parseModules(p) shouldBe Right(
        List(
          Module.build(
            name = modName,
            definitions = List(
              DottedName.assertFromSegments(ImmArray("Tree", "Node").toSeq) -> recDef,
              DottedName.assertFromSegments(ImmArray("Tree").toSeq) -> varDef,
              DottedName.assertFromSegments(ImmArray("Color").toSeq) -> enumDef,
            ),
            templates = List.empty,
            exceptions = List.empty,
            interfaces = List.empty,
            featureFlags = FeatureFlags.default,
          )
        )
      )

    }

    "parse value definitions" in {

      val p =
        """
         module Mod {

           val fact : Int64 -> Int64 = \(x: Int64) -> ERROR @INT64 "not implemented";

         }
        """

      val valDef =
        DValue(t"Int64 -> Int64", e"""\(x: Int64) -> ERROR @INT64 "not implemented"""", false)

      parseModules(p) shouldBe Right(
        List(
          Module(
            name = modName,
            definitions = Map(DottedName.assertFromString("fact") -> valDef),
            templates = Map.empty,
            exceptions = Map.empty,
            interfaces = Map.empty,
            featureFlags = FeatureFlags.default,
          )
        )
      )

    }

    "parse template definitions" in {

      val p =
        """
        module Mod {

          record @serializable Person = { person: Party, name: Text } ;

          template (this : Person) =  {
            precondition True;
            signatories Cons @Party [person] (Nil @Party);
            observers Cons @Party [Mod:Person {person} this] (Nil @Party);
            agreement "Agreement";
            choice Sleep (self) (u:Unit) : ContractId Mod:Person
              , controllers Cons @Party [person] (Nil @Party)
              to upure @(ContractId Mod:Person) self;
            choice @nonConsuming Nap (self) (i : Int64): Int64
              , controllers Cons @Party [person] (Nil @Party)
              , observers Nil @Party
              to upure @Int64 i;
            choice @nonConsuming PowerNap (self) (i : Int64): Int64
              , controllers Cons @Party [person] (Nil @Party)
              , observers Cons @Party [person] (Nil @Party)
              , authorizers Cons @Party [person] (Nil @Party)
              to upure @Int64 i;
            implements Mod1:Human {
              view = Mod1:HumanView { name = "Foo B. Baz" };
              method age = 42;
              method alive = True;
            };
            implements '-pkgId-':Mod2:Referenceable {
              view = Mod1:ReferenceableView { indirect = False };
              method uuid = "123e4567-e89b-12d3-a456-426614174000";
            };
            key @Party (Mod:Person {name} this) (\ (p: Party) -> p);
          } ;
        }
      """

      val TTyCon(human) = t"Mod1:Human"
      val TTyCon(referenceable) = t"Mod2:Referenceable"

      val template =
        Template(
          param = n"this",
          precond = e"True",
          signatories = e"Cons @Party [person] (Nil @Party)",
          agreementText = e""" "Agreement" """,
          choices = Map(
            n"Sleep" ->
              TemplateChoice(
                name = n"Sleep",
                consuming = true,
                controllers = e"Cons @Party [person] (Nil @Party)",
                choiceObservers = None,
                choiceAuthorizers = None,
                selfBinder = n"self",
                argBinder = n"u" -> TUnit,
                returnType = t"ContractId Mod:Person",
                update = e"upure @(ContractId Mod:Person) self",
              ),
            n"Nap" ->
              TemplateChoice(
                name = n"Nap",
                consuming = false,
                controllers = e"Cons @Party [person] (Nil @Party)",
                choiceObservers = Some(e"Nil @Party"),
                choiceAuthorizers = None,
                selfBinder = n"self",
                argBinder = n"i" -> TInt64,
                returnType = t"Int64",
                update = e"upure @Int64 i",
              ),
            n"PowerNap" ->
              TemplateChoice(
                name = n"PowerNap",
                consuming = false,
                controllers = e"Cons @Party [person] (Nil @Party)",
                choiceObservers = Some(e"Cons @Party [person] (Nil @Party)"),
                choiceAuthorizers = Some(e"Cons @Party [person] (Nil @Party)"),
                selfBinder = n"self",
                argBinder = n"i" -> TInt64,
                returnType = t"Int64",
                update = e"upure @Int64 i",
              ),
          ),
          observers = e"Cons @Party [Mod:Person {person} this] (Nil @Party)",
          key = Some(TemplateKey(t"Party", e"(Mod:Person {name} this)", e"""\ (p: Party) -> p""")),
          implements = VectorMap(
            human ->
              TemplateImplements(
                human,
                InterfaceInstanceBody(
                  Map(
                    n"age" -> InterfaceInstanceMethod(n"age", e"42"),
                    n"alive" -> InterfaceInstanceMethod(n"alive", e"True"),
                  ),
                  e"""Mod1:HumanView { name = "Foo B. Baz" }""",
                ),
              ),
            referenceable -> TemplateImplements(
              referenceable,
              InterfaceInstanceBody(
                Map(
                  n"uuid" -> InterfaceInstanceMethod(
                    n"uuid",
                    e""""123e4567-e89b-12d3-a456-426614174000"""",
                  )
                ),
                e"Mod1:ReferenceableView { indirect = False }",
              ),
            ),
          ),
        )

      val recDef = DDataType(
        true,
        ImmArray.Empty,
        DataRecord(ImmArray(n"person" -> t"Party", n"name" -> t"Text")),
      )
      val name = DottedName.assertFromString("Person")
      parseModules(p) shouldBe Right(
        List(
          Module(
            name = modName,
            definitions = Map(name -> recDef),
            templates = Map(name -> template),
            exceptions = Map.empty,
            interfaces = Map.empty,
            featureFlags = FeatureFlags.default,
          )
        )
      )

    }

    "parses template without key" in {

      val p =
        """
          module Mod {

            record @serializable R = { } ;

            template (this : R) =  {
              precondition True;
              signatories Nil @Unit;
              observers Nil @Unit;
              agreement "Agreement";
            } ;
          }
        """

      val template =
        Template.build(
          param = n"this",
          precond = e"True",
          signatories = e"Nil @Unit",
          agreementText = e""" "Agreement" """,
          choices = List.empty,
          observers = e"Nil @Unit",
          key = None,
          implements = List.empty,
        )

      val recDef = DDataType(
        true,
        ImmArray.Empty,
        DataRecord(ImmArray.Empty),
      )
      val name = DottedName.assertFromString("R")
      parseModules(p) shouldBe Right(
        List(
          Module(
            name = modName,
            definitions = Map(name -> recDef),
            templates = Map(name -> template),
            exceptions = Map.empty,
            interfaces = Map.empty,
            featureFlags = FeatureFlags.default,
          )
        )
      )

    }

    "parses exception definition" in {

      val p =
        """
          module Mod {

            record @serializable Exception = { message: Text } ;

            exception Exception = {
              message \(e: Mod:Exception) -> Mod:Exception {message} e
            } ;
          }
      """

      val recDef = DDataType(
        true,
        ImmArray.Empty,
        DataRecord(ImmArray(n"message" -> TText)),
      )
      val name = DottedName.assertFromString("Exception")
      val exception = DefException(e"""\(e: Mod:Exception) -> Mod:Exception {message} e""")
      parseModules(p) shouldBe Right(
        List(
          Module(
            name = modName,
            definitions = Map(name -> recDef),
            templates = Map.empty,
            exceptions = Map(name -> exception),
            interfaces = Map.empty,
            featureFlags = FeatureFlags.default,
          )
        )
      )
    }

    "parses interface definition" in {

      val p = """
       module Mod {
          interface (this: Person) = {
            viewtype Mod1:PersonView;
            method asParty: Party;
            method getName: Text;
            choice Sleep (self) (u:Unit) : ContractId Mod:Person
              , controllers Cons @Party [call_method @Mod:Person asParty this] (Nil @Party)
              to upure @(ContractId Mod:Person) self;
            choice @nonConsuming Nap (self) (i : Int64): Int64
              , controllers Cons @Party [call_method @Mod:Person asParty this] (Nil @Party)
              , observers Nil @Party
              to upure @Int64 i;
            coimplements Mod1:Company {
              view = Mod1:PersonView { name = callMethod @Mod:Person getName this };
              method asParty = Mod1:Company {party} this;
              method getName = Mod1:Company {legalName} this;
            };
          } ;
       }

      """
      val TTyCon(company) = t"Mod1:Company"

      val interface =
        DefInterface(
          requires = Set.empty,
          param = n"this",
          methods = Map(
            n"asParty" -> InterfaceMethod(n"asParty", t"Party"),
            n"getName" -> InterfaceMethod(n"getName", t"Text"),
          ),
          choices = Map(
            n"Sleep" -> TemplateChoice(
              name = n"Sleep",
              consuming = true,
              controllers = e"Cons @Party [call_method @Mod:Person asParty this] (Nil @Party)",
              choiceObservers = None,
              choiceAuthorizers = None,
              selfBinder = n"self",
              argBinder = n"u" -> TUnit,
              returnType = t"ContractId Mod:Person",
              update = e"upure @(ContractId Mod:Person) self",
            ),
            n"Nap" -> TemplateChoice(
              name = n"Nap",
              consuming = false,
              controllers = e"Cons @Party [call_method @Mod:Person asParty this] (Nil @Party)",
              choiceObservers = Some(e"Nil @Party"),
              choiceAuthorizers = None,
              selfBinder = n"self",
              argBinder = n"i" -> TInt64,
              returnType = t"Int64",
              update = e"upure @Int64 i",
            ),
          ),
          coImplements = Map(
            company ->
              InterfaceCoImplements(
                company,
                InterfaceInstanceBody(
                  Map(
                    n"asParty" -> InterfaceInstanceMethod(
                      n"asParty",
                      e"Mod1:Company {party} this",
                    ),
                    n"getName" -> InterfaceInstanceMethod(
                      n"getName",
                      e"Mod1:Company {legalName} this",
                    ),
                  ),
                  e"Mod1:PersonView { name = callMethod @Mod:Person getName this }",
                ),
              )
          ),
          view = t"Mod1:PersonView",
        )

      val person = DottedName.assertFromString("Person")
      parseModules(p) shouldBe Right(
        List(
          Module(
            name = modName,
            definitions = Map(person -> DDataType.Interface),
            templates = Map.empty,
            exceptions = Map.empty,
            interfaces = Map(person -> interface),
            featureFlags = FeatureFlags.default,
          )
        )
      )
    }

    "parses location annotations" in {
      e"loc(Mod, def, 0, 1, 2, 3) f" shouldEqual
        ELocation(
          Location(
            defaultPackageId,
            ModuleName.assertFromString("Mod"),
            "def",
            (0, 1),
            (2, 3),
          ),
          EVar(n"f"),
        )
    }

    "rejects bad location annotations" in {
      a[ParsingError] should be thrownBy e"loc(Mod, def, 0, 1, 2, 3) f g"
    }
  }

  private val keywords = Table(
    "Cons",
    "Nil",
    "Some",
    "None",
    "forall",
    "let",
    "in",
    "with",
    "case",
    "of",
    "to",
    "to_any",
    "from_any",
    "type_rep",
    "loc",
    "to_any_exception",
    "from_any_exception",
    "throw",
    "catch",
    "to_interface",
    "from_interface",
    "unsafe_from_interface",
    "to_required_interface",
    "from_required_interface",
    "unsafe_from_required_interface",
    "interface_template_type_rep",
    "signatory_interface",
    "observer_interface",
  )

  private val modName = DottedName.assertFromString("Mod")

  private def qualify(s: String) =
    Identifier(defaultPackageId, QualifiedName(modName, DottedName.assertFromString(s)))

  private val E: TTyCon = TTyCon(qualify("E"))
  private val I: TTyCon = TTyCon(qualify("I"))
  private val R: TTyCon = TTyCon(qualify("R"))
  private val RIntBool = TypeConApp(R.tycon, ImmArray(t"Int64", t"Bool"))
  private val T: TTyCon = TTyCon(qualify("T"))
  private val α: TVar = TVar(n"a")
  private val β: TVar = TVar(n"b")

  private val x = EVar(n"x")
  private val v = EVal(qualify("v"))
}
