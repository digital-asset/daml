// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.{ImmArray, Ref, Struct}
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SError.SError
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.value.Value.ContractId
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ComparisonSBuiltinTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import SpeedyTestLib.loggingContext

  import com.daml.lf.testing.parser.Implicits.{defaultParserParameters => _, _}

  private[this] val pkgId1 = Ref.PackageId.assertFromString("-packageId1-")
  private[this] val pkgId2 = Ref.PackageId.assertFromString("-packageId2-")

  val parserParameters1: ParserParameters[this.type] =
    com.daml.lf.testing.parser.Implicits.defaultParserParameters.copy(
      defaultPackageId = pkgId1
    )

  val parserParameters2: ParserParameters[this.type] =
    com.daml.lf.testing.parser.Implicits.defaultParserParameters.copy(
      defaultPackageId = pkgId2
    )

  private[this] val pkg1 = {
    implicit def parserParameters: ParserParameters[this.type] = parserParameters1

    p"""
        module Mod {
          variant Either a b = Left : a | Right : b ;
          record MyUnit = { };
          record Box a = { x: a };
          record Tuple a b = { fst: a, snd: b };
          enum Color = Red | Green | Blue;

          record @serializable Template = { };

          template (this : Template) =  {
             precondition True;
             signatories (Nil @Party);
             observers (Nil @Party);
             agreement "Agreement for Mod:Template";
          };
        }

        module A {
          enum A = ;
          enum B = ;
        }

        module B {
          enum A = ;
          enum B = ;
        }


    """
  }

  private[this] val pkg2 = {

    implicit def parserParameters: ParserParameters[this.type] = parserParameters2

    p"""
         module A {
          enum A = ;
          enum B = ;
        }

    """
  }

  private[this] val builtins: TableFor2[Ast.BuiltinFunction, Int => Boolean] = Table(
    "builtin" -> "result",
    Ast.BLess -> (_ < 0),
    Ast.BLessEq -> (_ <= 0),
    Ast.BEqual -> (_ == 0),
    Ast.BGreaterEq -> (_ >= 0),
    Ast.BGreater -> (_ > 0),
  )

  "generic comparison builtins" should {

    implicit def parserParameters: ParserParameters[this.type] = parserParameters1

    "respects specified order" in {

      val comparableValues: TableFor2[Ast.Type, List[Ast.Expr]] =
        Table(
          "type" -> "comparable values in order",
          // Atomic values
          t"Unit" -> List(e"()"),
          t"Bool" -> List(e"False", e"True"),
          t"Int64" -> List(e"-3", e"0", e"1"),
          t"Decimal" -> List(e"-10000.0000000000", e"0.0000000000", e"10000.0000000000"),
          t"Numeric 0" -> List(e"-10000.", e"0.", e"10000."),
          t"Text" ->
            // Note that in UTF8  "｡" < "😂" but in UTF16 "｡" > "😂"
            List("a bit of text", "some other text", "some other text again", "｡", "😂")
              .map(t => Ast.EPrimLit(Ast.PLText(t))),
          t"Date" -> List(e"1969-07-21", e"1970-01-01", e"2020-02-02"),
          t"Timestamp" -> List(
            e"1969-07-21T02:56:15.000000Z",
            e"1970-01-01T00:00:00.000000Z",
            e"2020-02-02T20:20:02.020000Z",
          ),
          // Parties cannot be built from expressions.
          // We map at runtime the `party1`, `party2` and, `party3` two 3 party IDS in increasing order.
          t"Party" -> List(e"party1", e"party2", e"party3"),
          t"Mod:Color" -> List(e"Mod:Color:Red", e"Mod:Color:Green", e"Mod:Color:Blue"),
          t"Mod:MyUnit" -> List(e"Mod:MyUnit {}"),
          // Contract IDs cannot be built from expressions.
          // We map at runtime the variables `cid1`, `cid2` and, `cid3` two 3 contract IDs in increasing order.
          t"ContractId Mod:Template" -> List(e"cid1", e"cid2", e"cid3"),
          /// Type Representations
          t"Mod:TypRep" ->
            List(
              e"type_rep @Unit",
              e"type_rep @Bool",
              e"type_rep @Int64",
              e"type_rep @Text",
              e"type_rep @Timestamp",
              e"type_rep @Party",
              e"type_rep @Date",
              e"type_rep @Any",
              e"type_rep @TypeRep",
              e"type_rep @Mod:Template",
            ),
          t"Mod:TypRep" ->
            List(
              e"type_rep @(List Mod:Template)",
              e"type_rep @(Update Mod:Template)",
              e"type_rep @(Scenario Mod:Template)",
              e"type_rep @(ContractId Mod:Template)",
              e"type_rep @(Option Mod:Template)",
              e"type_rep @(TextMap Mod:Template)",
              e"type_rep @(Numeric 0)",
              e"type_rep @(Mod:Box Mod:Template)",
            ),
          t"Mod:TypRep" ->
            List(
              e"type_rep @(Arrow Mod:Template  Mod:Template)",
              e"type_rep @(GenMap Mod:Template  Mod:Template)",
              e"type_rep @(Mod:Tuple Mod:Template  Mod:Template)",
            ),
          t"Mod:TypeRep" ->
            List(
              e"type_rep @(Numeric 0)",
              e"type_rep @(Numeric 10)",
              e"type_rep @(Numeric 37)",
            ),
          t"Mod:TypeRep" ->
            List(
              e"type_rep @'-packageId1-':A:A",
              e"type_rep @'-packageId1-':A:B",
              e"type_rep @'-packageId1-':B:A",
              e"type_rep @'-packageId1-':B:B",
              e"type_rep @'-packageId2-':A:A",
              e"type_rep @'-packageId2-':A:B",
            ),
          t"Mod:TypeRep" ->
            List(
              e"type_rep @<field0: Unit>",
              e"type_rep @<field0: Int64>",
              e"type_rep @<field1: Unit>",
              e"type_rep @<field1: Unit, field2: Unit>",
              e"type_rep @<field1: Unit, field2: Int64>",
              e"type_rep @<field1: Int64, field2: Unit>",
              e"type_rep @<field1: Int64, field2: Int64>",
              e"type_rep @<field1: Unit, field3: Unit>",
            ),
          t"Mod:TypeRep" ->
            List(
              e"type_rep @(List Unit)",
              e"type_rep @(List Int64)",
              e"type_rep @(Option Unit)",
              e"type_rep @(Option Int64)",
            ),
          t"Mod:TypeRep" ->
            List(
              e"type_rep @Unit",
              e"type_rep @Party",
              e"type_rep @a:a",
              e"type_rep @b:b",
              e"type_rep @5",
              e"type_rep @20",
              e"type_rep @<field: Unit>",
              e"type_rep @(Option Unit)",
              e"type_rep @(Arrow Unit Unit)",
            ),
          // 1 level nested values
          t"Mod:Tuple Int64 Int64" ->
            List(
              e"Mod:Tuple @Int64 @Int64 {fst = 0, snd = 0}",
              e"Mod:Tuple @Int64 @Int64 {fst = 0, snd = 1}",
              e"Mod:Tuple @Int64 @Int64 {fst = 1, snd = 0}",
              e"Mod:Tuple @Int64 @Int64 {fst = 1, snd = 1}",
            ),
          t"Mod:Either Text Int64" -> List(
            e"""Mod:Either:Left @Text @Int64 "a" """,
            e"""Mod:Either:Left @Text @Int64 "b" """,
            e"""Mod:Either:Right @Text @Int64 -1 """,
            e"""Mod:Either:Right @Text @Int64 1 """,
          ),
          t"<fst: Int64, snd: Int64>" ->
            List(
              e"<fst = 0, snd = 0>",
              e"<fst = 0, snd = 1>",
              e"<fst = 1, snd = 0>",
              e"<fst = 1, snd = 1>",
            ),
          t"Option Text" ->
            List(
              e""" None @Text """,
              e""" Some @Text "A" """,
              e""" Some @Text "B" """,
            ),
          t"List Int64" ->
            List(
              e"Nil @Int64",
              e"Cons @Int64 [0] (Nil @Int64)",
              e"Cons @Int64 [0,0] (Nil @Int64)",
              e"Cons @Int64 [0,0,1] (Nil @Int64)",
              e"Cons @Int64 [0,1,1] (Nil @Int64)",
              e"Cons @Int64 [1] (Nil @Int64)",
              e"Cons @Int64 [1,0] (Nil @Int64)",
              e"Cons @Int64 [1,1] (Nil @Int64)",
            ),
          t"TextMap Int64" ->
            List(
              e"TEXTMAP_EMPTY @Int64",
              e"""TEXTMAP_INSERT @Int64 "a" 1 (TEXTMAP_EMPTY @Int64)""",
              e"""TEXTMAP_INSERT @Int64 "a" 1 (TEXTMAP_INSERT @Int64 "b" 1 (TEXTMAP_EMPTY @Int64))""",
              e"""TEXTMAP_INSERT @Int64 "a" 1 (TEXTMAP_INSERT @Int64 "b" 2 (TEXTMAP_EMPTY @Int64))""",
              e"""TEXTMAP_INSERT @Int64 "a" 2 (TEXTMAP_EMPTY @Int64)""",
              e"""TEXTMAP_INSERT @Int64 "a" 2 (TEXTMAP_INSERT @Int64 "b" 1 (TEXTMAP_EMPTY @Int64))""",
              e"""TEXTMAP_INSERT @Int64 "a" 2 (TEXTMAP_INSERT @Int64 "b" 2 (TEXTMAP_EMPTY @Int64))""",
              e"""TEXTMAP_INSERT @Int64 "a" 2 (TEXTMAP_INSERT @Int64 "b" 2 (TEXTMAP_INSERT @Int64 "c" 3 (TEXTMAP_EMPTY @Int64)))""",
              e"""TEXTMAP_INSERT @Int64 "b" 1 (TEXTMAP_EMPTY @Int64)""",
            ),
          t"GenMap Text Int64" ->
            List(
              e"GENMAP_EMPTY @Text @Int64",
              e"""GENMAP_INSERT @Text @Int64 "a" 1 (GENMAP_EMPTY @Text @Int64)""",
              e"""GENMAP_INSERT @Text @Int64 "a" 1 (GENMAP_INSERT @Text @Int64"b" 1 (GENMAP_EMPTY @Text @Int64))""",
              e"""GENMAP_INSERT @Text @Int64 "a" 1 (GENMAP_INSERT @Text @Int64"b" 2 (GENMAP_EMPTY @Text @Int64))""",
              e"""GENMAP_INSERT @Text @Int64 "a" 2 (GENMAP_EMPTY @Text @Int64)""",
              e"""GENMAP_INSERT @Text @Int64 "a" 2 (GENMAP_INSERT @Text @Int64 "b" 1 (GENMAP_EMPTY @Text @Int64))""",
              e"""GENMAP_INSERT @Text @Int64 "a" 2 (GENMAP_INSERT @Text @Int64 "b" 2 (GENMAP_EMPTY @Text @Int64))""",
              e"""GENMAP_INSERT @Text @Int64 "a" 2 (GENMAP_INSERT @Text @Int64 "b" 2 (GENMAP_INSERT @Text @Int64 "c" 3 (GENMAP_EMPTY @Text @Int64)))""",
              e"""GENMAP_INSERT @Text @Int64 "b" 1 (GENMAP_EMPTY @Text @Int64)""",
            ),
          // 2 level nested values
          t"Mod:Tuple (Option Text) (Option Int64)" ->
            List(
              e"""Mod:Tuple @Int64 @Int64 {fst = None @Text, snd = None @Int64}""",
              e"""Mod:Tuple @Int64 @Int64 {fst = None @Text, snd = Some @Int64 0}""",
              e"""Mod:Tuple @Int64 @Int64 {fst = None @Text, snd = Some @Int64 1}""",
              e"""Mod:Tuple @Int64 @Int64 {fst = Some @Text "a", snd = None @Int64}""",
              e"""Mod:Tuple @Int64 @Int64 {fst = Some @Text "a", snd = Some @Int64 0}""",
              e"""Mod:Tuple @Int64 @Int64 {fst = Some @Text "a", snd = Some @Int64 1}""",
              e"""Mod:Tuple @Int64 @Int64 {fst = Some @Text "b", snd = None @Int64}""",
              e"""Mod:Tuple @Int64 @Int64 {fst = Some @Text "b", snd = Some @Int64 0}""",
              e"""Mod:Tuple @Int64 @Int64 {fst = Some @Text "b", snd = Some @Int64 1}""",
            ),
          t"Mod:Either (Option Int64) (Option Int64)" -> List(
            e"""Mod:Either:Left @(Option Text) @(Option Int64) None @Text""",
            e"""Mod:Either:Left @(Option Text) @(Option Int64) Some @Text "a" """,
            e"""Mod:Either:Left @(Option Text) @(Option Int64) Some @Text "b" """,
            e"""Mod:Either:Right @(Option Int64) @(Option Int64) None @Int64 """,
            e"""Mod:Either:Right @(Option Int64) @(Option Int64) Some @Int64 42 """,
            e"""Mod:Either:Right @(Option Int64) @(Option Int64) Some @Int64 43 """,
          ),
          t"<fst: Option Text, snd: Option Int64>" ->
            List(
              e"""<fst = None @Text, snd = None @Int64>""",
              e"""<fst = None @Text, snd = Some @Int64 0>""",
              e"""<fst = None @Text, snd = Some @Int64 1>""",
              e"""<fst = Some @Text "a", snd = None @Int64>""",
              e"""<fst = Some @Text "a", snd = Some @Int64 0>""",
              e"""<fst = Some @Text "a", snd = Some @Int64 1>""",
              e"""<fst = Some @Text "b", snd = None @Int64>""",
              e"""<fst = Some @Text "b", snd = Some @Int64 0>""",
              e"""<fst = Some @Text "b", snd = Some @Int64 1>""",
            ),
          t"Option (Option Int64)" ->
            List(
              e"None @(Option Int64)",
              e"Some @(Option Int64) (None @Int64)",
              e"Some @(Option Int64) (Some @Int64 0)",
              e"Some @(Option Int64) (Some @Int64 1)",
            ),
          t"List (Option Int64)" -> List(
            e"Nil @(Option Int64)",
            e"Cons @(Option Int64) [None @Int64] (Nil @(Option Int64))",
            e"Cons @(Option Int64) [None @Int64, None @Int64] (Nil @(Option Int64))",
            e"Cons @(Option Int64) [None @Int64, None @Int64, Some @Int64 0] (Nil @(Option Int64))",
            e"Cons @(Option Int64) [None @Int64, Some @Int64 0, Some @Int64 0] (Nil @(Option Int64))",
            e"Cons @(Option Int64) [None @Int64, Some @Int64 1, Some @Int64 0] (Nil @(Option Int64))",
            e"Cons @(Option Int64) [Some @Int64 0] (Nil @(Option Int64))",
            e"Cons @(Option Int64) [Some @Int64 0, None @Int64] (Nil @(Option Int64))",
            e"Cons @(Option Int64) [Some @Int64 0, Some @Int64 0] (Nil @(Option Int64))",
            e"Cons @(Option Int64) [Some @Int64 1, Some @Int64 0] (Nil @(Option Int64))",
          ),
          t"TextMap (Option Int64)" ->
            List(
              e"TEXTMAP_EMPTY @(Option Int64)",
              e"""TEXTMAP_INSERT @(Option Int64) "a" (None @Int64) (TEXTMAP_EMPTY @(Option Int64))""",
              e"""TEXTMAP_INSERT @(Option Int64) "a" (None @Int64) (TEXTMAP_INSERT @(Option Int64)"b" (None @Int64) (TEXTMAP_EMPTY @(Option Int64)))""",
              e"""TEXTMAP_INSERT @(Option Int64) "a" (None @Int64) (TEXTMAP_INSERT @(Option Int64)"b" (Some @Int64 -10) (TEXTMAP_EMPTY @(Option Int64)))""",
              e"""TEXTMAP_INSERT @(Option Int64) "a" (Some @Int64 0) (TEXTMAP_EMPTY @(Option Int64))""",
              e"""TEXTMAP_INSERT @(Option Int64) "a" (Some @Int64 0) (TEXTMAP_INSERT @(Option Int64) "b" (None @Int64) (TEXTMAP_EMPTY @(Option Int64)))""",
              e"""TEXTMAP_INSERT @(Option Int64) "a" (Some @Int64 0) (TEXTMAP_INSERT @(Option Int64) "b" (Some @Int64 -10) (TEXTMAP_EMPTY @(Option Int64)))""",
              e"""TEXTMAP_INSERT @(Option Int64) "a" (Some @Int64 0) (TEXTMAP_INSERT @(Option Int64) "b" (Some @Int64 -10) (TEXTMAP_INSERT @(Option Int64) "c" (None @Int64) (TEXTMAP_EMPTY @(Option Int64))))""",
              e"""TEXTMAP_INSERT @(Option Int64) "a" (Some @Int64 0) (TEXTMAP_INSERT @(Option Int64) "b" (Some @Int64 -1) (TEXTMAP_EMPTY @(Option Int64)))""",
              e"""TEXTMAP_INSERT @(Option Int64) "a" (Some @Int64 1) (TEXTMAP_EMPTY @(Option Int64))""",
            ),
          t"GenMap Text Int64" ->
            List(
              e"GENMAP_EMPTY @(Option Text) @(Option Int64)",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (None @Int64) (GENMAP_EMPTY @(Option Text) @(Option Int64))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (None @Int64) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (None @Int64) (GENMAP_EMPTY @(Option Text) @(Option Int64)))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (None @Int64) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (Some @Int64 0) (GENMAP_EMPTY @(Option Text) @(Option Int64)))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (Some @Int64 0) (GENMAP_EMPTY @(Option Text) @(Option Int64))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (Some @Int64 0) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (None @Int64) (GENMAP_EMPTY @(Option Text) @(Option Int64)))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (Some @Int64 0) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (Some @Int64 0) (GENMAP_EMPTY @(Option Text) @(Option Int64)))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (Some @Int64 0) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (Some @Int64 0) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "1") (Some @Int64 2) (GENMAP_EMPTY @(Option Text) @(Option Int64))))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (Some @Int64 1) (GENMAP_EMPTY @(Option Text) @(Option Int64))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (Some @Int64 1) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (None @Int64) (GENMAP_EMPTY @(Option Text) @(Option Int64)))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (Some @Int64 1) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (Some @Int64 0) (GENMAP_EMPTY @(Option Text) @(Option Int64)))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (None @Text) (Some @Int64 1) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (Some @Int64 0) (GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "1") (Some @Int64 2) (GENMAP_EMPTY @(Option Text) @(Option Int64))))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (None @Int64) (GENMAP_EMPTY @(Option Text) @(Option Int64))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "0") (Some @Int64 0) (GENMAP_EMPTY @(Option Text) @(Option Int64))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "1") (Some @Int64 0) (GENMAP_EMPTY @(Option Text) @(Option Int64))""",
              e"""GENMAP_INSERT @(Option Text) @(Option Int64) (Some @Text "1") (Some @Int64 1)  (GENMAP_EMPTY @(Option Text) @(Option Int64))""",
            ),
          // any
          t"Any" -> List(
            e"to_any @Int64 -1",
            e"to_any @Int64 0",
            e"to_any @Int64 1",
            e"to_any @Any (to_any @Int64 1)",
            e"to_any @Any (to_any @Int64 2)",
            e"to_any @(Option Int64) (None @Int64)",
            e"to_any @(Option Int64) (Some @Int64 0)",
            e"to_any @(Option Int64) (Some @Int64 1)",
          ),
        )

      forEvery(comparableValues) { case (typ, expr) =>
        for {
          xi <- expr.zipWithIndex
          (x, i) = xi
          yj <- expr.zipWithIndex
          (y, j) = yj
        } {
          val diff = i compare j
          forEvery(builtins) { case (bi, result) =>
            eval(bi, typ, x, y) shouldBe Right(SValue.SBool(result(diff)))
          }
        }
      }
    }

    "should not distinguish struct built in different order" in {

      val typ = t"""<first: Int64, second: Int64, third: Int64>"""

      val values = Table(
        "expr",
        e"""<first = 1, second = 2, third = 3>""",
        e"""<third = 3, second = 2, first = 1>""",
        e"""<first = 1, third = 3, second = 2>""",
      )

      forEvery(builtins) { case (bi, result) =>
        val expectedResult = Right(SValue.SBool(result(0)))
        forEvery(values) { x =>
          forEvery(values) { y =>
            eval(bi, typ, x, y) shouldBe expectedResult
            eval(bi, typ, y, x) shouldBe expectedResult
          }
        }
      }

    }

    "fail when comparing non-comparable values" in {
      import language.Ast._
      import language.Util._

      val funT = t"(Int64 -> Int64)"
      val fun1 = e"(ADD_INT64 1)"
      val fun2 = e"""(\(x: Int64) -> 42)"""

      def tApps(t: Type, args: Type*) =
        args.foldLeft(t)(TApp)

      def eApps(e: Expr, args: Expr*) =
        args.foldLeft(e)(EApp)

      def etApps(e: Expr, types: Type*) =
        types.foldLeft(e)(ETyApp)

      def text(s: String) = EPrimLit(PLText(s))

      val eitherT = t"Mod:Either"
      val TTyCon(eitherTyCon) = eitherT
      val leftName = Ref.Name.assertFromString("Left")
      val rightName = Ref.Name.assertFromString("Right")

      def left(leftT: Type, rightT: Type)(l: Expr) =
        EVariantCon(TypeConApp(eitherTyCon, ImmArray(leftT, rightT)), rightName, l)

      def right(leftT: Type, rightT: Type)(r: Expr) =
        EVariantCon(TypeConApp(eitherTyCon, ImmArray(leftT, rightT)), leftName, r)

      val tupleT = t"Mod:Tuple"
      val TTyCon(tupleTyCon) = tupleT
      val fstName = Ref.Name.assertFromString("fst")
      val sndName = Ref.Name.assertFromString("snd")

      def tupleR(fstT: Type, sndT: Type)(fst: Expr, snd: Expr) =
        ERecCon(
          TypeConApp(tupleTyCon, ImmArray(fstT, sndT)),
          ImmArray(fstName -> fst, sndName -> snd),
        )

      def list(t: Type)(es: Expr*) =
        if (es.isEmpty) ENil(t) else ECons(t, es.to(ImmArray), ENil(t))

      def textMap(T: Type)(entries: (String, Expr)*) =
        entries.foldRight(etApps(EBuiltin(BTextMapEmpty), T)) { case ((key, value), acc) =>
          eApps(etApps(EBuiltin(BTextMapInsert), T), text(key), value, acc)
        }

      def genMap(kT: Type, vT: Type)(entries: (Expr, Expr)*) =
        entries.foldRight(etApps(EBuiltin(BGenMapEmpty), kT, vT)) { case ((key, value), acc) =>
          eApps(etApps(EBuiltin(BGenMapInsert), kT, vT), key, value, acc)
        }

      def tupleS(fst: Expr, snd: Expr) =
        EStructCon(ImmArray(fstName -> fst, sndName -> snd))

      val T = tApps(eitherT, funT, TInt64)
      val X1 = left(funT, TInt64)(e"1")
      val X2 = left(funT, TInt64)(e"2")
      val X3 = left(funT, TInt64)(e"3")
      val U = right(funT, TInt64)(fun1)

      case class Test(
          typ: Type,
          negativeTestCases: List[(Expr, Expr)],
          positiveTestCases: List[(Expr, Expr)],
      )

      val tests = Table(
        ("test cases"),
        Test(
          typ = funT,
          negativeTestCases = List.empty,
          positiveTestCases = List(fun1 -> fun1, fun1 -> fun2),
        ),
        Test(
          typ = T,
          negativeTestCases = List(X1 -> U, U -> X1),
          positiveTestCases = List(U -> U),
        ),
        Test(
          typ = tApps(tupleT, T, T),
          negativeTestCases = List(
            tupleR(T, T)(X1, U) -> tupleR(T, T)(X2, U)
          ),
          positiveTestCases = List(
            tupleR(T, T)(U, X1) -> tupleR(T, T)(U, X1),
            tupleR(T, T)(U, X1) -> tupleR(T, T)(U, X2),
            tupleR(T, T)(X1, U) -> tupleR(T, T)(X1, U),
          ),
        ),
        Test(
          typ = TList(T),
          negativeTestCases = List(
            list(T)(X1, U) -> list(T)(X2, U),
            list(T)(U) -> list(T)(),
            list(T)() -> list(T)(U),
          ),
          positiveTestCases = List(
            list(T)(U) -> list(T)(U),
            list(T)(U, X1) -> list(T)(U),
            list(T)(X1, U) -> list(T)(X1, U),
            list(T)(X1, U, X1) -> list(T)(X1, U),
          ),
        ),
        Test(
          typ = TOptional(T),
          negativeTestCases = List(
            ENone(T) -> ESome(T, U),
            ESome(T, X1) -> ESome(T, U),
          ),
          positiveTestCases = List(
            ESome(T, U) -> ESome(T, U)
          ),
        ),
        Test(
          typ = TTextMap(T),
          negativeTestCases = List(
            textMap(T)("a" -> X1, "b" -> U) ->
              textMap(T)("a" -> X2, "b" -> U),
            textMap(T)("a" -> X1) ->
              textMap(T)("b" -> U),
            textMap(T)("a" -> X1, "b" -> U) ->
              textMap(T)("a" -> X1, "c" -> U),
            textMap(T)("a" -> X1, "b" -> U) ->
              textMap(T)("b" -> U),
            textMap(T)("a" -> X1, "b" -> U) ->
              textMap(T)("b" -> U),
            textMap(T)("a" -> X1, "b" -> U) ->
              textMap(T)("b" -> X1, "c" -> U),
            textMap(T)("a" -> U) ->
              textMap(T)(),
          ),
          positiveTestCases = List(
            textMap(T)("a" -> U) ->
              textMap(T)("a" -> U),
            textMap(T)("a" -> U, "b" -> X1) ->
              textMap(T)("a" -> U),
            textMap(T)("a" -> U, "b" -> X1) ->
              textMap(T)("a" -> U, "c" -> X1),
            textMap(T)("a" -> U, "b" -> X1) ->
              textMap(T)("a" -> U, "b" -> X2),
            textMap(T)("a" -> X1, "b" -> U) ->
              textMap(T)("a" -> X1, "b" -> U),
          ),
        ),
        Test(
          typ = TGenMap(T, T),
          negativeTestCases = List(
            genMap(T, T)(X1 -> X1, X2 -> U) ->
              genMap(T, T)(X1 -> X2, X2 -> U),
            genMap(T, T)(X1 -> X1) ->
              genMap(T, T)(X2 -> U),
            genMap(T, T)(X1 -> X1, X2 -> U) ->
              genMap(T, T)(X1 -> X1, X3 -> U),
            genMap(T, T)(X1 -> X1, X2 -> U) ->
              genMap(T, T)(X2 -> U),
            genMap(T, T)(X1 -> X1, X2 -> U) ->
              genMap(T, T)(X2 -> U),
            genMap(T, T)(X1 -> X1, X2 -> U) ->
              genMap(T, T)(X2 -> X1, X3 -> U),
            genMap(T, T)(X1 -> U) ->
              genMap(T, T)(),
          ),
          positiveTestCases = List(
            genMap(T, T)(X1 -> U) ->
              genMap(T, T)(X1 -> U),
            genMap(T, T)(X1 -> U, X2 -> X1) ->
              genMap(T, T)(X1 -> U),
            genMap(T, T)(X1 -> U, X2 -> X1) ->
              genMap(T, T)(X1 -> U, X3 -> X1),
            genMap(T, T)(X1 -> U, X2 -> X1) ->
              genMap(T, T)(X1 -> U, X2 -> X2),
            genMap(T, T)(X1 -> X1, X2 -> U) ->
              genMap(T, T)(X1 -> X1, X2 -> U),
          ),
        ),
        Test(
          typ = TStruct(Struct.assertFromSeq(List(fstName -> T, sndName -> T))),
          negativeTestCases = List(
            tupleS(X1, U) -> tupleS(X2, U)
          ),
          positiveTestCases = List(
            tupleS(U, X1) -> tupleS(U, X1),
            tupleS(U, X1) -> tupleS(U, X2),
            tupleS(X1, U) -> tupleS(X1, U),
          ),
        ),
        Test(
          typ = TAny,
          negativeTestCases = List(
            EToAny(tApps(eitherT, funT, TText), right(funT, TText)(fun1)) ->
              EToAny(tApps(eitherT, funT, TInt64), right(funT, TInt64)(fun1))
          ),
          positiveTestCases = List(
            EToAny(T, U) -> EToAny(T, U)
          ),
        ),
      )

      forEvery(tests) { case Test(typ, negativeTestCases, positiveTestCases) =>
        negativeTestCases.foreach { case (x, y) =>
          forEvery(builtins) { case (bi, _) =>
            eval(bi, typ, x, y) shouldBe a[Right[_, _]]
            if (x != y) eval(bi, typ, y, x) shouldBe a[Right[_, _]]
          }
        }

        positiveTestCases.foreach { case (x, y) =>
          forEvery(builtins) { case (bi, _) =>
            eval(bi, typ, x, y) shouldBe a[Left[_, _]]
            if (x != y) eval(bi, typ, y, x) shouldBe a[Left[_, _]]
          }
        }

      }

    }

  }

  private[this] val compiledPackages =
    PureCompiledPackages.assertBuild(Map(pkgId1 -> pkg1, pkgId2 -> pkg2))

  private[this] val cidBinderType = {
    implicit def parserParameters: ParserParameters[this.type] = parserParameters1
    t"ContractId Mod:Template"
  }

  private[this] val partyBinderType = {
    implicit def parserParameters: ParserParameters[this.type] = parserParameters1
    t"Party"
  }

  private[this] val cidBinder1 = Ref.Name.assertFromString("cid1") -> cidBinderType
  private[this] val cidBinder2 = Ref.Name.assertFromString("cid2") -> cidBinderType
  private[this] val cidBinder3 = Ref.Name.assertFromString("cid3") -> cidBinderType

  private[this] val partyBinder1 = Ref.Name.assertFromString("party1") -> partyBinderType
  private[this] val partyBinder2 = Ref.Name.assertFromString("party2") -> partyBinderType
  private[this] val partyBinder3 = Ref.Name.assertFromString("party3") -> partyBinderType

  private[this] val contractIds =
    Seq(
      ContractId.V1.assertFromString("00" * 32 + "0000"),
      ContractId.V1.assertFromString("00" * 32 + "0001"),
      ContractId.V1.assertFromString("00" + "ff" * 32),
    ).map(cid => SValue.SContractId(cid): SValue)

  private[this] val parties =
    Seq(
      Ref.Party.assertFromString("alice"),
      Ref.Party.assertFromString("bob"),
      Ref.Party.assertFromString("carol"),
    ).map(p => SValue.SParty(p): SValue)

  private[this] def eval(bi: Ast.BuiltinFunction, t: Ast.Type, x: Ast.Expr, y: Ast.Expr) = {
    final case class Goodbye(e: SError) extends RuntimeException("", null, false, false)
    val sexpr = compiledPackages.compiler.unsafeCompile(
      Ast.EAbs(
        partyBinder1,
        Ast.EAbs(
          partyBinder2,
          Ast.EAbs(
            partyBinder3,
            Ast.EAbs(
              cidBinder1,
              Ast.EAbs(
                cidBinder2,
                Ast.EAbs(
                  cidBinder3,
                  Ast.EApp(Ast.EApp(Ast.ETyApp(Ast.EBuiltin(bi), t), x), y),
                  None,
                ),
                None,
              ),
              None,
            ),
            None,
          ),
          None,
        ),
        None,
      )
    )
    Speedy.Machine.runPureSExpr(
      compiledPackages,
      SEApp(sexpr, (parties ++ contractIds).toArray),
    )
  }

}
