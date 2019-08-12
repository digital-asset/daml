// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.interp.testing

import java.util

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.speedy.{SBuiltin, SResult, SValue, Speedy, TraceLog}
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.Compiler.CompileError
import com.digitalasset.daml.lf.speedy.SExpr.LfDefRef
import org.scalatest.{Matchers, WordSpec}
import com.digitalasset.daml.lf.testing.parser.Implicits._
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.language.implicitConversions

class InterpTest extends WordSpec with Matchers with TableDrivenPropertyChecks {

  private implicit def id(s: String): Ref.Name.T = Name.assertFromString(s)

  private def runExpr(e: Expr): SValue = {
    val machine = Speedy.Machine.fromExpr(e, true, PureCompiledPackages(Map.empty).right.get, false)
    while (!machine.isFinal) {
      machine.step match {
        case SResultContinue => ()
        case res => throw new RuntimeException(s"Got unexpected interpretation result $res")
      }
    }
    machine.toSValue
  }

  "evaluator behaves responsibly" should {
    // isolated rendition of the DA.Test.List.concat_test scenario in
    // stdlib, which failed after I introduced FrontQueue. It happened
    // to be a missing reverse in Interp.
    "concat works" in {
      val int64 = TBuiltin(BTInt64)
      val int64List = TApp(TBuiltin(BTList), int64)
      def int64Cons(nums: ImmArray[Long], tail: Expr): Expr =
        ECons(int64, nums.map(i => EPrimLit(PLInt64(i))), tail)
      val int64Nil = ENil(int64)
      val concat =
        EAbs(
          ("xss", TApp(TBuiltin(BTList), int64List)),
          ELet(
            Binding(
              Some("work"),
              TFun(int64List, TFun(int64List, int64List)),
              EAbs(
                ("xs", int64List),
                EAbs(
                  ("acc", int64List),
                  EApp(
                    EApp(
                      EApp(
                        EBuiltin(BFoldr),
                        EAbs(
                          ("x", int64),
                          EAbs(
                            ("accInner", int64List),
                            ECons(int64, ImmArray(EVar("x")), EVar("accInner")),
                            None),
                          None
                        )
                      ),
                      EVar("acc")
                    ),
                    EVar("xs")
                  ),
                  None
                ),
                None
              )
            ),
            EApp(EApp(EApp(EBuiltin(BFoldl), EVar("work")), ENil(int64)), EVar("xss"))
          ),
          None
        )
      val xss1 = ECons(
        int64List,
        ImmArray(int64Cons(ImmArray(2, 5), int64Nil), int64Cons(ImmArray[Long](7), int64Nil)),
        ENil(int64List))
      val xss2 = ECons(int64List, ImmArray(int64Cons(ImmArray(2, 5, 7), int64Nil)), ENil(int64List))
      runExpr(EApp(concat, xss1)) shouldBe runExpr(EApp(concat, xss2))
    }
  }

  "compilation and evaluation handle properly nat types" in {

    val testCases = Table(
      "input",
      e"""(/\ (n: nat). ROUND_NUMERIC @n 2) @4""",
      e"""(/\ (n: nat). /\ (n: nat). ROUND_NUMERIC @n 2) @3 @4 """,
      e"""(/\ (n: nat). /\ (n: nat). \(n: Int64) -> ROUND_NUMERIC @n n) @3 @4 2""",
      e"""(/\ (n: nat). \(n: Int64) -> /\ (n: nat). ROUND_NUMERIC @n n) @3 2 @4""",
      e"""(\(n: Int64) -> /\ (n: nat). /\ (n: nat). ROUND_NUMERIC @n n) 2 @3 @4""",
      e"""(\(n: Int64) -> /\ (n: *). /\ (n: nat). ROUND_NUMERIC @n n) 2 @Int64 @4""",
    )

    val expectedOutput =
      SValue.SPAP(
        SValue.PBuiltin(SBuiltin.SBRoundNumeric),
        ArrayList(SValue.STNat(4), SValue.SInt64(2)),
        3
      )

    forEvery(testCases)(input => runExpr(input) shouldBe expectedOutput)

    an[CompileError] shouldBe thrownBy(
      runExpr(e"""(/\ (n: nat). /\ (n: *). ROUND_NUMERIC @n n) @4 @Int64"""))
  }

  "large lists" should {
    val t_int64 = TBuiltin(BTInt64)
    val t_int64List = TApp(TBuiltin(BTList), t_int64)
    val list = ECons(
      t_int64List,
      ImmArray((1 to 100000).map(i => EPrimLit(PLInt64(i.toLong)))),
      ENil(t_int64List)
    )
    var machine: Speedy.Machine = null
    "compile" in {
      machine =
        Speedy.Machine.fromExpr(list, true, PureCompiledPackages(Map.empty).right.get, false)
    }
    "interpret" in {
      while (!machine.isFinal) {
        machine.step match {
          case SResultContinue => ()
          case res => throw new RuntimeException(s"Got unexpected interpretation result $res")
        }
      }
      machine.toSValue match {
        case SValue.SList(lst) =>
          lst.length shouldBe 100000
          val arr = lst.toImmArray
          arr(0) shouldBe SValue.SInt64(1)
          arr(99999) shouldBe SValue.SInt64(100000)
        case v => sys.error(s"unexpected resulting value $v")

      }
    }
  }

  "tracelog" should {
    "empty size" in {
      val log = TraceLog(10)
      log.iterator.hasNext shouldBe false
    }
    "half full" in {
      val log = TraceLog(2)
      log.add("test", None)
      val iter = log.iterator
      iter.hasNext shouldBe true
      iter.next shouldBe (("test", None))
      iter.hasNext shouldBe false
    }
    "overflow" in {
      val log = TraceLog(2)
      log.add("test1", None)
      log.add("test2", None)
      log.add("test3", None) // should replace "test1"
      val iter = log.iterator
      iter.hasNext shouldBe true
      iter.next shouldBe (("test2", None))
      iter.hasNext shouldBe true
      iter.next shouldBe (("test3", None))
      iter.hasNext shouldBe false
    }
  }

  /** Test that the package reloading works */
  "package reloading" should {
    val dummyPkg = PackageId.assertFromString("dummy")
    val ref = Identifier(dummyPkg, QualifiedName.assertFromString("Foo:bar"))
    val modName = DottedName.assertFromString("Foo")
    val pkgs1 = PureCompiledPackages(Map.empty).right.get
    val pkgs2 =
      PureCompiledPackages(
        Map(
          dummyPkg ->
            Package(
              List(
                Module(
                  modName,
                  Map(DottedName.assertFromString("bar") ->
                    DValue(TBuiltin(BTBool), true, ETrue, false)),
                  LanguageVersion.default,
                  FeatureFlags.default))))).right.get
    "succeeds" in {
      val machine = Speedy.Machine.fromExpr(
        EVal(ref),
        true,
        pkgs1,
        false
      )
      var result: SResult = SResultContinue
      def run() = {
        while (result == SResultContinue && !machine.isFinal) result = machine.step()
      }

      run()
      result match {
        case SResultMissingDefinition(ref2, cb) =>
          LfDefRef(ref) shouldBe ref2
          cb(pkgs2)
          result = SResultContinue
          run()
          machine.ctrl shouldBe Speedy.CtrlValue(SValue.SBool(true))
        case _ =>
          sys.error(s"expected result to be missing definition, got $result")
      }

    }

    "crashes without definition" in {
      val machine = Speedy.Machine.fromExpr(
        EVal(ref),
        true,
        pkgs1,
        false
      )
      var result: SResult = SResultContinue
      def run() = {
        while (result == SResultContinue && !machine.isFinal) result = machine.step()
      }
      run()
      result match {
        case SResultMissingDefinition(ref2, cb) =>
          LfDefRef(ref) shouldBe ref2
          result = SResultContinue
          try {
            cb(pkgs1)
            sys.error(s"expected crash when definition not provided")
          } catch {
            case _: SErrorCrash => ()
          }
        case _ =>
          sys.error(s"expected result to be missing definition, got $result")
      }

    }

    "tracks packages" in {
      val machine = Speedy.Machine.fromExpr(
        EVal(ref),
        true,
        pkgs1,
        false
      )
      var result: SResult = SResultContinue
      def run() = {
        while (result == SResultContinue && !machine.isFinal) result = machine.step()
      }

      run()

      machine.ptx.usedPackages shouldBe Set(dummyPkg)
    }

  }

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

}
