// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Numeric, Ref}
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.language.Util._
import com.daml.lf.speedy.SExpr.LfDefRef
import com.daml.lf.speedy.SResult._
import com.daml.lf.testing.parser.Implicits._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import com.daml.logging.ContextualizedLogger

class InterpreterTest extends AnyWordSpec with Inside with Matchers with TableDrivenPropertyChecks {

  import SpeedyTestLib.loggingContext

  private def runExpr(e: Expr): SValue = {
    val machine = Speedy.Machine.fromPureExpr(PureCompiledPackages.Empty, e)
    machine.run() match {
      case SResultFinal(v) => v
      case res => throw new RuntimeException(s"Got unexpected interpretation result $res")
    }
  }

  "compilation and evaluation handle properly nat types" in {

    def result(s: String) =
      SValue.SOptional(Some(SValue.SNumeric(Numeric.assertFromString(s))))

    val testCases = Table(
      "input" -> "output",
      e"""(/\ (n: nat). TEXT_TO_NUMERIC_LEGACY @n "0") @1""" ->
        result("0.0"),
      e"""(/\ (n: nat). /\ (n: nat). TEXT_TO_NUMERIC_LEGACY @n "1") @2 @3 """ ->
        result("1.000"),
      e"""(/\ (n: nat). /\ (n: nat). \(n: Text) -> TEXT_TO_NUMERIC_LEGACY @n n) @4 @5 "2"""" ->
        result("2.00000"),
      e"""(/\ (n: nat). \(n: Text) -> /\ (n: nat). TEXT_TO_NUMERIC_LEGACY @n n) @6 "3" @7""" ->
        result("3.0000000"),
      e"""(\(n: Text) -> /\ (n: nat). /\ (n: nat). TEXT_TO_NUMERIC_LEGACY @n n) "4" @8 @9""" ->
        result("4.000000000"),
      e"""(\(n: Text) -> /\ (n: *). /\ (n: nat). TEXT_TO_NUMERIC_LEGACY @n n) "5" @Text @10""" ->
        result("5.0000000000"),
    )

    forEvery(testCases) { (input, output) =>
      runExpr(input) shouldBe output
    }

    a[Compiler.CompilationError] shouldBe thrownBy(
      runExpr(e"""(/\ (n: nat). /\ (n: *). TEXT_TO_NUMERIC_LEGACY @n n) @4 @Text""")
    )
  }

  "large lists" should {
    val t_int64 = TBuiltin(BTInt64)
    val t_int64List = TApp(TBuiltin(BTList), t_int64)
    val list = ECons(
      t_int64List,
      (1 to 100000).map(i => EPrimLit(PLInt64(i.toLong))).to(ImmArray),
      ENil(t_int64List),
    )
    var machine: Speedy.PureMachine = null
    "compile" in {
      machine = Speedy.Machine.fromPureExpr(PureCompiledPackages.Empty, list)
    }
    "interpret" in {
      val value = machine.run() match {
        case SResultFinal(v) => v
        case res => throw new RuntimeException(s"Got unexpected interpretation result $res")
      }
      value match {
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
    val logger = ContextualizedLogger.get(getClass)
    "empty size" in {
      val log = new RingBufferTraceLog(logger, 10)
      log.iterator.hasNext shouldBe false
    }
    "half full" in {
      val log = new RingBufferTraceLog(logger, 2)
      log.add("test", None)
      val iter = log.iterator
      iter.hasNext shouldBe true
      iter.next() shouldBe (("test", None))
      iter.hasNext shouldBe false
    }
    "overflow" in {
      val log = new RingBufferTraceLog(logger, 2)
      log.add("test1", None)
      log.add("test2", None)
      log.add("test3", None) // should replace "test1"
      val iter = log.iterator
      iter.hasNext shouldBe true
      iter.next() shouldBe (("test2", None))
      iter.hasNext shouldBe true
      iter.next() shouldBe (("test3", None))
      iter.hasNext shouldBe false
    }
  }

  /** Test that the package reloading works */
  "package reloading" should {
    val dummyPkg = PackageId.assertFromString("dummy")
    val ref = Identifier(dummyPkg, QualifiedName.assertFromString("Foo:bar"))
    val modName = DottedName.assertFromString("Foo")
    val pkgs1 = PureCompiledPackages.Empty
    val pkgs2 =
      PureCompiledPackages.assertBuild(
        Map(
          dummyPkg ->
            Package.build(
              List(
                Module.build(
                  name = modName,
                  definitions = Map(
                    DottedName.assertFromString("bar") ->
                      DValue(TUpdate(TBool), EUpdate(UpdatePure(TBool, ETrue)), false)
                  ),
                  templates = Map.empty,
                  exceptions = Map.empty,
                  interfaces = List.empty,
                  featureFlags = FeatureFlags.default,
                )
              ),
              Set.empty[PackageId],
              LanguageVersion.default,
              None,
            )
        )
      )
    val pkgs3 = PureCompiledPackages.assertBuild(
      Map(
        dummyPkg ->
          Package.build(
            List(
              Module(
                name = modName,
                definitions = Map.empty,
                templates = Map.empty,
                exceptions = Map.empty,
                interfaces = Map.empty,
                featureFlags = FeatureFlags.default,
              )
            ),
            Set.empty[PackageId],
            LanguageVersion.default,
            None,
          )
      )
    )

    val seed = crypto.Hash.hashPrivateKey("test")
    val committers = Set(Ref.Party.assertFromString("alice"))

    "succeeds" in {
      val result = SpeedyTestLib.run(
        machine = Speedy.Machine.fromUpdateExpr(pkgs1, seed, EVal(ref), committers),
        getPkg = { case pkgId if pkgId == ref.packageId => pkgs2 },
      )
      result shouldBe Right(SValue.SBool(true))
    }

    "crashes without definition" in {
      val result = SpeedyTestLib.run(
        machine = Speedy.Machine.fromUpdateExpr(pkgs1, seed, EVal(ref), committers),
        getPkg = { case pkgId if pkgId == ref.packageId => pkgs3 },
      )
      inside(result) { case Left(SError.SErrorCrash(loc, msg)) =>
        loc shouldBe "com.daml.lf.speedy.Speedy.Machine.lookupVal"
        msg should include(s"definition ${LfDefRef(ref)} not found")
      }
    }
  }

}
