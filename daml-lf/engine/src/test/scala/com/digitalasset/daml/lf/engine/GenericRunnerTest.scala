// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package test

import com.daml.lf.engine._
import com.daml.lf._
import com.daml.lf.archive.UniversalArchiveDecoder
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.data.Ref
import com.daml.lf.language.{Ast, Util => AstUtil}
import com.daml.lf.value.{Value => lf}
import com.daml.logging.LoggingContext

import java.io.File
import java.util.concurrent.atomic.AtomicLong

class GenericRunnerTest extends AnyWordSpec with Matchers with Inside with BazelRunfiles {

  import GenericRunner._

  private[this] implicit def loggingContext: LoggingContext = LoggingContext.ForTesting

  val (mainPkgId, packages) = {
    val packages =
      UniversalArchiveDecoder.assertReadFile(new File(rlocation("daml-lf/engine/IO.dar")))
    (packages.main._1, packages.all.toMap)
  }

  "runner" should {
    "work " in {

      val moduleName = Ref.DottedName.assertFromString("IO")

      def id(s: String) =
        Ref.Identifier(mainPkgId, Ref.QualifiedName(moduleName, Ref.DottedName.assertFromString(s)))

      val pop = id("pop_")
      val log = id("log_")
      val run = id("run")

      val runner = data.assertRight(
        GenericRunner.build(
          packages,
          List(pop, log),
        )
      )

      val counter = new AtomicLong()
      val logger = new StringBuilder

      def answer(tag: Ref.Identifier, arg: Value): Result[Value] = {
        tag match {
          case `pop` =>
            runner.lfToValue(AstUtil.TInt64, lf.ValueInt64(counter.getAndIncrement()))
          case `log` =>
            runner.valueToText(arg).map { t =>
              logger ++= t
              Value.Unit
            }
          case _ =>
            throw new IllegalStateException("2")

        }
      }

      val typ = AstUtil.TUnit
      val ref = id("test")

      val expr = {
        import Ast._
        EApp(EVal(run), EVal(ref))
      }
      inside(runner.run(typ, expr).consume(answer)) { case Right(value) =>
        value.typ shouldBe typ
        logger.toString() shouldBe ">1.0<"
        counter.get() shouldBe 2
      }
    }
  }
}
