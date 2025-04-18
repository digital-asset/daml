// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.data.Ref.Location
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}

class LedgerTimeTestV2 extends LedgerTimeTest(LanguageMajorVersion.V2)

class LedgerTimeTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with Inside
    with TableDrivenPropertyChecks {

  private[this] implicit val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)
  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  private[this] val now = Time.Timestamp.now()
  private[this] val nowP1 = now.addMicros(1)

  "ledger time primitives" - {
    "ledgerTimeLT" in {
      val testData = Table[Time.Timestamp, Time.Timestamp, Boolean](
        ("now", "time", "result"),
        (nowP1, now, false),
        (now, now, false),
        (now, nowP1, true),
      )

      forEvery(testData) { (now, time, expected) =>
        inside(runTimeMachine("ledger_time_lt", now, time)) {
          case (Success(Right(SValue.SBool(`expected`))), messages) =>
            messages shouldBe Seq("queried time")
        }
      }
    }
  }

  private def runTimeMachine(builtin: String, now: Time.Timestamp, time: Time.Timestamp) = {
    val compilerConfig = Compiler.Config.Default(majorLanguageVersion)
    val pkgs = PureCompiledPackages.Empty(compilerConfig)
    val builtinSExpr = pkgs.compiler.unsafeCompile(e"""\(time: Timestamp) -> $builtin time""")
    val seed = crypto.Hash.hashPrivateKey("seed")
    val traceLog = new TestTraceLog()
    val machine = Speedy.Machine
      .fromUpdateSExpr(
        pkgs,
        seed,
        SEApp(builtinSExpr, Array(SValue.STimestamp(time))),
        Set.empty,
        traceLog = traceLog,
      )
    val result = Try(
      SpeedyTestLib.run(
        machine,
        getTime = traceLog.tracePF("queried time", { case () => now }),
      )
    )

    (result, traceLog.getMessages)
  }
}

class TestTraceLog extends TraceLog {
  private val messages: ArrayBuffer[(String, Option[Location])] = new ArrayBuffer()

  override def add(message: String, optLocation: Option[Location])(implicit
      loggingContext: LoggingContext
  ) = {
    messages += ((message, optLocation))
  }

  def tracePF[X, Y](text: String, pf: PartialFunction[X, Y]): PartialFunction[X, Y] = {
    case x if { add(text, None)(LoggingContext.ForTesting); pf.isDefinedAt(x) } => pf(x)
  }

  override def iterator = messages.iterator

  def getMessages: Seq[String] = messages.view.map(_._1).toSeq
}
