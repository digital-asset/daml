// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.{Success, Try}

class LedgerTimeTestV2 extends LedgerTimeTest(LanguageMajorVersion.V2)

class LedgerTimeTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks {

  private[this] implicit val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)
  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  private[this] val now = Time.Timestamp.now()
  private[this] val nowP1 = now.addMicros(1)

  "ledger time primitives" - {
    "ledgerTimeLT" in {
      val testData = Table[Time.Timestamp, Time.Timestamp, Boolean, Int](
        ("now", "time", "result", "GetTime calls"),
        (nowP1, now, false, 1),
        (now, now, false, 1),
        (now, nowP1, true, 1),
      )

      forEvery(testData) { (now, time, expected, _) =>
        runTimeMachine("ledger_time_lt", now, time) shouldBe Success(Right(SValue.SBool(expected)))
      // TODO: ensure GetTime called numOfGetTimeCalls times
      }
    }
  }

  private def runTimeMachine(builtin: String, now: Time.Timestamp, time: Time.Timestamp) = {
    val compilerConfig = Compiler.Config.Default(majorLanguageVersion)
    val pkgs = PureCompiledPackages.Empty(compilerConfig)
    val builtinSExpr = pkgs.compiler.unsafeCompile(e"""\(time: Timestamp) -> $builtin time""")
    val seed = crypto.Hash.hashPrivateKey("seed")
//    val traceLog = new TestTraceLog()
    val machine = Speedy.Machine
      .fromUpdateSExpr(
        pkgs,
        seed,
        SEApp(builtinSExpr, Array(SValue.STimestamp(time))),
        Set.empty,
//        traceLog = traceLog,
      )

    Try(
      SpeedyTestLib.run(
        machine,
        getTime = { case () => now },
//        getTime = traceLog.tracePF("queries time", () => now),
      )
    )
  }
}
