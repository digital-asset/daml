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
  private[this] val nowP2 = now.addMicros(2)

  "ledger time primitives" - {
    "ledgerTimeLT" in {
      val testData = Table[Time.Timestamp, Time.Timestamp, Time.Range, Option[Boolean], Time.Range](
        ("now", "time", "time-boundary", "result", "updated-time-boundary"),
        // now > time
        // - lb < ub
        //   + time < lb
        (nowP2, now, Time.Range(nowP1, nowP2), Some(false), Time.Range(nowP1, nowP2)),
        //   + time == lb
        (nowP2, now, Time.Range(now, nowP1), Some(false), Time.Range(now, nowP1)),
        //   + time > lb
        (nowP2, nowP1, Time.Range(now, nowP1), Some(false), Time.Range(nowP1, nowP1)),
        // - lb == ub
        //   + time < lb
        (nowP2, now, Time.Range(nowP1, nowP1), Some(false), Time.Range(nowP1, nowP1)),
        //   + time == lb
        (nowP2, now, Time.Range(now, now), Some(false), Time.Range(now, now)),
        //   + time > lb
        (nowP2, nowP1, Time.Range(now, now), None, Time.Range(now, now)), // Speedy crash expected

        // now == time
        // - lb < ub
        //   + time < lb
        (now, now, Time.Range(nowP1, nowP2), Some(false), Time.Range(nowP1, nowP2)),
        //   + time == lb
        (now, now, Time.Range(now, nowP1), Some(false), Time.Range(now, nowP1)),
        //   + time > lb
        (nowP1, nowP1, Time.Range(now, nowP1), Some(false), Time.Range(nowP1, nowP1)),
        // - lb == ub
        //   + time < lb
        (now, now, Time.Range(nowP1, nowP1), Some(false), Time.Range(nowP1, nowP1)),
        //   + time == lb
        (now, now, Time.Range(now, now), Some(false), Time.Range(now, now)),
        //   + time > lb
        (
          nowP2,
          nowP2,
          Time.Range(nowP1, nowP1),
          None,
          Time.Range(nowP1, nowP1),
        ), // Speedy crash expected

        // now < time
        // - lb < ub
        //   + time-1 < ub
        (now, nowP1, Time.Range(now, nowP1), Some(true), Time.Range(now, now)),
        //   + time-1 == ub
        (now, nowP2, Time.Range(now, nowP1), Some(true), Time.Range(now, nowP1)),
        //   + time-1 > ub
        (now, Time.Timestamp.MaxValue, Time.Range(now, nowP1), Some(true), Time.Range(now, nowP1)),
        // - lb == ub
        //   + time-1 < ub
        (
          now,
          nowP1,
          Time.Range(nowP1, nowP1),
          None,
          Time.Range(nowP1, nowP1),
        ), // Speedy crash expected
        //   + time-1 == ub
        (now, nowP1, Time.Range(now, now), Some(true), Time.Range(now, now)),
        //   + time-1 > ub
        (now, nowP2, Time.Range(now, now), Some(true), Time.Range(now, now)),
      )

      forEvery(testData) { (now, time, timeBoundaries, expectedResult, expectedTimeBoundaries) =>
        inside(runTimeMachine("ledger_time_lt", now, time, timeBoundaries)) {
          case (Success(Right(SValue.SBool(actualResult))), actualTimeBoundaries, messages) =>
            Some(actualResult) shouldBe expectedResult
            // Empty string is due to use of transactionTrace and no commands being in the transaction tree
            messages shouldBe Seq("queried time", "")
            actualTimeBoundaries shouldBe expectedTimeBoundaries

          case (
                Success(Left(SError.SErrorCrash(location, cause))),
                actualTimeBoundaries,
                messages,
              ) =>
            expectedResult shouldBe None
            location shouldBe "com.digitalasset.daml.lf.speedy.Speedy.UpdateMachine.needTime"
            cause shouldBe "unexpected exception java.lang.AssertionError: assertion failed when running continuation of question NeedTime"
            // Empty string is due to use of transactionTrace and no commands being in the transaction tree
            messages shouldBe Seq("queried time", "")
            actualTimeBoundaries shouldBe expectedTimeBoundaries
        }
      }
    }
  }

  private def runTimeMachine(
      builtin: String,
      now: Time.Timestamp,
      time: Time.Timestamp,
      timeBoundaries: Time.Range,
  ) = {
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

    machine.setTimeBoundaries(timeBoundaries)

    val result = Try(
      SpeedyTestLib.run(
        machine,
        getTime = traceLog.tracePF("queried time", { case () => now }),
      )
    )

    (result, machine.getTimeBoundaries, traceLog.getMessages)
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
