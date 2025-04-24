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

  private[this] val t0 = Time.Timestamp.now()
  private[this] val t1 = t0.addMicros(1)
  private[this] val t2 = t0.addMicros(2)
  private[this] val t3 = t0.addMicros(3)

  "ledger time primitives" - {
    "ledgerTimeLT" in {
      val testData = Table[Time.Timestamp, Time.Timestamp, Time.Range, Option[Boolean], Time.Range](
        ("now", "time", "time-boundary", "result", "updated-time-boundary"),
        // now > time
        // - lb < ub
        //   + time < lb
        (t2, t0, Time.Range(t1, t2), Some(false), Time.Range(t1, t2)),
        (
          Time.Timestamp.MaxValue,
          t0,
          Time.Range(t1, t2),
          None,
          Time.Range(t1, t2),
        ), // NeedTime pre-condition failure
        (t0, Time.Timestamp.MinValue, Time.Range(t0, t1), Some(false), Time.Range(t0, t1)),
        //   + time == lb
        (t1, t0, Time.Range(t0, t1), Some(false), Time.Range(t0, t1)),
        (t2, t0, Time.Range(t0, t1), None, Time.Range(t0, t1)), // NeedTime pre-condition failure
        (
          t0,
          Time.Timestamp.MinValue,
          Time.Range(Time.Timestamp.MinValue, t0),
          Some(false),
          Time.Range(Time.Timestamp.MinValue, t0),
        ),
        //   + time > lb
        (t2, t1, Time.Range(t0, t1), None, Time.Range(t0, t1)), // NeedTime pre-condition failure
        (t2, t1, Time.Range(t0, t2), Some(false), Time.Range(t1, t2)),
        // - lb == ub
        //   + time < lb
        (t1, t0, Time.Range(t1, t1), Some(false), Time.Range(t1, t1)),
        (t2, t0, Time.Range(t1, t1), None, Time.Range(t1, t1)), // NeedTime pre-condition failure
        (t0, Time.Timestamp.MinValue, Time.Range(t0, t0), Some(false), Time.Range(t0, t0)),
        //   + time == lb
        (t2, t0, Time.Range(t0, t0), None, Time.Range(t0, t0)), // NeedTime pre-condition failure
        (
          t0,
          Time.Timestamp.MinValue,
          Time.Range(Time.Timestamp.MinValue, t0),
          Some(false),
          Time.Range(Time.Timestamp.MinValue, t0),
        ),
        //   + time > lb
        (t2, t1, Time.Range(t0, t0), None, Time.Range(t0, t0)), // NeedTime pre-condition failure

        // now == time
        // - lb < ub
        //   + time < lb
        (t0, t0, Time.Range(t1, t2), None, Time.Range(t1, t2)), // NeedTime pre-condition failure
        (
          Time.Timestamp.MinValue,
          Time.Timestamp.MinValue,
          Time.Range(t0, t1),
          None,
          Time.Range(t0, t1),
        ), // NeedTime pre-condition failure
        //   + time == lb
        (t0, t0, Time.Range(t0, t1), Some(false), Time.Range(t0, t1)),
        (
          Time.Timestamp.MinValue,
          Time.Timestamp.MinValue,
          Time.Range(Time.Timestamp.MinValue, t0),
          Some(false),
          Time.Range(Time.Timestamp.MinValue, t0),
        ),
        //   + time > lb
        (t1, t1, Time.Range(t0, t1), Some(false), Time.Range(t1, t1)),
        (t2, t2, Time.Range(t0, t1), None, Time.Range(t0, t1)), // NeedTime pre-condition failure
        // - lb == ub
        //   + time < lb
        (t0, t0, Time.Range(t1, t1), None, Time.Range(t1, t1)), // NeedTime pre-condition failure
        (
          Time.Timestamp.MinValue,
          Time.Timestamp.MinValue,
          Time.Range(t0, t0),
          None,
          Time.Range(t0, t0),
        ), // NeedTime pre-condition failure
        //   + time == lb
        (t0, t0, Time.Range(t0, t0), Some(false), Time.Range(t0, t0)),
        (
          Time.Timestamp.MinValue,
          Time.Timestamp.MinValue,
          Time.Range(Time.Timestamp.MinValue, Time.Timestamp.MinValue),
          Some(false),
          Time.Range(Time.Timestamp.MinValue, Time.Timestamp.MinValue),
        ),
        //   + time > lb
        (
          t2,
          t2,
          Time.Range(t1, t1),
          None,
          Time.Range(t1, t1),
        ), // NeedTime pre-condition failure

        // now < time
        // - lb < ub
        //   + time-1 < ub
        (t0, t2, Time.Range(t1, t2), None, Time.Range(t1, t2)), // NeedTime pre-condition failure
        (t0, t1, Time.Range(t0, t1), Some(true), Time.Range(t0, t0)),
        //   + time-1 == ub
        (t0, t3, Time.Range(t1, t2), None, Time.Range(t1, t2)), // NeedTime pre-condition failure
        (t0, t2, Time.Range(t0, t1), Some(true), Time.Range(t0, t1)),
        //   + time-1 > ub
        (t0, t3, Time.Range(t0, t1), Some(true), Time.Range(t0, t1)),
        (t0, Time.Timestamp.MaxValue, Time.Range(t0, t1), Some(true), Time.Range(t0, t1)),
        // - lb == ub
        //   + time-1 < ub
        (
          t0,
          t1,
          Time.Range(t1, t1),
          None,
          Time.Range(t1, t1),
        ), // NeedTime pre-condition failure
        //   + time-1 == ub
        (t0, t1, Time.Range(t0, t0), Some(true), Time.Range(t0, t0)),
        (t0, t2, Time.Range(t1, t1), None, Time.Range(t1, t1)), // NeedTime pre-condition failure
        //   + time-1 > ub
        (t0, t2, Time.Range(t0, t0), Some(true), Time.Range(t0, t0)),
        (t0, t3, Time.Range(t1, t1), None, Time.Range(t1, t1)), // NeedTime pre-condition failure
      )

      forEvery(testData) { (now, time, timeBoundaries, expectedResult, expectedTimeBoundaries) =>
        inside(runTimeMachine("ledger_time_lt", now, time, timeBoundaries)) {
          case (Success(Right(SValue.SBool(actualResult))), actualTimeBoundaries, messages) =>
            Some(actualResult) shouldBe expectedResult
            messages shouldBe Seq("queried time")
            actualTimeBoundaries shouldBe expectedTimeBoundaries

          case (
                Success(Left(SError.SErrorCrash(location, cause))),
                actualTimeBoundaries,
                messages,
              ) =>
            expectedResult shouldBe None
            location shouldBe "com.digitalasset.daml.lf.speedy.Speedy.UpdateMachine.needTime"
            cause shouldBe s"unexpected exception java.lang.IllegalArgumentException: requirement failed: NeedTime pre-condition failed: time $now lies outside time boundaries $timeBoundaries when running continuation of question NeedTime"
            messages shouldBe Seq("queried time")
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
