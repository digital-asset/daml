// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.time.Instant

import akka.stream.scaladsl.Sink
import com.digitalasset.api.util.TimestampConversion.{fromInstant, toInstant}
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  SetTimeRequest
}
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import org.scalatest._
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import scalaz.syntax.tag._

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Option2Iterable",
    "org.wartremover.warts.StringPlusAny"
  ))
class TimeServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with TestExecutionSequencerFactory
    with Matchers
    with Inside
    with OptionValues {

  override def timeLimit: Span = scaled(15.seconds)

  override protected def config: Config = Config.default

  "Time Service" when {
    "querying time" should {
      "return the initial time" in allFixtures { c =>
        val timeSource = ClientAdapter.serverStreaming(
          GetTimeRequest(Config.defaultLedgerId.unwrap),
          c.timeService.getTime)

        timeSource.take(1).runWith(Sink.seq).map { times =>
          inside(times) {
            case Seq(GetTimeResponse(Some(time))) =>
              val instant = toInstant(time)
              instant shouldBe Instant.EPOCH
          }
        }
      }
    }

    "advancing time" should {
      "return the new time" in allFixtures { c =>
        def timeSource =
          ClientAdapter.serverStreaming(
            GetTimeRequest(Config.defaultLedgerId.unwrap),
            c.timeService.getTime)

        for {
          times1 <- timeSource.take(1).runWith(Sink.seq)
          _ <- c.timeService.setTime(
            SetTimeRequest(
              Config.defaultLedgerId.unwrap,
              Some(fromInstant(Instant.EPOCH)),
              Some(fromInstant(Instant.EPOCH.plusSeconds(1)))
            ))
          times2 <- timeSource.take(1).runWith(Sink.seq)

        } yield {
          inside(times1) {
            case Seq(GetTimeResponse(Some(time1))) =>
              toInstant(time1) shouldBe Instant.EPOCH
          }
          inside(times2) {
            case Seq(GetTimeResponse(Some(time2))) =>
              toInstant(time2) shouldBe Instant.EPOCH.plusSeconds(1)
          }
        }
      }
    }
  }
}
