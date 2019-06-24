// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.time.Instant

import akka.stream.scaladsl.Sink
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.testing.time_service.{GetTimeRequest, SetTimeRequest}
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import com.digitalasset.platform.services.time.TimeProviderType.WallClock
import io.grpc.{Status, StatusRuntimeException}
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
class TimeServiceDisabledIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with TestExecutionSequencerFactory
    with AsyncTimeLimitedTests
    with Matchers
    with Inside
    with OptionValues {

  override def timeLimit: Span = scaled(15.seconds)

  override protected def config: Config = Config.default.withTimeProvider(WallClock)

  "Time Service" when {
    "server is not in static mode" should {
      "not have getTime available" in forAllFixtures {
        case TestFixture(_, context) =>
          val timeSource =
            ClientAdapter.serverStreaming(
              GetTimeRequest(Config.defaultLedgerId.unwrap),
              context.timeService.getTime)

          timeSource
            .take(1)
            .runWith(Sink.head)
            .failed
            .map(assertNotImplemented)
      }

      "not have setTime available" in forAllFixtures {
        case TestFixture(_, context) =>
          context.timeService
            .setTime(
              SetTimeRequest(
                Config.defaultLedgerId.unwrap,
                Some(fromInstant(Instant.EPOCH)),
                Some(fromInstant(Instant.EPOCH.plusSeconds(1)))
              ))
            .failed
            .map(assertNotImplemented)
      }

      def assertNotImplemented(throwable: Throwable) = {
        val ex = throwable.asInstanceOf[StatusRuntimeException]
        ex.getStatus.getCode shouldEqual Status.UNIMPLEMENTED.getCode
      }
    }
  }
}
