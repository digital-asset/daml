// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.time

import akka.stream.scaladsl.Sink
import com.daml.api.util.TimestampConversion.fromInstant
import com.daml.grpc.GrpcException
import com.daml.grpc.adapter.client.akka.ClientAdapter
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.testing.time_service.{GetTimeRequest, SetTimeRequest, TimeServiceGrpc}
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag.ToTagOps

import java.time.Instant

final class WallClockTimeIT
    extends AsyncWordSpec
    with SandboxFixture
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with ScalaFutures
    with Matchers {

  override val timeLimit: Span = 15.seconds

  override def config = super.config.copy(participants =
    singleParticipant(
      ApiServerConfig.copy(
        timeProviderType = TimeProviderType.WallClock
      )
    )
  )

  private val unimplemented: PartialFunction[Any, Unit] = { case GrpcException.UNIMPLEMENTED() =>
    ()
  }

  "Time Service" when {
    "server is not in static mode" should {
      "not have getTime available" in {
        ClientAdapter
          .serverStreaming(GetTimeRequest(ledgerId().unwrap), TimeServiceGrpc.stub(channel).getTime)
          .take(1)
          .runWith(Sink.head)
          .failed
          .map(_ should matchPattern(unimplemented))
      }

      "not have setTime available" in {
        TimeServiceGrpc
          .stub(channel)
          .setTime(
            SetTimeRequest(
              ledgerId().unwrap,
              Some(fromInstant(Instant.EPOCH)),
              Some(fromInstant(Instant.EPOCH.plusSeconds(1))),
            )
          )
          .failed
          .map(_ should matchPattern(unimplemented))
      }
    }
  }
}
