// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.commands.Commands
import com.daml.tracing.TelemetryContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.Ctx
import com.google.protobuf.empty.Empty
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class CommandSubmissionFlowTest
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with Matchers
    with AkkaBeforeAndAfterAll
    with BaseTest {

  "apply" should {
    "propagate trace context" in {
      val mockTelemetryContext = mock[TelemetryContext]
      val argCaptor = ArgCaptor[Future[Ctx[_, _]]]
      when(mockTelemetryContext.runInOpenTelemetryScope(argCaptor.capture))
        .thenAnswer(argCaptor.value)

      Source
        .single(Ctx((), CommandSubmission(Commands.defaultInstance), mockTelemetryContext))
        .via(CommandSubmissionFlow(_ => Future.successful(Empty.defaultInstance), 1, loggerFactory))
        .runWith(Sink.head)
        .map { ctx =>
          verify(mockTelemetryContext).runInOpenTelemetryScope(any[Future[_]])
          ctx.telemetryContext shouldBe mockTelemetryContext
        }
    }
  }
}
