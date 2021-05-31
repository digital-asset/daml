package com.daml.ledger.client.services.commands

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.telemetry.TelemetryContext
import com.daml.util.Ctx
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
    with AkkaBeforeAndAfterAll {

  "apply" should {
    "propagate trace context" in {
      val mockTelemetryContext = mock[TelemetryContext]
      val argCaptor = ArgCaptor[Future[Ctx[_, _]]]
      when(mockTelemetryContext.runInOpenTelemetryScope(argCaptor.capture))
        .thenAnswer(argCaptor.value)

      Source
        .single(Ctx((), SubmitRequest.defaultInstance, mockTelemetryContext))
        .via(CommandSubmissionFlow(_ => Future.successful(Empty.defaultInstance), 1))
        .runWith(Sink.head)
        .map { ctx =>
          verify(mockTelemetryContext).runInOpenTelemetryScope(any[Future[_]])
          ctx.telemetryContext shouldBe mockTelemetryContext
        }
    }
  }
}
