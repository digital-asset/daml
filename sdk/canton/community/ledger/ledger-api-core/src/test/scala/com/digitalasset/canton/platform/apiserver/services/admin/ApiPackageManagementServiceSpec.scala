// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ErrorsAssertions
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.admin.package_management_service.{
  PackageManagementServiceGrpc,
  UploadDarFileRequest,
  ValidateDarFileRequest,
  ValidateDarFileResponse,
}
import com.digitalasset.daml.lf.data.Ref
import com.daml.tracing.DefaultOpenTelemetry
import com.daml.tracing.TelemetrySpecBase.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.{
  InternalStateService,
  ReadService,
  SubmissionResult,
  Update,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext, Traced}
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level.DEBUG

import scala.concurrent.Future

// TODO(#17635) Very thin layer. Revisit utility of testing
class ApiPackageManagementServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with PekkoBeforeAndAfterAll
    with Eventually
    with ErrorsAssertions
    with BaseTest
    with BeforeAndAfterEach {

  import ApiPackageManagementServiceSpec.*

  var testTelemetrySetup: TestTelemetrySetup = _

  override def beforeEach(): Unit = {
    testTelemetrySetup = new TestTelemetrySetup()
  }

  override def afterEach(): Unit = {
    testTelemetrySetup.close()
  }

  "ApiPackageManagementService $suffix" should {
    "propagate trace context" in {
      val apiService = createApiService()
      val span = testTelemetrySetup.anEmptySpan()
      val scope = span.makeCurrent()
      apiService
        .uploadDarFile(UploadDarFileRequest(ByteString.EMPTY, aSubmissionId))
        .andThen { case _ =>
          scope.close()
          span.end()
        }
        .map { _ =>
          testTelemetrySetup.reportedSpanAttributes should contain(anApplicationIdSpanAttribute)
          succeed
        }
    }

    "have a tid" in {
      val apiService = createApiService()
      val span = testTelemetrySetup.anEmptySpan()
      val _ = span.makeCurrent()

      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(DEBUG))(
        within = {
          apiService
            .uploadDarFile(UploadDarFileRequest(ByteString.EMPTY, aSubmissionId))
            .map(_ => succeed)
        },
        { logEntries =>
          logEntries should not be empty

          val mdcs = logEntries.map(_.mdc)
          forEvery(mdcs) { _.getOrElse("trace-id", "") should not be empty }
        },
      )
    }

    "validate a dar" in {
      val apiService = createApiService()
      apiService
        .validateDarFile(ValidateDarFileRequest(ByteString.EMPTY, aSubmissionId))
        .map { case ValidateDarFileResponse() => succeed }
    }
  }

  private def createApiService(): PackageManagementServiceGrpc.PackageManagementService = {
    ApiPackageManagementService.createApiService(
      TestReadService(testTelemetrySetup.tracer),
      TestWritePackagesService(testTelemetrySetup.tracer),
      telemetry = new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
      loggerFactory = loggerFactory,
    )
  }
}

object ApiPackageManagementServiceSpec {
  private val aSubmissionId = "aSubmission"

  private final case class TestWritePackagesService(tracer: Tracer)
      extends state.WritePackagesService {
    override def uploadDar(
        dar: ByteString,
        submissionId: Ref.SubmissionId,
    )(implicit
        traceContext: TraceContext
    ): Future[SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      Future.successful(state.SubmissionResult.Acknowledged)
    }
  }

  private final case class TestReadService(tracer: Tracer) extends ReadService {
    override def validateDar(dar: ByteString, darName: String)(implicit
        traceContext: TraceContext
    ): Future[SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      Future.successful(state.SubmissionResult.Acknowledged)
    }

    override def stateUpdates(beginAfter: Option[Offset])(implicit
        traceContext: TraceContext
    ): Source[(Offset, Traced[Update]), NotUsed] =
      throw new UnsupportedOperationException()

    override def internalStateService: Option[InternalStateService] =
      throw new UnsupportedOperationException()

    override def registerInternalStateService(internalStateService: InternalStateService): Unit =
      throw new UnsupportedOperationException()

    override def unregisterInternalStateService(): Unit =
      throw new UnsupportedOperationException()

    override def currentHealth(): HealthStatus =
      throw new UnsupportedOperationException()
  }
}
